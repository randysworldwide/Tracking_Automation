"""
suredone_tracking_update.py
============================
Full replacement for suredone_export.py.

Old approach: query GP for last N days -> CSV -> SFTP (blind push)
New approach: query SureDone for orders with MISSING tracking
             -> look those up in GP -> CSV with only what's needed -> SFTP

Channels supported (all follow {prefix}{GP_PO#} pattern):
  shopifyYUK*, shopifyZUM*, shopifyRWW*, shopifyUSA*, shopifyB2B*, shopifyDYN*
  amazon*   (GP customer 305943)
  walmart*  (GP customer 310319)

Channels NOT supported:
  shopify*  (plain consumer orders - no GP record)

eBay matching:
  ebay*     - GP stores the eBay order number in VehicleInfo.MODEL (joined on MASTER#)
              for BILL_TO_CUST# = 237093. We match SureDone ebay{order_num} to
              GP by querying MODEL = order_num, same as suredone_export.py.

Usage:
    py suredone_tracking_update.py              # last 10 days
    py suredone_tracking_update.py --days 90    # last 90 days
    py suredone_tracking_update.py --all        # all orders ever (full backfill)
    py suredone_tracking_update.py --dry-run    # preview CSV, don't upload
"""

import os, sys, re, time, argparse, datetime, traceback
import pyodbc, requests, paramiko, pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from config import DB_CONFIG, FTP_CONFIG, CSV_CONFIG, SUREDONE_CONFIG
except ImportError as e:
    print(f"ERROR: config.py missing keys: {e}")
    sys.exit(1)

SD_BASE   = "https://api.suredone.com/v1"
PAGE_SIZE = 50

# ─────────────────────────────────────────────────────────────────────────────
# Channel → PO# extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_po(order_ref):
    """
    Extract the GP PO# from a SureDone order reference string.
    Returns None if the order can't be matched to GP.

    shopifyYUK5894            -> YUK5894
    shopifyZUM27423           -> ZUM27423
    amazon112-6686174-5308259 -> 112-6686174-5308259
    walmart200014510320786    -> 200014510320786
    shopify132488             -> None  (consumer, no GP record)
    ebay16-14428-51370-109832 -> None  (can't match to GP)
    """
    if order_ref.startswith("shopify"):
        po = order_ref[len("shopify"):]
        # Only branded: starts with letters then digits (YUK5894, ZUM27423, etc.)
        # Plain consumer Shopify orders are pure-numeric (132488) → skip
        if re.match(r"^[A-Za-z]+\d", po):
            return po
        return None

    if order_ref.startswith("amazon"):
        return order_ref[len("amazon"):]

    if order_ref.startswith("walmart"):
        return order_ref[len("walmart"):]

    if order_ref.startswith("ebay"):
        return order_ref[len("ebay"):]

    # plain shopify consumer, unknown channels → skip
    return None


def channel_of(order_ref):
    """Return a short channel label for reporting."""
    for prefix in ("shopifyYUK","shopifyZUM","shopifyRWW","shopifyUSA","shopifyB2B","shopifyDYN"):
        if order_ref.startswith(prefix):
            return prefix.replace("shopify","")
    if order_ref.startswith("amazon"):  return "AMAZON"
    if order_ref.startswith("walmart"): return "WALMART"
    if order_ref.startswith("ebay"):    return "EBAY"
    if order_ref.startswith("shopify"): return "shopify(skip)"
    return "OTHER(skip)"


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Pull SureDone orders missing tracking
# ─────────────────────────────────────────────────────────────────────────────

def sd_headers():
    return {
        "X-Auth-User":  SUREDONE_CONFIG["username"],
        "X-Auth-Token": SUREDONE_CONFIG["api_token"],
    }


def fetch_orders_needing_tracking(since_date=None):
    """
    Pages through ALL SureDone orders. Returns those where:
      - shiptracking is blank
      - order ref can be mapped to a GP PO# (extract_po returns non-None)
    Stops early once order dates fall before since_date.
    """
    needs = []
    skipped_channels = {}
    page = 0
    total_scanned = 0
    early_stop = False

    while not early_stop:
        page += 1
        try:
            r = requests.get(f"{SD_BASE}/orders", headers=sd_headers(),
                             params={"limit": PAGE_SIZE, "page": page}, timeout=30)
            r.raise_for_status()
            d = r.json()
        except Exception as e:
            print(f"\n  API error on page {page}: {e}")
            break

        orders = [v for k, v in d.items() if str(k).isdigit()]
        if not orders:
            break

        total_scanned += len(orders)

        for o in orders:
            order_ref    = str(o.get("order", ""))
            order_date_s = str(o.get("date", ""))[:10]

            # Date-based early stop
            if since_date and order_date_s:
                try:
                    if datetime.date.fromisoformat(order_date_s) < since_date:
                        early_stop = True
                        break
                except ValueError:
                    pass

            # Skip if tracking already populated
            tracking = str(o.get("shiptracking", "")).strip()
            if tracking and tracking.lower() not in ("", "none", "null", "0"):
                continue

            po = extract_po(order_ref)
            if po is None:
                ch = channel_of(order_ref)
                skipped_channels[ch] = skipped_channels.get(ch, 0) + 1
                continue

            needs.append({
                "oid":       o.get("oid"),
                "order_ref": order_ref,
                "po":        po,
                "channel":   channel_of(order_ref),
                "date":      order_date_s,
                "is_ebay":   order_ref.startswith("ebay"),
            })

        print(f"  Page {page}: {total_scanned} scanned, "
              f"{len(needs)} needing tracking...", end="\r")
        time.sleep(0.15)

    print()
    if skipped_channels:
        print(f"  Skipped channels: { {k:v for k,v in sorted(skipped_channels.items())} }")
    return needs


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Query GP for tracking
# ─────────────────────────────────────────────────────────────────────────────

GP_SQL = """
SELECT
    D.[PO#],
    MIN(D.[MASTER#])      AS [MASTER#],
    MIN(D.[INVOICEDDATE]) AS [INVOICEDDATE],
    MIN(D.[TRACKING#])    AS [TRACKING#],
    MIN(D.[ITEM#])        AS [ITEM#],
    MIN(D.[QUANTITY])     AS [QUANTITY],
    MIN(D.[INVOICE#])     AS [INVOICE#],
    MIN(D.[CARRIER])      AS [CARRIER]
FROM [RRPRead].[dbo].[CustomerShippingDetail] D
WHERE D.[PO#] IN ({placeholders})
  AND D.[TRACKING#] IS NOT NULL
  AND LTRIM(RTRIM(D.[TRACKING#])) <> ''
GROUP BY D.[PO#]
"""

# eBay: order number lives in VehicleInfo.MODEL, not PO#
GP_EBAY_SQL = """
SELECT
    V.[MODEL]             AS [PO#],
    MIN(D.[MASTER#])      AS [MASTER#],
    MIN(D.[INVOICEDDATE]) AS [INVOICEDDATE],
    MIN(D.[TRACKING#])    AS [TRACKING#],
    MIN(D.[ITEM#])        AS [ITEM#],
    MIN(D.[QUANTITY])     AS [QUANTITY],
    MIN(D.[INVOICE#])     AS [INVOICE#],
    MIN(D.[CARRIER])      AS [CARRIER]
FROM [RRPRead].[dbo].[CustomerShippingDetail] D
LEFT JOIN [RRPRead].[dbo].[VehicleInfo] V
    ON RTRIM(V.[mstrnumb]) = RTRIM(D.[MASTER#])
WHERE D.[BILL_TO_CUST#] = '237093'
  AND V.[MODEL] IN ({placeholders})
  AND D.[TRACKING#] IS NOT NULL
  AND LTRIM(RTRIM(D.[TRACKING#])) <> ''
GROUP BY V.[MODEL]
"""

def get_db_conn():
    preferred = [
        "ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server", "SQL Server Native Client 11.0", "SQL Server",
    ]
    driver = next((d for d in preferred if d in pyodbc.drivers()), None)
    if not driver:
        raise RuntimeError(f"No SQL Server ODBC driver. Available: {pyodbc.drivers()}")
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};Trusted_Connection=yes;"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )


def fetch_tracking_from_gp(needs):
    """
    Returns dict: order_key -> {OrderNum, MasterNum, ShipDate, ShipTracking, ...}

    For eBay orders (is_ebay=True), matches via VehicleInfo.MODEL using GP_EBAY_SQL.
    For all others, matches via CustomerShippingDetail.PO# using GP_SQL.
    The returned dict is keyed by the same 'po' value stored in each needs entry,
    so build_csv can look up by o["po"] regardless of channel.
    """
    regular_pos = list({o["po"] for o in needs if not o.get("is_ebay")})
    ebay_models  = list({o["po"] for o in needs if     o.get("is_ebay")})

    conn = get_db_conn()
    results = {}

    def _parse_rows(cur):
        out = {}
        for row in cur.fetchall():
            po, master, ship_date, tracking, item, qty, invoice, carrier = row
            key = str(po).strip() if po else ""
            if not key:
                continue
            out[key] = {
                "OrderNum":    key,
                "MasterNum":   str(master).strip()   if master   else "",
                "ShipDate":    ship_date.strftime(CSV_CONFIG["shipdate_format"]) if ship_date else "",
                "ShipTracking": str(tracking).strip() if tracking else "",
                "Item":        str(item).strip()     if item     else "",
                "QtyShipped":  str(qty).strip()      if qty      else "",
                "InvoiceNum":  str(invoice).strip()  if invoice  else "",
                "ShipCarrier": str(carrier).strip()  if carrier  else "",
            }
        return out

    # Regular channels (Shopify branded, Amazon, Walmart)
    for i in range(0, len(regular_pos), 1000):
        batch = regular_pos[i: i + 1000]
        q = GP_SQL.format(placeholders=",".join("?" for _ in batch))
        cur = conn.cursor()
        cur.execute(q, batch)
        results.update(_parse_rows(cur))

    # eBay: match by MODEL
    if ebay_models:
        for i in range(0, len(ebay_models), 1000):
            batch = ebay_models[i: i + 1000]
            q = GP_EBAY_SQL.format(placeholders=",".join("?" for _ in batch))
            cur = conn.cursor()
            cur.execute(q, batch)
            ebay_results = _parse_rows(cur)
            results.update(ebay_results)
            print(f"  eBay MODEL matches: {len(ebay_results)} of {len(batch)}")

    conn.close()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Steps 3-5: Build CSV and upload
# ─────────────────────────────────────────────────────────────────────────────

def build_csv(needs, gp_data):
    rows, no_gp = [], []
    for o in needs:
        gp = gp_data.get(o["po"])
        if not gp or not gp["ShipTracking"]:
            no_gp.append(o["po"])
            continue
        rows.append(gp)   # gp dict already has all CSV columns keyed correctly
    df = pd.DataFrame(rows, columns=[
        "OrderNum","MasterNum","ShipDate","ShipTracking",
        "Item","QtyShipped","InvoiceNum","ShipCarrier"
    ])
    return df, no_gp


def export_csv(df):
    now = datetime.datetime.now()
    filename = CSV_CONFIG["filename_template"].format(
        date=now.strftime(CSV_CONFIG["date_format"]),
        time=now.strftime(CSV_CONFIG["time_format"]),
    )
    out_dir = os.path.join(SCRIPT_DIR, CSV_CONFIG["output_dir"])
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, filename)
    df.to_csv(filepath, index=False)
    print(f"  Saved: {filepath}  ({len(df)} rows)")
    return filepath, filename


def sftp_makedirs(sftp, remote_dir):
    parts = remote_dir.strip("/").split("/")
    cur = ""
    for p in parts:
        cur += "/" + p
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)


def upload_to_sftp(filepath, filename):
    host = FTP_CONFIG["host"]
    port = FTP_CONFIG.get("port", 22)
    remote_dir = FTP_CONFIG.get("remote_dir", "").rstrip("/")
    print(f"  Connecting to {host}:{port}...")
    t = paramiko.Transport((host, port))
    t.connect(username=FTP_CONFIG["username"], password=FTP_CONFIG["password"])
    sftp = paramiko.SFTPClient.from_transport(t)
    try:
        if remote_dir:
            sftp_makedirs(sftp, remote_dir)
        remote_path = f"{remote_dir}/{filename}" if remote_dir else filename
        sftp.put(filepath, remote_path)
        print(f"  Uploaded -> {remote_path}")
    finally:
        sftp.close()
        t.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(days=30, all_orders=False, dry_run=False):
    print(f"\n{'='*60}")
    print(f"  SureDone Tracking Updater  (replaces suredone_export.py)")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print(f"  *** DRY RUN — CSV shown, nothing uploaded ***")
    print(f"{'='*60}\n")

    since = None
    if not all_orders:
        since = datetime.date.today() - datetime.timedelta(days=days)
        print(f"  Scanning back {days} days (since {since})\n")
    else:
        print(f"  Scanning ALL SureDone orders (no date limit)\n")

    # ── 1. SureDone ───────────────────────────────────────────────────────────
    print("[1/4] Querying SureDone for orders missing tracking...")
    try:
        needs = fetch_orders_needing_tracking(since_date=since)
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

    print(f"  Found {len(needs)} orders missing tracking.")
    if not needs:
        print("\n  Nothing to do.\n")
        return

    # Channel breakdown
    channels = {}
    for o in needs:
        channels[o["channel"]] = channels.get(o["channel"], 0) + 1
    print(f"  By channel: { dict(sorted(channels.items())) }")

    # ── 2. GP ─────────────────────────────────────────────────────────────────
    po_list = list({o["po"] for o in needs})
    ebay_count = sum(1 for o in needs if o.get("is_ebay"))
    print(f"\n[2/4] Querying GP for {len(po_list)} PO#s "
          f"({ebay_count} eBay via MODEL, {len(po_list)-ebay_count} via PO#)...")
    try:
        gp_data = fetch_tracking_from_gp(needs)
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
    print(f"  GP matched {len(gp_data)} PO#s with tracking data.")

    # ── 3. Build CSV ──────────────────────────────────────────────────────────
    print(f"\n[3/4] Building CSV...")
    df, no_gp = build_csv(needs, gp_data)
    print(f"  Rows with tracking : {len(df)}")
    print(f"  No GP match/data   : {len(no_gp)}")

    if df.empty:
        print("\n  GP has no tracking data for any of these orders.\n")
        return

    filepath, filename = export_csv(df)

    if dry_run:
        print(f"\n  Preview (first 10 rows):")
        print(df.head(10).to_string(index=False))
        print(f"\n  Would upload as: {filename}\n")
        return

    # ── 4. SFTP ───────────────────────────────────────────────────────────────
    print(f"\n[4/4] Uploading to SureDone SFTP...")
    try:
        upload_to_sftp(filepath, filename)
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Done!  {len(df)} orders updated  |  {len(no_gp)} with no GP data")
    print(f"  File : {filename}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Push missing tracking to SureDone from GP (replaces suredone_export.py)."
    )
    parser.add_argument("--days",    type=int, default=10,
                        help="Days back to scan (default 10). Ignored if --all.")
    parser.add_argument("--all",     dest="all_orders", action="store_true",
                        help="Scan all SureDone orders with no date limit.")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true",
                        help="Show CSV preview without uploading.")
    args = parser.parse_args()
    run(days=args.days, all_orders=args.all_orders, dry_run=args.dry_run)
