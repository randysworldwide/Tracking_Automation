"""
shopify_update.py
=================
1. Pages through Shopify orders (branded only, e.g. ZUM, RWW, YUK)
2. Finds orders where custom.erp_master_order_number OR
   custom.erp_invoice_number is missing / blank
3. Looks those PO#s up in GP RRPRead.[dbo].[CustomerShippingDetail]
4. Updates the metafields on any order that has a GP match

Usage:
    py shopify_update.py                  # last 90 days, all stores in config
    py shopify_update.py --days 180       # last 180 days
    py shopify_update.py --store zumbrota # specific store key (substring match)
    py shopify_update.py --all            # no date filter (ALL orders)
"""

import os, sys, re, time, argparse, datetime, traceback
import pyodbc, requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from config import DB_CONFIG, SHOPIFY_CONFIG
except ImportError as e:
    print(f"ERROR: config.py not found or missing keys: {e}")
    sys.exit(1)

API_VERSION = "2025-01"

# ─────────────────────────────────────────────────────────────────────────────
# GraphQL: fetch a page of orders with ERP metafields
# ─────────────────────────────────────────────────────────────────────────────
ORDERS_QUERY = """
query GetOrders($cursor: String, $query: String) {
  orders(first: 50, after: $cursor, query: $query) {
    edges {
      node {
        id
        name
        erp_master: metafield(namespace: "custom", key: "erp_master_order_number") {
          value
        }
        erp_invoice: metafield(namespace: "custom", key: "erp_invoice_number") {
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

METAFIELDS_SET = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { key namespace value }
    userErrors  { field message }
  }
}
"""

SQL_QUERY = """
SELECT
    D.[PO#],
    MIN(D.[MASTER#])  AS [MASTER#],
    MIN(D.[INVOICE#]) AS [INVOICE#]
FROM [RRPRead].[dbo].[CustomerShippingDetail] D
WHERE D.[PO#] IN ({placeholders})
GROUP BY D.[PO#]
"""

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def extract_po(name):
    """'#ZUM1234' → 'ZUM1234'"""
    return str(name).lstrip("#").strip() if name else None

def detect_brand(po):
    """'ZUM1234' → 'ZUM',  '7001234' → 'NOPFX'"""
    if not po:
        return "NOPFX"
    m = re.match(r"^([A-Za-z]+)", po)
    return m.group(1).upper() if m else "NOPFX"

def shopify_gql(shop, token, query, variables=None, retries=3):
    url = f"https://{shop}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(retries):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
        except requests.RequestException as e:
            print(f"  Network error: {e}")
            time.sleep(5)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 4))
            print(f"  Rate limited — waiting {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")

        return r.json()

    raise RuntimeError(f"Failed after {retries} attempts")


def fetch_orders_needing_erp(shop, token, date_filter=None):
    """
    Pages through all Shopify orders and returns those where at least one
    ERP metafield is missing, AND the order name has a brand prefix.
    Returns list of dicts: {id, name, po, brand, has_master, has_invoice}
    """
    query_str = date_filter or ""
    cursor = None
    needs_erp = []
    page = 0

    print(f"  Fetching orders from {shop}...")
    if query_str:
        print(f"  Filter: {query_str}")

    while True:
        page += 1
        variables = {"query": query_str, "cursor": cursor}
        data = shopify_gql(shop, token, ORDERS_QUERY, variables)

        edges = data.get("data", {}).get("orders", {}).get("edges", [])
        page_info = data.get("data", {}).get("orders", {}).get("pageInfo", {})

        for edge in edges:
            node = edge["node"]
            order_id = node["id"]   # full GID: gid://shopify/Order/12345
            name     = node["name"]
            po       = extract_po(name)
            brand    = detect_brand(po)

            if brand == "NOPFX":
                continue

            master_val  = (node.get("erp_master") or {}).get("value", "")
            invoice_val = (node.get("erp_invoice") or {}).get("value", "")

            if master_val and invoice_val:
                continue  # already populated — skip

            needs_erp.append({
                "id":          order_id,
                "name":        name,
                "po":          po,
                "brand":       brand,
                "has_master":  bool(master_val),
                "has_invoice": bool(invoice_val),
            })

        print(f"  Page {page}: {len(edges)} orders scanned, "
              f"{len(needs_erp)} needing ERP so far...", end="\r")

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)  # polite pacing

    print()  # newline after \r
    return needs_erp


def fetch_erp_data(po_list):
    """Batch-query GP for MASTER# and INVOICE# by PO#."""
    if not po_list:
        return {}

    driver = None
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for d in preferred:
        if d in pyodbc.drivers():
            driver = d
            break
    if not driver:
        raise RuntimeError(f"No SQL Server ODBC driver found. Available: {pyodbc.drivers()}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    conn = pyodbc.connect(conn_str)

    erp = {}
    batch_size = 1000
    for i in range(0, len(po_list), batch_size):
        batch = po_list[i: i + batch_size]
        placeholders = ",".join(["?" for _ in batch])
        query = SQL_QUERY.format(placeholders=placeholders)
        cursor = conn.cursor()
        cursor.execute(query, batch)
        for row in cursor.fetchall():
            po, master, invoice = row[0], row[1], row[2]
            erp[str(po).strip()] = {
                "master":  str(master).strip() if master else "",
                "invoice": str(invoice).strip() if invoice else "",
            }
    conn.close()
    return erp


def update_metafields(shop, token, order_gid, master, invoice, has_master, has_invoice):
    """Only set the fields that are actually missing."""
    metafields = []
    if not has_master and master:
        metafields.append({
            "ownerId":   order_gid,
            "namespace": "custom",
            "key":       "erp_master_order_number",
            "value":     master,
            "type":      "single_line_text_field",
        })
    if not has_invoice and invoice:
        metafields.append({
            "ownerId":   order_gid,
            "namespace": "custom",
            "key":       "erp_invoice_number",
            "value":     invoice,
            "type":      "single_line_text_field",
        })
    if not metafields:
        return None  # nothing to do (GP had blank values)

    data = shopify_gql(shop, token, METAFIELDS_SET, {"metafields": metafields})
    return data.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])


def run(days=90, store_filter=None, all_orders=False):
    print(f"\n{'='*60}")
    print(f"  Shopify → GP ERP Metafield Filler")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # Pick which stores to process
    stores = {
        k: v for k, v in SHOPIFY_CONFIG.items()
        if not store_filter or store_filter.lower() in k.lower()
    }
    if not stores:
        print(f"ERROR: No store matches '{store_filter}'. "
              f"Available: {list(SHOPIFY_CONFIG.keys())}")
        sys.exit(1)

    # Build Shopify date filter
    if all_orders:
        date_filter = ""
        print("  Scanning ALL orders (no date limit)\n")
    else:
        since = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        date_filter = f"created_at:>={since}"
        print(f"  Scanning orders created since {since} (--days {days})\n")

    total_updated = 0
    total_no_gp   = 0
    total_errors  = 0

    for store_key, cfg in stores.items():
        shop  = cfg["shop"]
        token = cfg["token"]
        print(f"── Store: {store_key} ({shop}) ──────────────────────────────")

        # Step 1: find orders needing ERP data
        try:
            needs_erp = fetch_orders_needing_erp(shop, token, date_filter)
        except Exception as e:
            print(f"  ERROR fetching orders: {e}")
            traceback.print_exc()
            continue

        print(f"  Found {len(needs_erp)} branded orders missing ERP metafields.")
        if not needs_erp:
            print()
            continue

        brand_summary = {}
        for o in needs_erp:
            brand_summary[o["brand"]] = brand_summary.get(o["brand"], 0) + 1
        print(f"  By brand: {dict(sorted(brand_summary.items()))}")

        # Step 2: query GP
        po_list = list({o["po"] for o in needs_erp if o["po"]})
        print(f"\n  Querying GP for {len(po_list)} unique PO#s...")
        try:
            erp = fetch_erp_data(po_list)
        except Exception as e:
            print(f"  ERROR querying GP: {e}")
            traceback.print_exc()
            continue
        print(f"  GP matched {len(erp)} PO#s.")

        # Step 3: update Shopify
        print(f"\n  Updating Shopify metafields...")
        updated = no_gp = errors = skipped = 0

        for order in needs_erp:
            po = order["po"]
            if po not in erp:
                no_gp += 1
                continue

            gp_row  = erp[po]
            master  = gp_row["master"]
            invoice = gp_row["invoice"]

            if not master and not invoice:
                skipped += 1
                continue

            try:
                user_errors = update_metafields(
                    shop, token,
                    order["id"], master, invoice,
                    order["has_master"], order["has_invoice"]
                )
            except Exception as e:
                print(f"  ERROR updating {order['name']}: {e}")
                errors += 1
                time.sleep(1)
                continue

            if user_errors is None:
                skipped += 1   # GP had blank values, nothing written
            elif user_errors:
                print(f"  GraphQL error on {order['name']}: {user_errors}")
                errors += 1
            else:
                updated += 1

            time.sleep(0.4)

        total_updated += updated
        total_no_gp   += no_gp
        total_errors  += errors

        print(f"\n  Results for {store_key}:")
        print(f"    Updated      : {updated}")
        print(f"    No GP match  : {no_gp}")
        print(f"    GP blank/skip: {skipped}")
        print(f"    Errors       : {errors}")
        print()

    print(f"{'='*60}")
    print(f"  TOTAL  Updated: {total_updated}  |  No GP match: {total_no_gp}  |  Errors: {total_errors}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fill missing ERP metafields on Shopify orders from GP."
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="How many days back to scan (default: 90). Ignored if --all is set."
    )
    parser.add_argument(
        "--store", default=None,
        help="Store key substring to target a specific store (e.g. 'zumbrota'). "
             "Defaults to all stores in SHOPIFY_CONFIG."
    )
    parser.add_argument(
        "--all", dest="all_orders", action="store_true",
        help="Scan ALL orders with no date limit."
    )
    args = parser.parse_args()
    run(days=args.days, store_filter=args.store, all_orders=args.all_orders)
