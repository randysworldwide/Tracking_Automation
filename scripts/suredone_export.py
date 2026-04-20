"""
suredone_export.py
==================
Automation script: Azure SQL (RRPread) → CSV → SFTP (Suredone)

Replaces the manual Excel workflow:
  1. Queries CustomerShippingDetail + VehicleInfo from RRPread
  2. Applies column mapping for Suredone's upload template
  3. Exports a date-named CSV
  4. Uploads it to the FTP for Suredone to ingest

Run directly:   python suredone_export.py
Schedule with:  Windows Task Scheduler, cron, or Cowork schedule
"""

import os
import datetime
import sys
import traceback
import argparse

import pandas as pd
import pyodbc
import paramiko

# Import your credentials and settings
try:
    from config import DB_CONFIG, FTP_CONFIG, CSV_CONFIG, COLUMN_MAPPING
except ImportError:
    print("ERROR: config.py not found. Copy config.example.py to config.py and fill in your credentials.")
    sys.exit(1)


# ── SQL Query ─────────────────────────────────────────────────────────────────

SQL_QUERY = """
SELECT
    D.[INVOICEDDATE],
    D.[BILL_TO_CUST#],
    D.[INVOICE#],
    D.[BILL_TO],
    D.[SHIP_TO],
    D.[Warehouse_Shipped_From],
    D.[PO#],
    D.[ITEM#],
    D.[QUANTITY],
    D.[MASTER#],
    D.[SHIPPING_DETAIL],
    D.[SHIPPING_DATETIME],
    D.[CARRIER],
    D.[SHIP_OPTION],
    D.[TRACKING#],
    D.[INV_NOTE],
    V.[MODEL] AS [MODEL]
FROM [RRPRead].[dbo].[CustomerShippingDetail] D
LEFT JOIN [RRPRead].[dbo].[VehicleInfo] V
    ON RTRIM(V.[mstrnumb]) = RTRIM(D.[MASTER#])
WHERE
(
    (
        (D.[PO#] LIKE '%RWW%' OR D.[PO#] LIKE '%ZUM%' OR D.[PO#] LIKE '%B2B%'
         OR D.[PO#] LIKE '%YUK%' OR D.[PO#] LIKE '%USA%')
        AND D.[INVOICEDDATE] BETWEEN ? AND GETDATE()
    )
    OR
    (
        D.[BILL_TO_CUST#] IN ('305943', '237093', '310319')
        AND D.[INVOICEDDATE] BETWEEN ? AND GETDATE()
    )
)
"""


def get_start_date(business_days=None, calendar_days=2):
    """Calculate the lookback start date. Uses business days if specified, otherwise calendar days."""
    if business_days:
        today = datetime.date.today()
        count = 0
        current = today
        while count < business_days:
            current -= datetime.timedelta(days=1)
            if current.weekday() < 5:  # Mon=0 … Fri=4
                count += 1
        return current
    else:
        return datetime.date.today() - datetime.timedelta(days=calendar_days)


# ── Steps ─────────────────────────────────────────────────────────────────────

def get_best_odbc_driver():
    """Find the best available SQL Server ODBC driver installed on this machine."""
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    available = pyodbc.drivers()
    for driver in preferred:
        if driver in available:
            print(f"  Using driver: {driver}")
            return driver
    raise RuntimeError(
        f"No SQL Server ODBC driver found. Installed drivers: {available}\n"
        "Download from: https://aka.ms/downloadmsodbcsql"
    )


def get_db_connection():
    """Connect to SQL Server using Windows Integrated Security (no username/password needed)."""
    driver = get_best_odbc_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def fetch_data(start_date):
    """Run the SQL query and return a DataFrame."""
    print("  Connecting to database...")
    conn = get_db_connection()
    print(f"  Running query (from {start_date})...")
    df = pd.read_sql(SQL_QUERY, conn, params=[start_date, start_date])
    conn.close()
    print(f"  Fetched {len(df)} rows.")
    return df


def apply_column_mapping(df):
    """Rename columns and reorder them per Suredone's upload template."""
    # eBay orders (BILL_TO_CUST# = 237093): use MODEL as OrderNum instead of PO#
    ebay_mask = df["BILL_TO_CUST#"].astype(str).str.strip() == "237093"
    df.loc[ebay_mask, "PO#"] = df.loc[ebay_mask, "MODEL"]
    ebay_count = int(ebay_mask.sum())
    if ebay_count > 0:
        print(f"  eBay rows: {ebay_count} (OrderNum set from MODEL)")

    cols_present = [c for c in COLUMN_MAPPING if c in df.columns]
    missing = [c for c in COLUMN_MAPPING if c not in df.columns]
    if missing:
        print(f"  WARNING: These expected columns were not found in the query result: {missing}")

    df = df[cols_present].rename(columns=COLUMN_MAPPING)

    # Format ShipDate as MM/DD/YYYY to match Suredone's expected format
    if "ShipDate" in df.columns:
        df["ShipDate"] = pd.to_datetime(df["ShipDate"]).dt.strftime(CSV_CONFIG["shipdate_format"])

    # Preserve column order as defined in COLUMN_MAPPING
    ordered = [COLUMN_MAPPING[c] for c in cols_present]
    df = df[ordered]

    print(f"  Mapped to {len(df.columns)} Suredone columns.")
    return df


def export_csv(df):
    """Export DataFrame to a date+time-named CSV file matching Suredone's naming convention."""
    now = datetime.datetime.now()
    date_str = now.strftime(CSV_CONFIG["date_format"])  # e.g. 03162026
    time_str = now.strftime(CSV_CONFIG["time_format"])  # e.g. 152816
    filename = CSV_CONFIG["filename_template"].format(date=date_str, time=time_str)
    output_dir = CSV_CONFIG["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    print(f"  Saved: {filepath}")
    return filepath, filename


def sftp_makedirs(sftp, remote_dir):
    """Walk the remote path and create any missing directories."""
    parts = remote_dir.strip("/").split("/")
    current = ""
    for part in parts:
        current += "/" + part
        try:
            sftp.stat(current)
        except FileNotFoundError:
            print(f"  Creating remote directory: {current}")
            sftp.mkdir(current)


def upload_to_sftp(filepath, filename):
    """Upload the CSV to the configured SFTP server using paramiko."""
    host = FTP_CONFIG["host"]
    port = FTP_CONFIG.get("port", 22)
    remote_dir = FTP_CONFIG.get("remote_dir", "").rstrip("/")

    print(f"  Connecting to SFTP: {host}:{port} ...")
    transport = paramiko.Transport((host, port))
    transport.connect(username=FTP_CONFIG["username"], password=FTP_CONFIG["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        # Ensure remote directory exists
        if remote_dir:
            sftp_makedirs(sftp, remote_dir)

        remote_path = f"{remote_dir}/{filename}" if remote_dir else filename
        sftp.put(filepath, remote_path)
        print(f"  Uploaded '{filename}' → {remote_path}")
    finally:
        sftp.close()
        transport.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--business-days", type=int, default=None,
                        help="Look back N business days (e.g. 5)")
    parser.add_argument("--calendar-days", type=int, default=2,
                        help="Look back N calendar days (default: 2)")
    args = parser.parse_args()

    start_date = get_start_date(
        business_days=args.business_days,
        calendar_days=args.calendar_days
    )

    start = datetime.datetime.now()
    print(f"\n{'='*60}")
    print(f"  Suredone Export  |  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    if args.business_days:
        print(f"  Lookback: {args.business_days} business days (since {start_date})")
    else:
        print(f"  Lookback: {args.calendar_days} calendar days (since {start_date})")
    print(f"{'='*60}")

    try:
        print("\n[1/4] Fetching data from SQL...")
        df = fetch_data(start_date)

        print("\n[2/4] Applying column mapping...")
        df = apply_column_mapping(df)

        print("\n[3/4] Exporting CSV...")
        filepath, filename = export_csv(df)

        print("\n[4/4] Uploading to SFTP...")
        upload_to_sftp(filepath, filename)

        elapsed = (datetime.datetime.now() - start).seconds
        print(f"\n{'='*60}")
        print(f"  Done in {elapsed}s — {len(df)} rows exported as '{filename}'")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run()
