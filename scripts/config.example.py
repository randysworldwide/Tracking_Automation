"""
config.example.py
=================
Copy this file to config.py and fill in your credentials before running.

IMPORTANT: Do not commit config.py to source control — it contains passwords.
"""

# ── Database ──────────────────────────────────────────────────────────────────
# Uses Windows Integrated Security — no username/password required.
# Your Windows/AD account must have read access to RRPread on svazsql003.
DB_CONFIG = {
    "server":   "svazsql003",
    "database": "RRPread",
}

# ── SFTP ──────────────────────────────────────────────────────────────────────
FTP_CONFIG = {
    "host":       "sftp.suredone.com",
    "port":       22,
    "username":   "randys",
    "password":   "FILL_IN",          # ask James for the SFTP password
    "remote_dir": "/ftp/shipments/randys",
}

# ── CSV Output ────────────────────────────────────────────────────────────────
CSV_CONFIG = {
    "filename_template": "shipments_randys_{date}_{time}.csv",
    "date_format":       "%m%d%Y",
    "time_format":       "%H%M%S",
    "shipdate_format":   "%m/%d/%Y",
    "output_dir":        "./exports",
}

# ── Column Mapping ────────────────────────────────────────────────────────────
COLUMN_MAPPING = {
    "PO#":          "OrderNum",
    "MASTER#":      "MasterNum",
    "INVOICEDDATE": "ShipDate",
    "TRACKING#":    "ShipTracking",
    "ITEM#":        "Item",
    "QUANTITY":     "QtyShipped",
    "INVOICE#":     "InvoiceNum",
    "CARRIER":      "ShipCarrier",
}

# ── Shopify API ────────────────────────────────────────────────────────────────
SHOPIFY_CONFIG = {
    "zumbrotadrivetrain": {
        "shop":  "zumbrotadrivetrain.myshopify.com",
        "token": "FILL_IN",           # ask James for the Shopify API token
    },
}

# ── SureDone API ───────────────────────────────────────────────────────────────
SUREDONE_CONFIG = {
    "username":  "randys-worldwide",
    "api_token": "FILL_IN",           # ask James for the SureDone API token
}
