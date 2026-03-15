import gspread
import json
import base64
import os
import logging
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from src.config import INPUT_COLUMNS, FSN_MASTER_TAB_NAME, GOOGLE_SHEET_ID

logger = logging.getLogger(__name__)

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

REQUIRED_COLUMNS = [
    INPUT_COLUMNS["title"],
    INPUT_COLUMNS["sku"],
    INPUT_COLUMNS["subcategory"],
    INPUT_COLUMNS["fsn"],
    INPUT_COLUMNS["listing_id"],
    INPUT_COLUMNS["status"],
    INPUT_COLUMNS["mrp"],
    INPUT_COLUMNS["selling_price"],
    INPUT_COLUMNS["fulfillment"],
    INPUT_COLUMNS["stock"],
]


def get_gspread_client():
    """Authenticate with Google Sheets using base64-encoded service account creds."""
    creds_b64 = os.environ.get("GOOGLE_SHEETS_CREDS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_SHEETS_CREDS environment variable not set")
    creds_json = json.loads(base64.b64decode(creds_b64))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)


def load_fsn_from_sheets():
    """Read FSN Master tab from Google Sheets, filter ACTIVE, group by Sub-category.

    Returns:
        dict: {sub_category_name: [list of row dicts]}
    """
    client = get_gspread_client()
    sheet_id = GOOGLE_SHEET_ID
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID environment variable not set")

    spreadsheet = client.open_by_key(sheet_id)
    ws = spreadsheet.worksheet(FSN_MASTER_TAB_NAME)
    all_data = ws.get_all_records()

    if not all_data:
        raise ValueError("FSN Master sheet is empty!")

    df = pd.DataFrame(all_data)

    # Keep only required columns (handle missing gracefully)
    available = [c for c in REQUIRED_COLUMNS if c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        logger.warning(f"Missing columns in FSN Master sheet: {missing}")
    df = df[available].copy()

    # Filter only ACTIVE listings
    status_col = INPUT_COLUMNS["status"]
    if status_col in df.columns:
        df = df[df[status_col].astype(str).str.strip().str.upper() == "ACTIVE"]

    # Remove rows with empty FSN
    fsn_col = INPUT_COLUMNS["fsn"]
    df = df[df[fsn_col].notna() & (df[fsn_col].astype(str).str.strip() != "")]

    logger.info(f"Loaded {len(df)} ACTIVE FSNs from Google Sheets")

    # Group by Sub-category
    subcat_col = INPUT_COLUMNS["subcategory"]
    grouped = {}
    for subcat, group in df.groupby(subcat_col):
        grouped[str(subcat)] = group.to_dict("records")

    logger.info(f"Sub-categories found: {list(grouped.keys())}")
    for subcat, rows in grouped.items():
        logger.info(f"  {subcat}: {len(rows)} FSNs")

    return grouped
