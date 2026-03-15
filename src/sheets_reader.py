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

# Only these 3 are required; the rest are optional
REQUIRED_COLUMNS = [
    INPUT_COLUMNS["fsn"],
    INPUT_COLUMNS["subcategory"],
    INPUT_COLUMNS["title"],
]

OPTIONAL_COLUMNS = [
    INPUT_COLUMNS.get("sku", ""),
    INPUT_COLUMNS.get("status", ""),
    INPUT_COLUMNS.get("listing_id", ""),
    INPUT_COLUMNS.get("mrp", ""),
    INPUT_COLUMNS.get("selling_price", ""),
    INPUT_COLUMNS.get("fulfillment", ""),
    INPUT_COLUMNS.get("stock", ""),
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

    # Check required columns exist
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns in FSN Master sheet: {missing_required}")

    # Keep required + any optional columns that are present
    all_wanted = REQUIRED_COLUMNS + [c for c in OPTIONAL_COLUMNS if c and c in df.columns]
    df = df[[c for c in all_wanted if c in df.columns]].copy()

    # Filter only ACTIVE listings (if Listing Status column exists)
    status_col = INPUT_COLUMNS.get("status", "")
    if status_col and status_col in df.columns:
        df = df[df[status_col].astype(str).str.strip().str.upper() == "ACTIVE"]

    # Remove rows with empty FSN
    fsn_col = INPUT_COLUMNS["fsn"]
    df = df[df[fsn_col].notna() & (df[fsn_col].astype(str).str.strip() != "")]

    logger.info(f"Loaded {len(df)} FSNs from Google Sheets")

    # Group by Sub-category
    subcat_col = INPUT_COLUMNS["subcategory"]
    grouped = {}
    for subcat, group in df.groupby(subcat_col):
        grouped[str(subcat)] = group.to_dict("records")

    logger.info(f"Sub-categories found: {list(grouped.keys())}")
    for subcat, rows in grouped.items():
        logger.info(f"  {subcat}: {len(rows)} FSNs")

    return grouped
