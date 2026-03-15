import gspread
import logging
import pandas as pd
from datetime import datetime

from src.sheets_reader import get_gspread_client
from src.config import (
    GOOGLE_SHEET_ID,
    LATEST_SNAPSHOT_TAB,
    HISTORICAL_LOG_TAB,
    OOS_ALERTS_TAB,
)

logger = logging.getLogger(__name__)


def push_to_sheets(results_df: pd.DataFrame):
    """Push scrape results to Google Sheets (3 output tabs)."""
    try:
        client = get_gspread_client()
    except ValueError as e:
        logger.warning(f"Skipping Sheets push: {e}")
        return

    if not GOOGLE_SHEET_ID:
        logger.warning("GOOGLE_SHEET_ID not set — skipping Sheets push")
        return

    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # Tab 1: Latest Snapshot (pivot, overwrite)
    try:
        ws_latest = _get_or_create_worksheet(spreadsheet, LATEST_SNAPSHOT_TAB)
        pivot_df = _create_pivot_snapshot(results_df)
        ws_latest.clear()
        data = [pivot_df.columns.tolist()] + _df_to_rows(pivot_df)
        ws_latest.update(data, value_input_option="USER_ENTERED")
        logger.info(f"Updated '{LATEST_SNAPSHOT_TAB}' tab: {len(pivot_df)} rows")
    except Exception as e:
        logger.error(f"Failed to update Latest Snapshot: {e}")

    # Tab 2: Historical Log (append)
    try:
        ws_history = _get_or_create_worksheet(spreadsheet, HISTORICAL_LOG_TAB)
        existing = len(ws_history.get_all_values())
        rows = _df_to_rows(results_df)
        if existing == 0:
            data = [results_df.columns.tolist()] + rows
            ws_history.update(data, value_input_option="USER_ENTERED")
        else:
            ws_history.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info(f"Appended {len(rows)} rows to '{HISTORICAL_LOG_TAB}' tab")
    except Exception as e:
        logger.error(f"Failed to update Historical Log: {e}")

    # Tab 3: OOS Alerts (overwrite)
    try:
        ws_oos = _get_or_create_worksheet(spreadsheet, OOS_ALERTS_TAB)
        oos_df = _create_oos_alerts(results_df)
        ws_oos.clear()
        if not oos_df.empty:
            data = [oos_df.columns.tolist()] + _df_to_rows(oos_df)
            ws_oos.update(data, value_input_option="USER_ENTERED")
            logger.info(f"Updated '{OOS_ALERTS_TAB}' tab: {len(oos_df)} rows")
        else:
            ws_oos.update([["No OOS alerts"]], value_input_option="USER_ENTERED")
            logger.info("No OOS alerts to write")
    except Exception as e:
        logger.error(f"Failed to update OOS Alerts: {e}")


def _get_or_create_worksheet(spreadsheet, title: str):
    """Get existing worksheet or create a new one."""
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Creating new worksheet: {title}")
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=30)


def _df_to_rows(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of lists, replacing NaN/None with empty string."""
    return df.fillna("").astype(str).values.tolist()


def _create_pivot_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Create pivot snapshot: one row per FSN with city-specific columns."""
    pivot_rows = []
    for fsn, group in df.groupby("fsn"):
        first = group.iloc[0]
        row = {
            "FSN": fsn,
            "SKU": first.get("seller_sku", ""),
            "Sub-Category": first.get("sub_category", ""),
            "Product Title": first.get("product_title", ""),
            "Seller Price": first.get("seller_price", ""),
            "Live Price": first.get("live_price", ""),
            "Price Match": first.get("price_match", ""),
            "MRP": first.get("mrp", ""),
            "Discount %": first.get("discount_pct", ""),
            "Fulfillment": first.get("fulfillment_by", ""),
            "Seller Stock": first.get("seller_stock", ""),
        }
        for _, r in group.iterrows():
            city = r.get("city", "")
            if city:
                stock_val = r.get("in_stock")
                if stock_val is True:
                    row[f"{city} Stock"] = "In Stock"
                elif stock_val is False:
                    row[f"{city} Stock"] = "OOS"
                else:
                    row[f"{city} Stock"] = "Unknown"
                row[f"{city} Delivery"] = r.get("delivery_date", "N/A")

        row["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M IST")
        pivot_rows.append(row)

    return pd.DataFrame(pivot_rows)


def _create_oos_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """Create OOS alerts: one row per FSN that is OOS in at least one city."""
    alerts = []
    for fsn, group in df.groupby("fsn"):
        oos_cities = group[group["in_stock"] == False]["city"].tolist()
        in_stock_cities = group[group["in_stock"] == True]["city"].tolist()

        if oos_cities:
            first = group.iloc[0]
            alerts.append(
                {
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "FSN": fsn,
                    "SKU": first.get("seller_sku", ""),
                    "Product Title": first.get("product_title", ""),
                    "Sub-Category": first.get("sub_category", ""),
                    "OOS Cities": ", ".join(oos_cities),
                    "In-Stock Cities": ", ".join(in_stock_cities) or "None",
                    "Live Price": first.get("live_price", ""),
                    "Seller Stock": first.get("seller_stock", ""),
                }
            )

    return pd.DataFrame(alerts)
