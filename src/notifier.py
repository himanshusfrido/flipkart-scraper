import requests
import logging
from src.config import SLACK_WEBHOOK_URL

logger = logging.getLogger(__name__)


def send_slack_summary(
    total_fsns: int,
    success_count: int,
    fail_count: int,
    oos_count: int,
    price_mismatch_count: int,
    duration_mins: float,
):
    """Send scrape run summary to Slack via incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.info("No Slack webhook configured — skipping notification")
        return

    payload = {
        "text": (
            f":package: *Frido — Flipkart Scraper Run Complete*\n"
            f"*FSNs:* {total_fsns} total | {success_count} success | {fail_count} failed\n"
            f"*OOS Alerts:* {oos_count} products out of stock in 1+ cities\n"
            f"*Price Mismatches:* {price_mismatch_count} products with different live price\n"
            f"*Duration:* {duration_mins:.1f} minutes\n"
            f"*Google Sheet:* Updated"
        )
    }

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("Slack notification sent successfully")
        else:
            logger.warning(f"Slack returned status {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
