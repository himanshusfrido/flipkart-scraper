import asyncio
import aiohttp
import logging
import os
import sys
import time
import pandas as pd
from datetime import datetime

from src.config import OUTPUT_DIR, LOG_DIR, PINCODES
from src.sheets_reader import load_fsn_from_sheets
from src.scraper import scrape_subcategory
from src.browser_scraper import create_browser, close_browser
from src.sheets_writer import push_to_sheets
from src.notifier import send_slack_summary


def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR, f"scrape_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ],
    )
    return logging.getLogger(__name__)


async def orchestrate():
    """Main orchestrator: read -> scrape -> save -> push -> notify."""
    logger = setup_logging()
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("Flipkart FSN Scraper — Frido")
    logger.info(f"Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Pincodes: {PINCODES}")
    logger.info("=" * 60)

    # Step 1: Read FSN list from Google Sheets
    logger.info("Step 1: Reading FSN list from Google Sheets...")
    try:
        fsn_data = load_fsn_from_sheets()
    except Exception as e:
        logger.error(f"Failed to read FSN list from Google Sheets: {e}")
        raise

    total_fsns = sum(len(rows) for rows in fsn_data.values())
    logger.info(
        f"Found {total_fsns} active FSNs across {len(fsn_data)} sub-categories"
    )
    logger.info(f"Sub-categories: {list(fsn_data.keys())}")

    # Step 2: Launch browser + scrape all sub-categories concurrently
    logger.info("Step 2: Launching Playwright browser...")
    pw, browser = None, None
    try:
        pw, browser = await create_browser()
    except Exception as e:
        logger.warning(f"Playwright launch failed ({e}), falling back to aiohttp only")

    logger.info("Step 2: Starting concurrent scraping...")

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        subcategory_tasks = [
            scrape_subcategory(subcat, rows, session, browser=browser)
            for subcat, rows in fsn_data.items()
        ]
        all_results = await asyncio.gather(
            *subcategory_tasks, return_exceptions=True
        )

    # Close browser
    if pw and browser:
        try:
            await close_browser(pw, browser)
        except Exception:
            pass

    # Step 3: Flatten results, handle exceptions
    flat_results = []
    for i, result in enumerate(all_results):
        subcat = list(fsn_data.keys())[i]
        if isinstance(result, Exception):
            logger.error(f"Sub-category '{subcat}' failed entirely: {result}")
        elif isinstance(result, list):
            flat_results.extend(result)
            logger.info(f"Sub-category '{subcat}': {len(result)} results collected")
        else:
            logger.warning(f"Sub-category '{subcat}': unexpected result type {type(result)}")

    if not flat_results:
        logger.error("No results collected from any sub-category!")
        return

    # Step 4: Create results DataFrame
    results_df = pd.DataFrame(flat_results)

    # Step 5: Calculate summary stats
    total_scrapes = len(results_df)
    success_count = len(results_df[results_df["scrape_status"] == "success"])
    fail_count = total_scrapes - success_count

    # OOS count: unique FSNs that are OOS in at least one city
    oos_fsns = set()
    if "in_stock" in results_df.columns:
        oos_rows = results_df[results_df["in_stock"] == False]
        oos_fsns = set(oos_rows["fsn"].unique())
    oos_count = len(oos_fsns)

    # Price mismatch count: unique FSNs with price_match == "NO"
    price_mismatch_fsns = set()
    if "price_match" in results_df.columns:
        mismatch_rows = results_df[results_df["price_match"] == "NO"]
        price_mismatch_fsns = set(mismatch_rows["fsn"].unique())
    price_mismatch_count = len(price_mismatch_fsns)

    logger.info("=" * 60)
    logger.info("SCRAPE SUMMARY")
    logger.info(f"  Total FSNs: {total_fsns}")
    logger.info(f"  Total scrapes (FSN x pincode): {total_scrapes}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {fail_count}")
    logger.info(f"  OOS in 1+ cities: {oos_count} FSNs")
    logger.info(f"  Price mismatches: {price_mismatch_count} FSNs")
    logger.info("=" * 60)

    # Step 6: Save to CSV backup
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_filename = f"results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    csv_path = os.path.join(OUTPUT_DIR, csv_filename)
    results_df.to_csv(csv_path, index=False)
    logger.info(f"Results saved to {csv_path}")

    # Step 7: Push to Google Sheets
    logger.info("Step 7: Pushing results to Google Sheets...")
    try:
        push_to_sheets(results_df)
        logger.info("Google Sheets updated successfully")
    except Exception as e:
        logger.error(f"Failed to push to Google Sheets: {e}")

    # Step 8: Send Slack notification
    duration_mins = (time.time() - start_time) / 60
    logger.info("Step 8: Sending Slack notification...")
    try:
        send_slack_summary(
            total_fsns=total_fsns,
            success_count=success_count,
            fail_count=fail_count,
            oos_count=oos_count,
            price_mismatch_count=price_mismatch_count,
            duration_mins=duration_mins,
        )
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")

    logger.info(f"Run completed in {duration_mins:.1f} minutes")
    logger.info("=" * 60)


def main():
    asyncio.run(orchestrate())


if __name__ == "__main__":
    main()
