import asyncio
import aiohttp
import random
import logging
import requests
from datetime import datetime
from typing import Optional

from src.config import (
    PINCODES,
    USER_AGENTS,
    HEADERS,
    FLIPKART_URL_PATTERNS,
    RATE_LIMIT_DELAY,
    RATE_LIMIT_JITTER,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    MAX_CONCURRENT_PER_SUBCATEGORY,
    CAPTCHA_MIN_PAGE_SIZE,
    INPUT_COLUMNS,
    SCRAPINGBEE_API_KEY,
    CRAWLBASE_TOKEN,
)
from src.parser import parse_product_page, calculate_delivery_days

logger = logging.getLogger(__name__)

RETRY_DELAY = 5


async def scrape_with_retry(
    session: aiohttp.ClientSession, url: str, retries: int = MAX_RETRIES
) -> Optional[str]:
    """Fetch URL with retries, rate-limit detection, and CAPTCHA handling."""
    for attempt in range(retries):
        try:
            headers = {**HEADERS, "User-Agent": random.choice(USER_AGENTS)}
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    if len(html) < CAPTCHA_MIN_PAGE_SIZE:
                        logger.warning(
                            f"Suspicious small response ({len(html)} bytes) for {url}"
                        )
                        if "captcha" in html.lower() or "robot" in html.lower():
                            logger.error(f"CAPTCHA detected for {url}")
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1) * 3)
                            continue
                    return html
                elif resp.status == 429:
                    wait = RETRY_DELAY * (attempt + 1) * 2
                    logger.warning(f"Rate limited (429). Waiting {wait}s...")
                    await asyncio.sleep(wait)
                elif resp.status == 404:
                    logger.warning(f"FSN not found (404): {url}")
                    return None
                else:
                    logger.warning(f"HTTP {resp.status} for {url}")
                    await asyncio.sleep(RETRY_DELAY)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed for {url}: {e}")
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))

    return None


def scrape_with_scrapingbee(url: str) -> Optional[str]:
    """Fallback: use ScrapingBee API for JS-rendered pages."""
    if not SCRAPINGBEE_API_KEY:
        return None
    try:
        resp = requests.get(
            "https://app.scrapingbee.com/api/v1/",
            params={
                "api_key": SCRAPINGBEE_API_KEY,
                "url": url,
                "render_js": "true",
                "country_code": "in",
            },
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.error(f"ScrapingBee failed for {url}: {e}")
    return None


def scrape_with_crawlbase(url: str) -> Optional[str]:
    """Fallback: use Crawlbase API."""
    if not CRAWLBASE_TOKEN:
        return None
    try:
        resp = requests.get(
            "https://api.crawlbase.com/",
            params={"token": CRAWLBASE_TOKEN, "url": url},
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.error(f"Crawlbase failed for {url}: {e}")
    return None


async def scrape_fsn_pincode(
    session: aiohttp.ClientSession, row: dict, pincode: str, city: str
) -> dict:
    """Scrape a single FSN for a single pincode. Returns one result row."""
    fsn = str(row.get(INPUT_COLUMNS["fsn"], "")).strip()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    base_result = {
        "timestamp": timestamp,
        "fsn": fsn,
        "seller_sku": row.get(INPUT_COLUMNS.get("sku", ""), ""),
        "sub_category": row.get(INPUT_COLUMNS["subcategory"], ""),
        "product_title": row.get(INPUT_COLUMNS["title"], ""),
        "seller_price": _safe_int(row.get(INPUT_COLUMNS.get("selling_price", ""), "")),
        "live_price": None,
        "mrp": _safe_int(row.get(INPUT_COLUMNS.get("mrp", ""), "")),
        "discount_pct": "",
        "price_match": "",
        "fulfillment_by": row.get(INPUT_COLUMNS.get("fulfillment", ""), ""),
        "seller_stock": _safe_int(row.get(INPUT_COLUMNS.get("stock", ""), "")),
        "pincode": pincode,
        "city": city,
        "in_stock": None,
        "delivery_date": "N/A",
        "delivery_days": "N/A",
        "scrape_status": "failed",
        "error_message": "",
    }

    html = None
    error_msg = ""

    # Try each URL pattern
    for pattern in FLIPKART_URL_PATTERNS:
        url = pattern.format(fsn=fsn) + f"&pincode={pincode}"
        logger.debug(f"Trying {url}")
        html = await scrape_with_retry(session, url)
        if html and len(html) >= CAPTCHA_MIN_PAGE_SIZE:
            break
        html = None

    # Fallback to ScrapingBee
    if not html and SCRAPINGBEE_API_KEY:
        logger.info(f"Trying ScrapingBee for FSN {fsn} pincode {pincode}")
        url = FLIPKART_URL_PATTERNS[0].format(fsn=fsn) + f"&pincode={pincode}"
        html = await asyncio.get_running_loop().run_in_executor(
            None, scrape_with_scrapingbee, url
        )

    # Fallback to Crawlbase
    if not html and CRAWLBASE_TOKEN:
        logger.info(f"Trying Crawlbase for FSN {fsn} pincode {pincode}")
        url = FLIPKART_URL_PATTERNS[0].format(fsn=fsn) + f"&pincode={pincode}"
        html = await asyncio.get_running_loop().run_in_executor(
            None, scrape_with_crawlbase, url
        )

    if not html:
        base_result["error_message"] = "All fetch methods failed"
        logger.error(f"All methods failed for FSN {fsn} pincode {pincode}")
        return base_result

    # Parse the HTML
    try:
        parsed = parse_product_page(html, fsn, pincode)

        if parsed.get("product_title"):
            base_result["product_title"] = parsed["product_title"]

        if parsed.get("live_price") is not None:
            base_result["live_price"] = parsed["live_price"]

        if parsed.get("mrp") is not None:
            base_result["mrp"] = parsed["mrp"]

        if parsed.get("in_stock") is not None:
            base_result["in_stock"] = parsed["in_stock"]
        else:
            base_result["in_stock"] = None

        if parsed.get("delivery_date"):
            base_result["delivery_date"] = parsed["delivery_date"]
            base_result["delivery_days"] = calculate_delivery_days(parsed["delivery_date"])

        # Calculate discount percentage
        if base_result["live_price"] and base_result["mrp"] and base_result["mrp"] > 0:
            discount = round(
                (1 - base_result["live_price"] / base_result["mrp"]) * 100
            )
            base_result["discount_pct"] = f"{discount}%"

        # Price match check
        if base_result["live_price"] is not None and base_result["seller_price"] is not None:
            base_result["price_match"] = (
                "YES" if base_result["live_price"] == base_result["seller_price"] else "NO"
            )

        base_result["scrape_status"] = "success"

    except Exception as e:
        error_msg = f"Parse error: {str(e)}"
        base_result["error_message"] = error_msg
        logger.error(f"Parse error for FSN {fsn} pincode {pincode}: {e}")

    return base_result


async def scrape_subcategory(
    subcategory: str, fsn_rows: list, session: aiohttp.ClientSession
) -> list:
    """Scrape all FSNs in one sub-category. Runs as one concurrent task."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PER_SUBCATEGORY)
    results = []
    total = len(fsn_rows)

    logger.info(f"[{subcategory}] Starting scrape of {total} FSNs")

    async def scrape_single_fsn(row: dict, idx: int):
        async with semaphore:
            fsn = row.get(INPUT_COLUMNS["fsn"], "unknown")
            logger.info(
                f"[{subcategory}] Scraping FSN {idx + 1}/{total}: {fsn}"
            )
            for pincode, city in PINCODES.items():
                result = await scrape_fsn_pincode(session, row, pincode, city)
                results.append(result)
                # Rate limiting with jitter
                delay = RATE_LIMIT_DELAY + random.uniform(0, RATE_LIMIT_JITTER)
                await asyncio.sleep(delay)

    tasks = [scrape_single_fsn(row, i) for i, row in enumerate(fsn_rows)]
    await asyncio.gather(*tasks, return_exceptions=True)

    success = sum(1 for r in results if r.get("scrape_status") == "success")
    logger.info(
        f"[{subcategory}] Done: {success}/{len(results)} successful scrapes"
    )

    return results


def _safe_int(val) -> int | None:
    """Safely convert a value to int, return None on failure."""
    if val is None or val == "":
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None
