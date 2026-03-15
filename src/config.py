import os
from dotenv import load_dotenv

load_dotenv()

PINCODES = {
    "110001": "Delhi",
    "560001": "Bangalore",
    "400001": "Mumbai",
    "411001": "Pune",
}

# Column names from Flipkart Seller Panel export (EXACT match required)
INPUT_COLUMNS = {
    "fsn": "Flipkart Serial Number",
    "sku": "Seller SKU Id",
    "title": "Product Title",
    "subcategory": "Sub-category",
    "listing_id": "Listing ID",
    "status": "Listing Status",
    "mrp": "MRP",
    "selling_price": "Your Selling Price",
    "fulfillment": "Fulfillment By",
    "stock": "System Stock count",
}

RATE_LIMIT_DELAY = 2.0
RATE_LIMIT_JITTER = 1.5
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_PER_SUBCATEGORY = 3
CAPTCHA_MIN_PAGE_SIZE = 5000

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

FLIPKART_URL_PATTERNS = [
    "https://www.flipkart.com/product/p/itm?pid={fsn}",
    "https://www.flipkart.com/dl/product/p?pid={fsn}",
]

SCRAPINGBEE_API_KEY = os.environ.get("SCRAPINGBEE_API_KEY", "")
CRAWLBASE_TOKEN = os.environ.get("CRAWLBASE_TOKEN", "")

# Google Sheets config
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
FSN_MASTER_TAB_NAME = "FSN Master"
LATEST_SNAPSHOT_TAB = "Latest Snapshot"
HISTORICAL_LOG_TAB = "Historical Log"
OOS_ALERTS_TAB = "OOS Alerts"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

OUTPUT_DIR = "output"
LOG_DIR = "logs"
