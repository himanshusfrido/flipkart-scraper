import re
import json
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def parse_product_page(html: str, fsn: str, pincode: str) -> dict:
    """Parse Flipkart product page HTML using multiple fallback strategies.

    Returns dict with keys: product_title, live_price, mrp, in_stock, delivery_date
    """
    result = {
        "product_title": None,
        "live_price": None,
        "mrp": None,
        "in_stock": None,
        "delivery_date": None,
    }

    if not html:
        return result

    soup = BeautifulSoup(html, "lxml")

    # Strategy A: Extract from embedded JSON (most reliable)
    json_result = _parse_from_page_json(html, fsn, pincode)
    _merge_result(result, json_result)

    # Strategy B: JSON-LD structured data
    if result["live_price"] is None:
        jsonld_result = _parse_from_jsonld(soup)
        _merge_result(result, jsonld_result)

    # Strategy C: CSS selectors (fallback)
    if result["live_price"] is None:
        css_result = _parse_from_css(soup)
        _merge_result(result, css_result)

    # Always try to get product title if still missing
    if not result["product_title"]:
        result["product_title"] = _extract_title(soup)

    # Always try stock status from page text if still missing
    if result["in_stock"] is None:
        result["in_stock"] = _check_stock_from_text(soup)

    # Always try delivery date from page text if still missing
    if result["delivery_date"] is None:
        result["delivery_date"] = _extract_delivery_from_text(soup)

    return result


def _merge_result(target: dict, source: dict):
    """Merge non-None values from source into target."""
    for key, value in source.items():
        if value is not None and target.get(key) is None:
            target[key] = value


def _parse_from_page_json(html: str, fsn: str, pincode: str) -> dict:
    """Strategy A: Extract data from embedded JSON in script tags."""
    result = {
        "product_title": None,
        "live_price": None,
        "mrp": None,
        "in_stock": None,
        "delivery_date": None,
    }

    # Look for pageDataV4 or __INITIAL_STATE__
    json_patterns = [
        r'window\.__INITIAL_STATE__\s*=\s*({.+?});\s*</script>',
        r'pageDataV4\s*=\s*({.+?});\s*</script>',
        r'"pageDataV4"\s*:\s*({.+?})\s*,\s*"',
    ]

    page_data = None
    for pattern in json_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                page_data = json.loads(match.group(1))
                break
            except (json.JSONDecodeError, IndexError):
                continue

    if not page_data:
        # Try to find any large JSON blob in script tags
        for script_match in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
            script_text = script_match.group(1)
            if len(script_text) > 1000 and ('"price"' in script_text or '"sellingPrice"' in script_text):
                # Try to extract JSON object
                for json_match in re.finditer(r'(\{["\']pageDataV4["\'].*?\})\s*[;,]', script_text, re.DOTALL):
                    try:
                        page_data = json.loads(json_match.group(1))
                        break
                    except json.JSONDecodeError:
                        continue
            if page_data:
                break

    if not page_data:
        return result

    # Recursively search the JSON for pricing data
    _extract_from_json_recursive(page_data, result, fsn)

    return result


def _extract_from_json_recursive(data, result: dict, fsn: str, depth: int = 0):
    """Recursively search JSON for price, stock, and delivery data."""
    if depth > 15 or (result["live_price"] is not None and result["in_stock"] is not None):
        return

    if isinstance(data, dict):
        # Check for pricing keys
        for price_key in ["finalPrice", "sellingPrice", "selling_price", "price", "value"]:
            if price_key in data and isinstance(data[price_key], (int, float)):
                if result["live_price"] is None and data[price_key] > 0:
                    result["live_price"] = int(data[price_key])

        # Specific structure: pricing.finalPrice.value
        if "pricing" in data and isinstance(data["pricing"], dict):
            pricing = data["pricing"]
            if "finalPrice" in pricing:
                fp = pricing["finalPrice"]
                if isinstance(fp, dict) and "value" in fp:
                    result["live_price"] = int(fp["value"])
                elif isinstance(fp, (int, float)):
                    result["live_price"] = int(fp)
            if "mrp" in pricing:
                mrp_val = pricing["mrp"]
                if isinstance(mrp_val, dict) and "value" in mrp_val:
                    result["mrp"] = int(mrp_val["value"])
                elif isinstance(mrp_val, (int, float)):
                    result["mrp"] = int(mrp_val)

        # Check for MRP
        for mrp_key in ["mrp", "maximumRetailPrice", "basePrice"]:
            if mrp_key in data and isinstance(data[mrp_key], (int, float)):
                if result["mrp"] is None and data[mrp_key] > 0:
                    result["mrp"] = int(data[mrp_key])

        # Check for availability/stock
        for stock_key in ["availableStatus", "available", "inStock", "isAvailable", "stockStatus"]:
            if stock_key in data:
                val = data[stock_key]
                if isinstance(val, bool):
                    result["in_stock"] = val
                elif isinstance(val, str):
                    result["in_stock"] = val.upper() in ("AVAILABLE", "IN_STOCK", "TRUE", "YES")

        # Check for serviceability/delivery
        if "serviceability" in data and isinstance(data["serviceability"], dict):
            svc = data["serviceability"]
            if "deliveryDate" in svc:
                result["delivery_date"] = str(svc["deliveryDate"])
            elif "promiseDate" in svc:
                result["delivery_date"] = str(svc["promiseDate"])

        # Check for delivery date patterns
        for del_key in ["deliveryDate", "promiseDate", "estimatedDelivery", "deliveryText"]:
            if del_key in data and isinstance(data[del_key], str):
                if result["delivery_date"] is None:
                    result["delivery_date"] = data[del_key]

        # Check product title
        for title_key in ["title", "productName", "name"]:
            if title_key in data and isinstance(data[title_key], str):
                if result["product_title"] is None and len(data[title_key]) > 5:
                    result["product_title"] = data[title_key]

        # Recurse into nested dicts
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                _extract_from_json_recursive(value, result, fsn, depth + 1)

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                _extract_from_json_recursive(item, result, fsn, depth + 1)


def _parse_from_jsonld(soup: BeautifulSoup) -> dict:
    """Strategy B: Extract from JSON-LD structured data (schema.org)."""
    result = {
        "product_title": None,
        "live_price": None,
        "mrp": None,
        "in_stock": None,
        "delivery_date": None,
    }

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        if isinstance(data, list):
            for item in data:
                _extract_jsonld_product(item, result)
        elif isinstance(data, dict):
            _extract_jsonld_product(data, result)

    return result


def _extract_jsonld_product(data: dict, result: dict):
    """Extract product data from a JSON-LD object."""
    if data.get("@type") not in ("Product", "IndividualProduct"):
        return

    if "name" in data and result["product_title"] is None:
        result["product_title"] = data["name"]

    offers = data.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}

    if isinstance(offers, dict):
        if "price" in offers and result["live_price"] is None:
            try:
                result["live_price"] = int(float(offers["price"]))
            except (ValueError, TypeError):
                pass

        availability = offers.get("availability", "")
        if availability and result["in_stock"] is None:
            result["in_stock"] = "InStock" in str(availability)


def _parse_from_css(soup: BeautifulSoup) -> dict:
    """Strategy C: CSS selector-based extraction (least reliable)."""
    result = {
        "product_title": None,
        "live_price": None,
        "mrp": None,
        "in_stock": None,
        "delivery_date": None,
    }

    # Price selectors (Flipkart changes these frequently)
    price_selectors = [
        "._30jeq3",
        ".Nx9bqj._4b5DiR",
        ".Nx9bqj",
        "[class*='sellingPrice']",
        "div[class*='price'] div[class*='selling']",
    ]
    for selector in price_selectors:
        el = soup.select_one(selector)
        if el:
            price = _extract_price_from_text(el.get_text())
            if price:
                result["live_price"] = price
                break

    # MRP selectors
    mrp_selectors = [
        ".yRaY8j",
        "._3I9_wc",
        "[class*='mrp']",
        "div[class*='price'] div[class*='base']",
    ]
    for selector in mrp_selectors:
        el = soup.select_one(selector)
        if el:
            price = _extract_price_from_text(el.get_text())
            if price:
                result["mrp"] = price
                break

    return result


def _extract_title(soup: BeautifulSoup) -> str | None:
    """Extract product title from page."""
    # Try common title selectors
    for selector in [".B_NuCI", ".yhB1nd", "h1[class*='title']", "h1 span"]:
        el = soup.select_one(selector)
        if el and len(el.get_text(strip=True)) > 3:
            return el.get_text(strip=True)

    # Try meta og:title
    meta = soup.find("meta", property="og:title")
    if meta and meta.get("content"):
        return meta["content"]

    # Try page title
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)
        # Remove common suffixes
        for suffix in [" - Buy", " | Flipkart", " Online at Best"]:
            if suffix in title:
                title = title[:title.index(suffix)]
        if len(title) > 3:
            return title

    return None


def _check_stock_from_text(soup: BeautifulSoup) -> bool | None:
    """Check stock status from page text indicators."""
    page_text = soup.get_text().lower()

    oos_indicators = [
        "currently unavailable",
        "sold out",
        "out of stock",
        "coming soon",
        "not available",
        "this item is currently out of stock",
    ]
    for indicator in oos_indicators:
        if indicator in page_text:
            return False

    # If we see price or add to cart, it's likely in stock
    in_stock_indicators = ["add to cart", "buy now"]
    for indicator in in_stock_indicators:
        if indicator in page_text:
            return True

    return None


def _extract_delivery_from_text(soup: BeautifulSoup) -> str | None:
    """Extract delivery date from page text."""
    page_text = soup.get_text()

    # Pattern: "Delivery by Day, Mon Date" or "Delivery by Date Mon"
    delivery_patterns = [
        r'[Dd]elivery by\s+\w+,?\s+(\d{1,2}\s+\w+\s+\d{4})',
        r'[Dd]elivery by\s+\w+,?\s+(\d{1,2}\s+\w+)',
        r'[Dd]elivery by\s+(\w+\s+\d{1,2})',
        r'[Ee]stimated delivery[:\s]+(\d{1,2}\s+\w+\s*\d{0,4})',
        r'[Gg]et it by\s+\w+,?\s+(\d{1,2}\s+\w+)',
        r'[Dd]elivered by\s+(\d{1,2}\s+\w+)',
    ]

    for pattern in delivery_patterns:
        match = re.search(pattern, page_text)
        if match:
            return match.group(1).strip()

    # Also check for delivery info in specific elements
    for selector in ["[class*='delivery']", "[class*='serviceability']", "[class*='promise']"]:
        for el in soup.select(selector):
            text = el.get_text()
            for pattern in delivery_patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(1).strip()

    return None


def _extract_price_from_text(text: str) -> int | None:
    """Extract numeric price from text like '₹1,299' or 'Rs. 999'."""
    if not text:
        return None
    # Remove currency symbols and commas
    cleaned = re.sub(r'[₹Rs.\s,]', '', text)
    match = re.search(r'(\d+)', cleaned)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def calculate_delivery_days(delivery_date_str: str | None) -> str:
    """Calculate days from today to delivery date."""
    if not delivery_date_str or delivery_date_str == "N/A":
        return "N/A"

    today = datetime.now().date()

    # Try various date formats
    date_formats = [
        "%d %b %Y",      # 18 Mar 2026
        "%d %B %Y",      # 18 March 2026
        "%d %b",          # 18 Mar (assume current year)
        "%d %B",          # 18 March
        "%b %d",          # Mar 18
        "%B %d",          # March 18
        "%d/%m/%Y",       # 18/03/2026
    ]

    for fmt in date_formats:
        try:
            parsed = datetime.strptime(delivery_date_str.strip(), fmt).date()
            # If year not in format, assume current year
            if parsed.year == 1900:
                parsed = parsed.replace(year=today.year)
                # If date is in the past, it's probably next year
                if parsed < today:
                    parsed = parsed.replace(year=today.year + 1)
            days = (parsed - today).days
            return str(max(0, days))
        except ValueError:
            continue

    return "N/A"
