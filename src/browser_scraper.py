"""Playwright-based Flipkart scraper for delivery dates and pincode-specific stock."""

import asyncio
import logging
import random
import re
from datetime import datetime
from typing import Optional

from playwright.async_api import async_playwright, Browser, Page

from src.config import PINCODES, USER_AGENTS, INPUT_COLUMNS
from src.parser import calculate_delivery_days

logger = logging.getLogger(__name__)

PAGE_TIMEOUT = 30000
PINCODE_WAIT = 2500


async def create_browser():
    """Launch async Playwright + headless Chromium."""
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    logger.info("Playwright browser launched")
    return pw, browser


async def close_browser(pw, browser):
    """Shut down browser and Playwright."""
    await browser.close()
    await pw.stop()
    logger.info("Playwright browser closed")


async def _new_page(browser: Browser) -> Page:
    """Create a new page with stealth settings and resource blocking."""
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1366, "height": 768},
        extra_http_headers={
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
        },
    )
    page = await context.new_page()

    # Block heavy resources
    await page.route(
        "**/*.{png,jpg,jpeg,gif,svg,webp,woff,woff2,ttf,eot,mp4,mp3}",
        lambda route: route.abort(),
    )
    await page.route("**/ads/**", lambda route: route.abort())
    await page.route("**/analytics/**", lambda route: route.abort())

    return page


async def _close_login_popup(page: Page):
    """Dismiss Flipkart login popup if present."""
    try:
        for btn in await page.query_selector_all("button"):
            text = await btn.text_content()
            if text and ("\u2715" in text or "\u00d7" in text):
                await btn.click()
                break
    except Exception:
        pass


async def _extract_product_info(page: Page) -> dict:
    """Extract product name, price, MRP, stock from page."""
    info = {"product_title": None, "live_price": None, "mrp": None, "in_stock": None}

    try:
        data = await page.evaluate("""() => {
            let name = null, sp = null, mrp = null, available = true;

            // JSON-LD
            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (const s of scripts) {
                try {
                    const d = JSON.parse(s.textContent);
                    const product = Array.isArray(d) ? d[0] : d;
                    if (product['@type'] === 'Product' && product.name) {
                        name = product.name;
                        if (product.offers) {
                            if (product.offers.price) sp = Number(product.offers.price);
                            if (product.offers.availability && product.offers.availability.includes('OutOfStock'))
                                available = false;
                        }
                        break;
                    }
                } catch (e) {}
            }

            // MRP from body text
            const bt = document.body.innerText;
            const dm = bt.match(/(\\d+)%\\s+([\\d,]+)\\s+\\u20B9([\\d,]+)/);
            if (dm) {
                mrp = parseInt(dm[2].replace(/,/g, ''));
                if (!sp) sp = parseInt(dm[3].replace(/,/g, ''));
            }

            // Fallback title from meta
            if (!name) {
                const og = document.querySelector('meta[property="og:title"]');
                if (og) name = og.content;
            }

            // Out of stock from text
            if (bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock'))
                available = false;

            return { name, sp, mrp, available };
        }""")

        info["product_title"] = data.get("name")
        info["live_price"] = data.get("sp")
        info["mrp"] = data.get("mrp")
        info["in_stock"] = data.get("available")
    except Exception as e:
        logger.error(f"Product info extraction error: {e}")

    return info


async def _check_pincode(page: Page, pincode: str) -> dict:
    """Enter pincode and extract delivery info."""
    result = {"in_stock": None, "delivery_date": None}

    try:
        # Click delivery location link
        await page.evaluate("""() => {
            const els = document.querySelectorAll('a, div, span');
            for (const el of els) {
                const t = el.textContent.trim();
                if (t === 'Select delivery location' || t === 'Change' || t === 'Enter pincode') {
                    el.click();
                    break;
                }
            }
        }""")

        # Wait for pincode input
        try:
            await page.wait_for_function("""() => {
                const inputs = document.querySelectorAll('input');
                for (const inp of inputs) {
                    const ph = (inp.placeholder || '').toLowerCase();
                    if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) return true;
                }
                return false;
            }""", timeout=5000)
        except Exception:
            pass

        # Fill pincode using native setter (React-compatible)
        await page.evaluate("""(pin) => {
            const inputs = document.querySelectorAll('input');
            for (const inp of inputs) {
                const ph = (inp.placeholder || '').toLowerCase();
                if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) {
                    const nativeSet = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    nativeSet.call(inp, pin);
                    inp.dispatchEvent(new Event('input', { bubbles: true }));
                    inp.dispatchEvent(new Event('change', { bubbles: true }));
                    break;
                }
            }
        }""", pincode)
        await asyncio.sleep(0.5)

        # Click Apply/Check
        await page.evaluate("""() => {
            const btns = document.querySelectorAll('button, span');
            for (const b of btns) {
                const t = b.textContent.trim();
                if (t === 'Apply' || t === 'Check' || t === 'Submit') { b.click(); break; }
            }
        }""")
        await asyncio.sleep(PINCODE_WAIT / 1000)

        # Extract delivery info
        delivery = await page.evaluate("""() => {
            const bt = document.body.innerText;
            if (bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock'))
                return { available: false, dd: 'N/A' };
            if (bt.includes('not serviceable') || bt.includes('Cannot be delivered'))
                return { available: false, dd: 'Not Serviceable' };

            let dd = 'N/A';
            const patterns = [
                /Delivery\\s+by\\s+(\\d+\\s+\\w+,?\\s*\\w*)/i,
                /Delivery\\s*\\n\\s*by\\s+(\\d+\\s+\\w+,?\\s*\\w*)/i,
                /Get it by\\s+(\\d+\\s+\\w+,?\\s*\\w*)/i,
            ];
            for (const p of patterns) {
                const m = bt.match(p);
                if (m) { dd = m[1].trim().replace(/,?\\s*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/i, ''); break; }
            }
            return { available: true, dd };
        }""")

        result["in_stock"] = delivery.get("available", None)
        result["delivery_date"] = delivery.get("dd", "N/A")

    except Exception as e:
        logger.error(f"Pincode {pincode} check error: {e}")

    return result


async def scrape_fsn_with_browser(
    browser: Browser, fsn: str, row: dict, pincodes: dict
) -> list:
    """Scrape one FSN across all pincodes using Playwright. Returns list of result dicts."""
    results = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sub_category = row.get(INPUT_COLUMNS["subcategory"], "")
    product_title = row.get(INPUT_COLUMNS["title"], "")
    seller_price = _safe_int(row.get(INPUT_COLUMNS.get("selling_price", ""), ""))

    page = await _new_page(browser)

    try:
        # Load product page
        url = f"https://www.flipkart.com/product/p/itm?pid={fsn}"
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

        # Wait for content
        try:
            await page.wait_for_function(
                """() => document.querySelector('script[type="application/ld+json"]')
                    || document.body.innerText.length > 500""",
                timeout=10000,
            )
        except Exception:
            pass

        await asyncio.sleep(1.5)
        await _close_login_popup(page)
        await asyncio.sleep(0.3)

        # Extract product info (once for all pincodes)
        info = await _extract_product_info(page)
        if info["product_title"]:
            product_title = info["product_title"]

        # Check each pincode
        for pincode, city in pincodes.items():
            delivery = await _check_pincode(page, pincode)

            in_stock = info["in_stock"]
            if delivery["in_stock"] is not None:
                in_stock = in_stock and delivery["in_stock"] if in_stock is not None else delivery["in_stock"]

            live_price = info.get("live_price")
            mrp = info.get("mrp")
            discount_pct = ""
            if live_price and mrp and mrp > 0:
                discount_pct = f"{round((1 - live_price / mrp) * 100)}%"

            price_match = ""
            if live_price is not None and seller_price is not None:
                price_match = "YES" if live_price == seller_price else "NO"

            delivery_date = delivery.get("delivery_date", "N/A")

            results.append({
                "timestamp": timestamp,
                "fsn": fsn,
                "seller_sku": row.get(INPUT_COLUMNS.get("sku", ""), ""),
                "sub_category": sub_category,
                "product_title": product_title,
                "seller_price": seller_price,
                "live_price": live_price,
                "mrp": mrp,
                "discount_pct": discount_pct,
                "price_match": price_match,
                "fulfillment_by": row.get(INPUT_COLUMNS.get("fulfillment", ""), ""),
                "seller_stock": _safe_int(row.get(INPUT_COLUMNS.get("stock", ""), "")),
                "pincode": pincode,
                "city": city,
                "in_stock": in_stock,
                "delivery_date": delivery_date,
                "delivery_days": calculate_delivery_days(delivery_date),
                "scrape_status": "success",
                "error_message": "",
            })

    except Exception as e:
        logger.error(f"Browser scrape failed for FSN {fsn}: {e}")
        # Return error rows for all pincodes
        for pincode, city in pincodes.items():
            results.append({
                "timestamp": timestamp,
                "fsn": fsn,
                "seller_sku": row.get(INPUT_COLUMNS.get("sku", ""), ""),
                "sub_category": sub_category,
                "product_title": product_title,
                "seller_price": seller_price,
                "live_price": None,
                "mrp": None,
                "discount_pct": "",
                "price_match": "",
                "fulfillment_by": "",
                "seller_stock": None,
                "pincode": pincode,
                "city": city,
                "in_stock": None,
                "delivery_date": "N/A",
                "delivery_days": "N/A",
                "scrape_status": "failed",
                "error_message": str(e)[:200],
            })
    finally:
        await page.context.close()

    return results


def _safe_int(val) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None
