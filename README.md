# Flipkart FSN Scraper — Frido

Automated scraper that monitors Flipkart product availability, delivery dates, and pricing across 4 major cities. Runs every 6 hours via GitHub Actions.

## What it does

- Reads FSN list from Google Sheets "FSN Master" tab
- Scrapes each product page for live price, stock status, and delivery dates
- Checks 4 pincodes: Delhi (110001), Bangalore (560001), Mumbai (400001), Pune (411001)
- Flags price discrepancies (live price != seller price)
- Outputs to CSV backup + 3 Google Sheets tabs (Latest Snapshot, Historical Log, OOS Alerts)
- Sends Slack summary notification

## Setup

### 1. Google Sheets
1. Create a Google Cloud project, enable Sheets API + Drive API
2. Create a service account, download the JSON key
3. Create a Google Sheet with 4 tabs: `FSN Master`, `Latest Snapshot`, `Historical Log`, `OOS Alerts`
4. Share the sheet with your service account email (Editor)
5. Paste your Flipkart seller panel data in "FSN Master" tab

### 2. GitHub Secrets
| Secret | Description |
|--------|------------|
| `GOOGLE_SHEETS_CREDS` | Base64-encoded service account JSON: `base64 -w 0 key.json` |
| `GOOGLE_SHEET_ID` | From your Google Sheet URL |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook |
| `SCRAPINGBEE_API_KEY` | Optional fallback scraper |
| `CRAWLBASE_TOKEN` | Optional fallback scraper |

### 3. Run locally
```bash
cp .env.example .env
# Fill in .env values
pip install -r requirements.txt
python -m src.main
```

### 4. Run via GitHub Actions
Push to GitHub. The workflow runs automatically every 6 hours, or trigger manually from Actions tab.

## Architecture

```
Google Sheets "FSN Master" tab
  → main.py (orchestrator)
    → asyncio.gather() across all sub-categories (parallel)
      → per sub-category: semaphore(3) concurrent FSN scrapers
        → per FSN × 4 pincodes: fetch + parse + rate limit
  → CSV backup + Google Sheets (3 tabs) + Slack notification
```
