from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread
import json
import os
import time

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')

def post_results_to_sheet(service_account=None, scopes=None, spreadsheet_id=None, remain=0):
    """Post scraping results from a JSON file to a Google Sheet.
    Matches rows by 'url' (column A) instead of overwriting everything.
    Locks the sheet while posting to avoid concurrent edits.
    """

    # --- Load JSON results ---
    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    # --- Prepare header and rows ---
    header = [
        "url",
        "title",
        "price",
        "competitor",
        "price_in_installments",
        "image",
        "timestamp",
        "status",
        "api_cost_total",
        "remaining_credits"
    ]

    logger.info("Connecting to Google Sheet...")

    # --- Connect to Google Sheets ---
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("Scrapping")

    # --- LOCK SHEET ---
    logger.info("Locking sheet for update...")
    sheet.protected = True  # basic flag for readability
    sheet.add_protected_range('A:Z', description="Locked during data update", warning_only=False)
    time.sleep(1)

    # --- Get current data ---
    existing_data = sheet.get_all_records()
    existing_urls = [row["url"] for row in existing_data] if existing_data else []

    # --- Prepare updates ---
    logger.info("Matching existing URLs and preparing updates...")

    # Make sure the header exists (first row)
    if not existing_data:
        sheet.insert_row(header, 1)
        existing_urls = []

    for item in results:
        url = item.get("_url", "")
        price_raw = item.get("price", "")
        price = price_raw.replace('.', '').replace(',', '').strip() if price_raw else ""

        new_row = [
            url,
            item.get("title", ""),
            price,
            item.get("competitor", ""),
            item.get("price_in_installments", ""),
            item.get("image", ""),
            item.get("_timestamp", ""),
            item.get("_status", ""),
            item.get("_api_cost_total", ""),
            remain
        ]

        if url in existing_urls:
            # Update the existing row
            row_index = existing_urls.index(url) + 2  # +2 because list starts at 0 and header is row 1
            sheet.update(f"A{row_index}:J{row_index}", [new_row])
        else:
            # Append new row
            sheet.append_row(new_row, value_input_option="USER_ENTERED")
            existing_urls.append(url)

    # --- UNLOCK SHEET ---
    logger.info("Unlocking sheet...")
    protections = sheet.list_protected_ranges()
    for p in protections:
        sheet.delete_protection(p)

    logger.info("Finished posting results to Google Sheet.")
