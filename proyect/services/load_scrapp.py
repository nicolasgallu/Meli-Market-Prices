from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread
import json
import os

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')


def post_results_to_sheet(service_account=None, scopes=None, spreadsheet_id=None, remain=0):
    """
    Update the 'urls' sheet with scraping results from JSON.
    Overwrites the header row and fills each row by matching URLs.

    For each URL in column A:
      - If URL exists in JSON, update values.
      - If not, fill the rest of the row with 'n/a'.
    """

    # --- Load JSON results ---
    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    # Map: url → data
    results_map = {item.get("_url", "").strip(): item for item in results}

    # --- Define headers (always overwrite them) ---
    headers = [
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

    # --- Connect to Google Sheets ---
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("urls")

    # --- Always overwrite the header row ---
    logger.info("Writing header row to Google Sheet...")
    sheet.update("A1", [headers])

    # --- Get URLs from column A (skip header) ---
    urls = sheet.col_values(1)[1:]  # skip the first row (header)
    if not urls:
        logger.warning("No URLs found in column A. Nothing to update.")
        return

    # --- Build update data ---
    updated_values = []
    for url in urls:
        url = url.strip()
        item = results_map.get(url)

        if item:
            price_raw = item.get("price", "")
            price = price_raw.replace('.', '').replace(',', '').strip() if price_raw else ""

            row_values = [
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
        else:
            # URL not found in JSON → fill with 'n/a'
            row_values = ["n/a"] * (len(headers) - 2) + [remain]

        updated_values.append(row_values)

    # --- Write results starting from column B ---
    logger.info("Updating rows with matching data...")
    start_row = 2
    start_col = 2  # column B
    end_col = start_col + len(headers) - 2  # because column A is URL

    start_cell = gspread.utils.rowcol_to_a1(start_row, start_col)
    end_cell = gspread.utils.rowcol_to_a1(start_row + len(urls) - 1, end_col)
    update_range = f"{start_cell}:{end_cell}"

    sheet.update(update_range, updated_values)

    logger.info("Finished updating Google Sheet successfully.")
