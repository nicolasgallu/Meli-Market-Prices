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
    Update Google Sheet 'urls' with scraping results based on matching URLs.

    Behavior:
      - If the sheet only has the 'url' column, it will create the rest of the headers.
      - For each URL in column A:
          * If it exists in the JSON, update its data.
          * If not, fill cells with "n/a".
    """
    # --- Load JSON results ---
    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    # Build mapping: url -> data
    results_map = {item.get("_url", "").strip(): item for item in results}

    # --- Columns after URL ---
    json_headers = [
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

    # --- Get current headers and URLs ---
    existing_headers = sheet.row_values(1)
    urls = sheet.col_values(1)

    # If sheet is empty or only has "url" column, write full header row
    if not existing_headers or (len(existing_headers) == 1 and existing_headers[0].lower() == "url"):
        full_header = ["url"] + json_headers
        sheet.update("A1", [full_header])
        existing_headers = full_header
        logger.info("Header row created or updated.")

    # Skip header row
    start_row = 2
    if not urls:
        logger.warning("No URLs found in column A. Nothing to update.")
        return

    # Prepare updated values
    updated_values = []

    for i in range(start_row, len(urls) + 1):
        url = urls[i - 1].strip()
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
            # URL not in JSON → fill with "n/a"
            row_values = ["n/a"] * (len(json_headers) - 1) + [remain]

        updated_values.append(row_values)

    # --- Update sheet in one batch ---
    logger.info("Updating Google Sheet with matched data...")
    start_col = 2  # Column B
    end_col = start_col + len(json_headers) - 1
    start_cell = gspread.utils.rowcol_to_a1(start_row, start_col)
    end_cell = gspread.utils.rowcol_to_a1(len(urls), end_col)
    update_range = f"{start_cell}:{end_cell}"

    sheet.update(update_range, updated_values)
    logger.info("Finished updating Google Sheet successfully.")
