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

    For each URL in column A of the sheet:
      - If the URL exists in the JSON results, update its info.
      - If not, fill the row with "n/a" for all other columns.

    Args:
        service_account (dict): Google service account credentials.
        scopes (list): Google API scopes.
        spreadsheet_id (str): Spreadsheet ID.
        remain (int): Remaining credits to log per row.
    """
    # --- Load JSON results ---
    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    # Build dictionary: url -> data
    results_map = {item.get("_url", "").strip(): item for item in results}

    # Columns we’ll fill after URL
    headers = [
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

    logger.info("Fetching URLs from Google Sheet...")
    urls = sheet.col_values(1)  # Column A

    # Skip header row if present
    start_row = 2 if urls and urls[0].lower() == "url" else 1

    # Prepare update data
    updated_values = []

    for i in range(start_row, len(urls) + 1):
        url = urls[i - 1].strip()
        item = results_map.get(url)

        if item:
            price_raw = item.get("price", "")
            if price_raw:
                price = price_raw.replace('.', '').replace(',', '').strip()
            else:
                price = ""

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
            row_values = ["n/a"] * (len(headers) - 1) + [remain]  # keep remain numeric if needed

        updated_values.append(row_values)

    # --- Update sheet in batch ---
    logger.info("Updating Google Sheet with matched data...")
    start_col = 2  # column B
    end_col = start_col + len(headers) - 1

    # Define the update range dynamically (e.g. B2:J100)
    start_cell = gspread.utils.rowcol_to_a1(start_row, start_col)
    end_cell = gspread.utils.rowcol_to_a1(len(urls), end_col)
    update_range = f"{start_cell}:{end_cell}"

    sheet.update(update_range, updated_values)
    logger.info("Finished updating Google Sheet successfully.")
