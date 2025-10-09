from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread
import json
import os

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')


def post_results_to_sheet(service_account=None, scopes=None, spreadsheet_id=None, remain=0):
    """Post scraping results from a JSON file to a Google Sheet.

    Args:
        service_account (dict): Service account credentials for Google Sheets API.
        spreadsheet_id (str): The ID of the Google Sheet to post results to.
    """

    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

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
    rows = [header]

    logger.info("Posting results to Google Sheet...")

    for item in results:
        # Format price as integer: remove dots and commas
        price_raw = item.get("price", "")
        price = price_raw.replace('.', '').replace(',', '').strip() if price_raw else ""
        
        row = [
            item.get("_url", ""),
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
        rows.append(row)

    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("Scrapping")

    sheet.clear()
    sheet.update('A1', rows)

    logger.info("Finished posting results to Google Sheet.")