from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import datetime
import gspread
import json
import os

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')

def post_results_to_sheet(serive_account=None, scopes=None, spreadsheet_id=None):
    """Post scraping results from a JSON file to a Google Sheet.
    Args:
        serive_account (dict): Service account credentials for Google Sheets API.
        spreadsheet_id (str): The ID of the Google Sheet to post results to.
    """
    
    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    header = [
        "url", 
        "Title", 
        "Price", 
        "Competitor", 
        "Price in Installments", 
        "Image", 
        "timestamp"
    ]
    rows = [header]
    logger.info("Posting results to Google Sheet...")
    for item in results:
        iso_ts = item.get("_timestamp", "")
        if iso_ts:
            try:
                dt = datetime.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
                ts = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = ""
        else:
            ts = ""
        # Format price as integer: remove dots and commas
        price_raw = item.get("Price", "")
        if price_raw:
            price = price_raw.replace('.', '').replace(',', '').strip()
        else:
            price = ""
        row = [
            item.get("url", ""),
            item.get("Title", ""),
            price,
            item.get("Competitor", ""),
            item.get("Price in Installments", ""),
            item.get("Image", ""),
            ts
        ]
        rows.append(row)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(serive_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("Scrapping")
    sheet.clear()
    sheet.update('A1', rows)
    logger.info("finished posting results to Google Sheet.")
    logger.info("TEST TEST TEST")




