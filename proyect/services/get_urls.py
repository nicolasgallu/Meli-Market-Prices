from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread
import json
import os

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
URLS_JSON_PATH = os.path.join(DATABASE_DIR, 'urls.json')

def get_urls_from_sheet(serive_account=None, scopes=None, spreadsheet_id=None):
    """Fetch URLs from a Google Sheet and save them to a JSON file.
    Args:
        serive_account (dict): Service account credentials for Google Sheets API.
        spreadsheet_id (str): The ID of the Google Sheet to fetch URLs from.
    """

    logger.info("Fetching URLs from Google Sheet...")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(serive_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("urls")
    urls = sheet.col_values(1)
    urls = [url for url in urls if url and url.startswith('http')]

    logger.info("Writing URLs to JSON file...")
    with open(URLS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({"urls": urls}, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(urls)} URLs to {URLS_JSON_PATH}")
