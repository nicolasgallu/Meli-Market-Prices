from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json
import os

SPREADSHEET_ID = "1wYYkvwUcjdy63SbJmIwh2ixdfQXHkimMmp2uAgb8III"
WORKSHEET_NAME = "urls"
COLUMN = "A"

# --- Set up paths ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
URLS_JSON_PATH = os.path.join(DATABASE_DIR, 'urls.json')

def get_urls_from_sheet(serive_account=None):
    creds = ServiceAccountCredentials.from_json_keyfile_name(serive_account, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    urls = sheet.col_values(1)
    urls = [url for url in urls if url and url.startswith('http')]

    with open(URLS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({"urls": urls}, f, ensure_ascii=False, indent=2)
