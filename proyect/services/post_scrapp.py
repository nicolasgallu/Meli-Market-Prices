import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import datetime

# --- Load environment variables ---
SPREADSHEET_ID = "1wYYkvwUcjdy63SbJmIwh2ixdfQXHkimMmp2uAgb8III"
WORKSHEET_NAME = "Scrapping"

# --- Set up paths ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')


def post_results_to_sheet(serive_account=None, json_file=RESULTS_JSON_PATH):
    with open(json_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    header = ["url", "Title", "Price", "Competitor", "Price in Installments", "Image", "timestamp"]
    rows = [header]
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
    creds = ServiceAccountCredentials.from_json_keyfile_dict(serive_account, ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    sheet.clear()
    sheet.update('A1', rows)




