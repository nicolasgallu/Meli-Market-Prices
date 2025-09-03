import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from dotenv import load_dotenv
load_dotenv()

# --- Load environment variables ---
GOOGLE_CREDS = "credenciales.json"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
WORKSHEET_NAME = "Scrapping"

# --- Set up paths ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'scrap_results.json')


def post_results_to_sheet(json_file=RESULTS_JSON_PATH):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    results = data.get("results", [])
    timestamp = data.get("timestamp", [])

    header = ["url", "Título", "Precio con Centavos", "Competidor", "Precio en Cuotas", "Imagen","timestamp"]
    rows = [header]
    for item in results:
        row = [
            item.get("url", ""),
            item.get("Título", ""),
            item.get("Precio con Centavos", ""),
            item.get("Competidor", ""),
            item.get("Precio en Cuotas", ""),
            item.get("Imagen", ""),
            timestamp
        ]
        rows.append(row)
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS, ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    sheet.clear()
    sheet.update('A1', rows)
