from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json
import os
import time

# --- PATHS ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')

def post_results_to_sheet(service_account=None, scopes=None, spreadsheet_id=None, remain=0):
    """Post scraping results to a Google Sheet, protecting it for the service account only."""

    with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
        results = json.load(f)

    header = [
        "url","title","price","competitor","price_in_installments",
        "image","timestamp","status","api_cost_total","remaining_credits"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet("Scrapping")

    # --- TRUE SHEET PROTECTION ---
    service_email = service_account.get("client_email")  # the service account email
    body = {
        "requests": [
            {
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": sheet.id
                        },
                        "description": "Locked during data update",
                        "warningOnly": False,
                        "editors": {
                            "users": [service_email]  # <-- explicitly allow the service account
                        }
                    }
                }
            }
        ]
    }
    sheet.spreadsheet.batch_update(body)

    # --- Prepare current data ---
    existing_data = sheet.get_all_records()
    existing_urls = [row["url"] for row in existing_data] if existing_data else []

    if not existing_data:
        sheet.insert_row(header, 1)
        existing_urls = []

    # --- Update rows ---
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
            row_index = existing_urls.index(url) + 2  # +2 for header
            sheet.update(f"A{row_index}:J{row_index}", [new_row])
        else:
            sheet.append_row(new_row, value_input_option="USER_ENTERED")
            existing_urls.append(url)

    # --- REMOVE PROTECTION ---
    protections = sheet.list_protected_ranges()
    for p in protections:
        sheet.delete_protection(p)
