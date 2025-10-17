from proyect.services.google_manager import load_service_account
from proyect.services.extract_urls import get_urls_from_sheet  
from proyect.services.first_scrapp import scrap_meli_urls   
from proyect.services.second_scrapp import scrap_urls_failed    
from proyect.services.json_merge import merge_scraping
from proyect.services.budget import remain_budget
from proyect.services.load_scrapp import post_results_to_sheet  
from proyect.services.cleaning_jsons import remove_data
from proyect.services.protection import SheetProtector
from dotenv import load_dotenv
import os

load_dotenv()
spreadsheet_id = os.getenv("SPREADSHEET_ID")
scopes = os.getenv("SCOPES").split(",")
api_key = os.getenv("SCRAPFLY_API_KEY")

def main():
    """Entrypoint for Google Cloud Functions"""
    service_account = load_service_account()
    get_urls_from_sheet(service_account, scopes, spreadsheet_id)
    protecion = SheetProtector(service_account, scopes, spreadsheet_id)
    protecion.protect()
    scrap_meli_urls(api_key)
    scrap_urls_failed(api_key)
    merge_scraping()
    remain = remain_budget(api_key)
    post_results_to_sheet(service_account, scopes, spreadsheet_id, remain)
    remove_data()
    protecion.unprotect()
main()
