from proyect.services.google_manager import load_service_account
from proyect.services.get_urls import get_urls_from_sheet  
from proyect.services.scrapp_mercadolibre import run_scrapping   
from proyect.services.scrapp_failed import run_scrapp_failed    
from proyect.services.data_cleanup import cleaning
from proyect.services.post_scrapp import post_results_to_sheet  
from dotenv import load_dotenv
import os

load_dotenv()
spreadsheet_id = os.getenv("SPREADSHEET_ID")
scopes = os.getenv("SCOPES").split(",")

if __name__ == "__main__":
    """Entrypoint for Google Cloud Functions"""
    serive_account = load_service_account()
    get_urls_from_sheet(serive_account, scopes, spreadsheet_id)
    run_scrapping()
    run_scrapp_failed()
    cleaning()
    post_results_to_sheet(serive_account, scopes, spreadsheet_id)
