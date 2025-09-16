from proyect.services.google_manager import load_service_account
from proyect.services.get_urls import get_urls_from_sheet  
from proyect.services.scrapp_meli import run_scrapping   
from proyect.services.scrapp_failed import run_scrapp_failed    
from proyect.services.json_transform import build_union
from proyect.services.post_scrapp import post_results_to_sheet  
from proyect.services.delete_data_jsons import clean_all_jsons
from dotenv import load_dotenv
import os

load_dotenv()
spreadsheet_id = os.getenv("SPREADSHEET_ID")
scopes = os.getenv("SCOPES").split(",")
api_key = os.getenv("SCRAPFLY_API_KEY")

def main():
    """Entrypoint for Google Cloud Functions"""
    #serive_account = load_service_account()
    #get_urls_from_sheet(serive_account, scopes, spreadsheet_id)
    run_scrapping(api_key)
    run_scrapp_failed(api_key)
    build_union()
    #post_results_to_sheet(serive_account, scopes, spreadsheet_id)
    #clean_all_jsons()

main()