from proyect.services.get_urls import get_urls_from_sheet  
from proyect.services.scrapp_mercadolibre import run_scrapping   
from proyect.services.scrapp_failed import run_scrapp_failed    
from proyect.services.data_cleanup import cleaning
from proyect.services.post_scrapp import post_results_to_sheet  
from proyect.services.clean_all_jsons import clean_all_jsons
from google_manager import load_service_account

def scrape(request):
    """Entrypoint for Google Cloud Functions"""
    serive_account = load_service_account()

    get_urls_from_sheet(serive_account)
    run_scrapping()
    run_scrapp_failed()
    cleaning()
    post_results_to_sheet(serive_account)
    clean_all_jsons()
    return "Scraping done!"

