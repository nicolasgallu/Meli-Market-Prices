from proyect.services.get_urls import get_urls_from_sheet  
from proyect.services.scrapp_mercadolibre import run_scrapping   
from proyect.services.scrapp_failed import run_scrapp_failed    
from proyect.services.data_cleanup import cleaning
from proyect.services.post_scrapp import post_results_to_sheet  
from proyect.services.clean_all_jsons import clean_all_jsons

if __name__ == "__main__":
    get_urls_from_sheet()
    run_scrapping()
    run_scrapp_failed()
    cleaning()
    post_results_to_sheet()
    clean_all_jsons()