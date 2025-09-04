from proyect.services.get_urls import get_urls_from_sheet  
from proyect.services.scrapp_mercadolibre import run_scrapping      
from proyect.services.post_scrapp import post_results_to_sheet  


if __name__ == "__main__":
    get_urls_from_sheet()
    run_scrapping()
    post_results_to_sheet()