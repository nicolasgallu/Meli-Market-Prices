from scrapfly import ScrapeConfig, ScrapflyClient
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import asyncio
import random
import json
import os

# --- Load environment variables ---
load_dotenv()
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

# --- Set up paths ---
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '../database')
URLS_JSON_PATH = os.path.join(DATABASE_DIR, 'urls.json')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'scrap_results.json')

def parse_product(html):
    """Parse product details from HTML and return as a dictionary."""
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    titulo = soup.find("h1", class_="ui-pdp-title")
    data["Title"] = titulo.text.strip() if titulo else "Title not found"

    precio_span = soup.find("span", class_="andes-money-amount__fraction")
    data["Price"] = precio_span.text.strip() if precio_span else ""
    
    competidor_h2 = soup.find("h2", class_="ui-seller-data-header__title")
    data["Competitor"] = competidor_h2.text.strip() if competidor_h2 else "Competitor not found"
    
    cuotas = soup.find("div", class_="ui-pdp-price__subtitles")
    data["Price in Installments"] = cuotas.text.strip() if cuotas else "Installments not found"
    
    img = soup.find("img", class_="ui-pdp-image")
    data["Image"] = img["src"] if img and img.get("src") else "Image not found"
    return data

async def robust_scrape_single(url, client, semaphore, max_retries=5):
    async with semaphore:
        for attempt in range(max_retries):
            try:
                config = ScrapeConfig(
                    url=url,
                    asp=True,
                    proxy_pool="residential",
                    country="AR",
                    render_js=False,
                    cache=True,
                )
                response = await asyncio.to_thread(client.scrape, config)
                if response.status_code == 200:
                    product_data = parse_product(response.content)
                    return {"url": url, **product_data}
                else:
                    print(f"Non-200 status ({response.status_code}) for {url}, retrying...")
            except Exception as e:
                print(f"Error scraping {url}: {e}, retrying...")
            await asyncio.sleep(random.uniform(5, 15))
        print(f"Failed to scrape {url} after multiple attempts.")
        return {"url": url, "Título": "Error", "Precio con Centavos": "Error", "Competidor": "Error", "Precio en Cuotas": "Error", "Imagen": "Error"}

async def robust_scrape_async(urls, concurrency=3):
    semaphore = asyncio.Semaphore(concurrency)
    client = ScrapflyClient(key=SCRAPFLY_API_KEY)
    tasks = [robust_scrape_single(url, client, semaphore) for url in urls]
    return await asyncio.gather(*tasks)

def run_scrapping():
    with open(URLS_JSON_PATH, "r", encoding="utf-8") as f:
        urls = json.load(f)["urls"]

    results = asyncio.run(robust_scrape_async(urls, concurrency=3))
    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "results": results
        }

    with open(RESULTS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
