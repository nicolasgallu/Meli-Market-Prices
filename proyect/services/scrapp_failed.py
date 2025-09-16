from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyScrapeError
from proyect.utils.logger import logger
from datetime import datetime
from bs4 import BeautifulSoup
import os, json, uuid, time

# ──────────────────────────────────────────────────────────────────────────────
# PATHS / CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, '../database')
FAILED_JSON_PATH = os.path.join(DATABASE_DIR, 'failed_urls.json')
OUTPUT_JSON_PATH = os.path.join(DATABASE_DIR, 'scrapping_failed_urls.json')

DISCARD_PHRASE = "Este producto no está disponible. Elige otra variante."

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def now_ts():
    """Return current time formatted to seconds (ISO style)."""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def write_results(rows):
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def read_failed():
    """Read failed_urls.json."""
    with open(FAILED_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [row["url"]["_url"] for row in data]


def api_cost(response):
    """Returns the cost of the API call to Scrapfly"""
    cost = getattr(getattr(response, "response", None), "headers", {}).get("X-Scrapfly-Api-Cost", "n/a")
    return cost


def parse_product(url, response):
    """Scraping & Parser Workflow"""

    # 1. Parse HTML
    html = response.content
    soup = BeautifulSoup(html, "html.parser")

    # 2. Check availability
    msg_el = soup.find(class_="ui-pdp-shipping-message__text")
    if msg_el and DISCARD_PHRASE in msg_el.get_text(strip=True):
        parsed = {
            "url": url,
            "_status": "Discard",
            "_timestamp": now_ts(),
            "_api_cost": api_cost(response),
        }
        logger.warning(f"discarded (not available): {url}")
        return parsed
    
    # 3. Extract fields
    t = soup.find("h1", class_="ui-pdp-title")
    p = soup.find("span", class_="andes-money-amount__fraction")
    c = soup.find("h2", class_="ui-seller-data-header__title")
    q = soup.find("div", class_="ui-pdp-price__subtitles")
    img = soup.find("img", class_="ui-pdp-image")
    parsed = {
        "url": url,
        "_timestamp": now_ts(),
        "_api_cost": api_cost(response),
        "Title": (t.text.strip() if t else "n/a"),
        "Price": (p.text.strip() if p else "n/a"),
        "Competitor": (c.text.strip() if c else "n/a"),
        "Price in Installments": (q.text.strip() if q else "n/a"),
        "Image": (img["src"] if (img and img.get("src")) else "n/a"),
    }
    
    # 4. Validate parse
    if parsed["Title"] == "n/a":
        parsed["_status"] = "Failed"
        logger.error(f"failed to parse title from url: {url}")
        return parsed
    
    # 4. Success
    else:
        parsed["_status"] = "Success"
        logger.info(f"success: {url}")
        return parsed


# ──────────────────────────────────────────────────────────────────────────────
# CORE: scrape a single failed URL (two attempts)
# ──────────────────────────────────────────────────────────────────────────────
def scrape_one(client, session_id, url):
    """Attempt to scrape a single failed URL (up to 2 tries)."""

    # Default Scrapfly config (retry enabled, sticky session; we pop 'timeout' per attempt)
    base = dict(
        asp=True,
        render_js=True,
        wait_for_selector="h1.ui-pdp-title",
        proxy_pool="public_residential_pool",
        country="ar",
        lang=["es-AR", "es"],
        session_sticky_proxy=True,
        retry=True,
    )
    base["session"] = session_id

    # 1. First Attempt (plain)
    base1 = base.copy()
    base1.pop("timeout", None)
    cfg1 = ScrapeConfig(url=url, **base1)
    try:
        response = client.scrape(cfg1)
        parsed = parse_product(url, response)
        if parsed.get("_status") in ["Discard","Success"]:
            return parsed
    except ScrapflyScrapeError:
        pass
    except Exception:
        pass

    # 2. Second Attempt (slightly more patient)
    base2 = base.copy()
    base2.update({"rendering_wait": 10_000, "auto_scroll": True})
    base2.pop("timeout", None)
    cfg2 = ScrapeConfig(url=url, **base2)
    try:
        response = client.scrape(cfg2)
        parsed = parse_product(url, response)
        if parsed.get("_status") in ["Discard","Success"]:
            return parsed
        
    # 3.If nothing works we post the error
    except ScrapflyScrapeError as e2:
        return {
            "url": url,
            "_status": "ERROR",
            "_timestamp": now_ts(),
            "Title": "ERROR",
            "Price": "n/a",
            "Competitor": "n/a",
            "Price in Installments": "n/a",
            "Image": "n/a",
            "_error": getattr(e2, "error_code", "") or type(e2).__name__,
        }
    except Exception as e2:
        return {
            "url": url,
            "_status": "ERROR",
            "_timestamp": now_ts(),
            "Title": "ERROR",
            "Price": "n/a",
            "Competitor": "n/a",
            "Price in Installments": "n/a",
            "Image": "n/a",
            "_error": f"UNEXPECTED {type(e2).__name__}",
        }

# ──────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR: process all failed URLs sequentially
# ──────────────────────────────────────────────────────────────────────────────
def scrape_all_failed(urls, api_key):
    client = ScrapflyClient(key=api_key)
    session_id = f"FAILED-{uuid.uuid4()}"  # one sticky session for the whole run
    results = []

    logger.info(f"Retrying {len(urls)} failed URLs..")
    for url in urls:
        out = scrape_one(client, session_id, url)
        results.append(out)
        time.sleep(0.8)  # short polite pause
    return results

# ──────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────────
def run_scrapp_failed(api_key):

    failed_urls = read_failed()
    if not failed_urls:
        logger.info("No failed URLs found - nothing to do.")
        return
    
    results = scrape_all_failed(failed_urls, api_key)
    write_results(results)

