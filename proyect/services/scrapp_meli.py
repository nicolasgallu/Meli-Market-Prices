from scrapfly import ScrapflyClient, ScrapeConfig
from proyect.utils.logger import logger
import os, json, asyncio
from datetime import datetime
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────────────────────
# PATHS / CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, "../database")
URLS_JSON = os.path.join(DATABASE_DIR, "urls.json")
RESULTS_JSON = os.path.join(DATABASE_DIR, "scrap_results.json")
FAILED_JSON = os.path.join(DATABASE_DIR, "failed_urls.json")
DISCARDED_JSON = os.path.join(DATABASE_DIR, "discarded_urls.json")

DISCARD_PHRASE = "Este producto no está disponible. Elige otra variante."

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def now_ts():
    """Return current time formatted to seconds (ISO style)."""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def read_urls():
    """read urls"""
    with open(URLS_JSON, "r", encoding="utf-8") as f:
        urls = json.load(f)["urls"]
    return urls


def write_json(path, data):
    """Write a list/dict to JSON file with UTF-8 encoding."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def api_cost(response):
    """Returns the cost of the API call to Scrapfly"""
    cost = getattr(getattr(response, "response", None), "headers", {}).get("X-Scrapfly-Api-Cost", "n/a")
    return cost


# ──────────────────────────────────────────────────────────────────────────────
# CORE:
# ──────────────────────────────────────────────────────────────────────────────
async def scrape_one(client, url, discard_phrase):
    """
    Scrape one URL and return a tuple describing the result.
    """
    try:
        # 1. Build request config
        cfg = ScrapeConfig(
            url=url,
            asp=True,
            render_js=True,
            wait_for_selector="h1.ui-pdp-title",
            proxy_pool="public_residential_pool",
            country="ar",
            lang=["es-AR", "es"],
            retry=False,
            timeout=90_000,
            cost_budget=30,
        )

        # 2. Execute scrapping
        res = await client.async_scrape(cfg)
        html = res.content
        soup = BeautifulSoup(html, "html.parser")

        # 3. Check availability
        msg_el = soup.find(class_="ui-pdp-shipping-message__text")
        if msg_el and discard_phrase in msg_el.get_text(strip=True):
            parsed = {
                "_url": url,
                "_timestamp": now_ts(),
                "_api_cost": api_cost(res)
            }
            logger.warning(f"discarded (not available): {url}")
            return "discard", parsed

        # 4. Extract fields & Parsing
        t = soup.find("h1", class_="ui-pdp-title")
        p = soup.find("span", class_="andes-money-amount__fraction")
        c = soup.find("h2", class_="ui-seller-data-header__title")
        q = soup.find("div", class_="ui-pdp-price__subtitles")
        img = soup.find("img", class_="ui-pdp-image")
        parsed = {
            "Title": (t.text.strip() if t else "n/a"),
            "Price": (p.text.strip() if p else "n/a"),
            "Competitor": (c.text.strip() if c else "n/a"),
            "Price in Installments": (q.text.strip() if q else "n/a"),
            "Image": (img["src"] if (img and img.get("src")) else "n/a"),
            "_url": url,
            "_timestamp": now_ts(),
            "_api_cost": api_cost(res)
        }

        # 5. Validate if Failed
        if parsed["Title"] == "n/a":
            parsed = {
                "_url": url,
                "_timestamp": now_ts(),
                "_api_cost": api_cost(res)
            }
            logger.error(f"failed to parse title from url: {url}")
            return "failure", parsed
        
        # 6. Successed
        logger.info(f"success: {url}")
        return "success", parsed

    except Exception:
        logger.error(f"exception while scraping: {url}")
        return "failure", url


async def scrape_all(urls, api_key):
    """
    Orchestrates scraping for all URLs.
    Returns results, failures, and discarded URLs.
    """
    client = ScrapflyClient(key=api_key)
    sem = asyncio.Semaphore(5)  # limit concurrency

    results = []
    failures = []
    discarded = []
    DISCARD_PHRASE = "Este producto no está disponible. Elige otra variante."

    # --- shared counter ---
    counter = [0]  # mutable wrapper
    lock = asyncio.Lock()
    total = len(urls)

    async def job(url):
        async with sem:
            status, payload = await scrape_one(client, url, DISCARD_PHRASE)

            # update lists
            if status == "success":
                results.append(payload)
            elif status == "failure":
                failures.append({"url": payload})
            elif status == "discard":
                discarded.append({"url": payload})

            # increment counter
            async with lock:
                counter[0] += 1
                print(f"[{counter[0]}/{total}] finished {url}")

    await asyncio.gather(*(job(u) for u in urls))
    return results, failures, discarded


def run_scrapping(api_key):
    """
    Entry point: load URLs, scrape, and save results.
    """
    urls = read_urls()

    # --- Run scraping ---
    results, failures, discarded = asyncio.run(scrape_all(urls, api_key))

    # --- Write outputs ---
    write_json(RESULTS_JSON, results)
    write_json(FAILED_JSON, failures)
    write_json(DISCARDED_JSON, discarded)
