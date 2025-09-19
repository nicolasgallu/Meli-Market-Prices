from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyScrapeError
from proyect.utils.logger import logger
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os, json, uuid, time

# ---------- PATHS / ENV ----------
BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, '../database')
FAILED_JSON_PATH = os.path.join(DATABASE_DIR, 'failed_urls.json')
OUTPUT_JSON_PATH = os.path.join(DATABASE_DIR, 'scrapping_failed_urls.json')

load_dotenv()
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

# ---------- SCRAPFLY DEFAULTS (SIMPLE) ----------
TIMEOUT_MS = 120_000
BASE_CFG = dict(
    asp=True,
    render_js=True,
    wait_for_selector="h1.ui-pdp-title",
    proxy_pool="public_residential_pool",
    country="ar",
    lang=["es-AR", "es"],
    session_sticky_proxy=True,
    timeout=TIMEOUT_MS,
)

# ---------- PARSER (very simple) ----------
def parse_product(html: str):
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    t = soup.find("h1", class_="ui-pdp-title")
    data["Title"] = t.text.strip() if t else "Title not found"
    precio_container = soup.find("div", {"class": "ui-pdp-price__second-line"})
    precio_span = precio_container.find("span", {"class": "andes-money-amount__fraction"}) if precio_container else None
    data["Price"] = precio_span.text.strip() if precio_span else ""
    c = soup.find("h2", class_="ui-seller-data-header__title")
    data["Competitor"] = c.text.strip() if c else "Competitor not found"
    q = soup.find("div", class_="ui-pdp-price__subtitles")
    data["Price in Installments"] = q.text.strip() if q else "Installments not found"
    img = soup.find("img", class_="ui-pdp-image")
    data["Image"] = img["src"] if (img and img.get("src")) else "Image not found"
    return data

# ---------- IO HELPERS ----------
def read_failed():
    if not os.path.exists(FAILED_JSON_PATH):
        return []
    with open(FAILED_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for row in data:
        idx = row.get("index")
        url = row.get("url")
        if idx is not None and url:
            rows.append((idx, url))
    return rows

def write_results(rows):
    os.makedirs(DATABASE_DIR, exist_ok=True)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"Wrote -> {OUTPUT_JSON_PATH}")

# ---------- SCRAPE (single-threaded, minimal retry) ----------
def scrape_url(client: ScrapflyClient, session_id: str, idx: int, url: str):
    base = BASE_CFG.copy()
    base["session"] = session_id
    # Remove 'timeout' if present, to avoid Scrapfly error when retry is enabled
    base.pop("timeout", None)

    # attempt 1 (plain)
    cfg1 = ScrapeConfig(url=url, **base)
    try:
        res = client.scrape(cfg1)
        html = res.content
        parsed = parse_product(html)
        status = "OK" if parsed.get("Title") not in ("ERROR", "Title not found") else "ERROR"
        out = {
            "index": idx,
            "url": url,
            "status": status,
            "_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **parsed,
        }
        if status == "OK":
            return out
    except ScrapflyScrapeError as e:
        err = getattr(e, "error_code", "") or type(e).__name__
        # fall through to a simple second attempt
    except Exception as e:
        err = type(e).__name__

    # attempt 2 (slightly more patient, still simple)
    base2 = base.copy()
    base2.update({"rendering_wait": 10_000, "auto_scroll": True})
    base2.pop("timeout", None)
    cfg2 = ScrapeConfig(url=url, **base2)
    try:
        res = client.scrape(cfg2)
        html = res.content
        parsed = parse_product(html)
        status = "OK" if parsed.get("Title") not in ("ERROR", "Title not found") else "ERROR"
        out = {
            "index": idx,
            "url": url,
            "status": status,
            "_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **parsed,
        }
        return out
    except ScrapflyScrapeError as e2:
        err2 = getattr(e2, "error_code", "") or type(e2).__name__
        return {
            "index": idx,
            "url": url,
            "status": "ERROR",
            "_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "Title": "ERROR",
            "Price": "",
            "Competitor": "",
            "Price in Installments": "",
            "Image": "",
            "_error": err2,
        }
    except Exception as e2:
        return {
            "index": idx,
            "url": url,
            "status": "ERROR",
            "_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "Title": "ERROR",
            "Price": "",
            "Competitor": "",
            "Price in Installments": "",
            "Image": "",
            "_error": f"UNEXPECTED {type(e2).__name__}",
        }

def run_scrapp_failed():
    if not SCRAPFLY_API_KEY:
        raise RuntimeError("SCRAPFLY_API_KEY is missing. Put it in your environment or .env")

    failed = read_failed()
    if not failed:
        logger.info("No failed URLs found - nothing to do.")
        write_results([])
        return

    client = ScrapflyClient(key=SCRAPFLY_API_KEY)
    session_id = f"FAILED-{uuid.uuid4()}"  # one sticky session for the whole run
    results = []

    logger.warning(f"Retrying {len(failed)} failed URLs (simple mode)...")
    for idx, url in failed:
        out = scrape_url(client, session_id, idx, url)
        results.append(out)
        # short polite pause; keep it simple
        time.sleep(0.8)

    # Sort by original index to keep things tidy
    results.sort(key=lambda r: r.get("index", 0))
    write_results(results)
    # final summary
    oks = sum(1 for r in results if r.get("status") == "OK")
