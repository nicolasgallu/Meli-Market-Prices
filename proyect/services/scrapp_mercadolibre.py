from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyScrapeError
import os, json, time, uuid, random, asyncio
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import logging

# ---------- SETTINGS ----------
URLS_PER_SESSION       = 18
MAX_PARALLEL_SESSIONS  = 5
BASE_TIMEOUT_MS        = 90000
HEAVY_TIMEOUT_MS       = 120000
DEEP_TIMEOUT_MS        = 150000
BASE_COST_BUDGET       = 30
HEAVY_COST_BUDGET      = 45
DEEP_COST_BUDGET       = 60
THINK_TIME_RANGE       = (0.9, 2.3)

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------- ENV / PATHS ----------
load_dotenv()
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, '../database')
URLS_JSON_PATH = os.path.join(DATABASE_DIR, 'urls.json')
RESULTS_JSON_PATH = os.path.join(DATABASE_DIR, 'scrap_results.json')
FAILED_JSON_PATH  = os.path.join(DATABASE_DIR, 'failed_urls.json')

# ---------- PARSER ----------
def parse_product(html):
    soup = BeautifulSoup(html, "html.parser")
    if not soup.find("h1", class_="ui-pdp-title"):
        return {"Title": "Title not found"}
    data = {}
    t = soup.find("h1", class_="ui-pdp-title")
    data["Title"] = t.text.strip() if t else "Title not found"
    p = soup.find("span", class_="andes-money-amount__fraction")
    data["Price"] = p.text.strip() if p else ""
    c = soup.find("h2", class_="ui-seller-data-header__title")
    data["Competitor"] = c.text.strip() if c else "Competitor not found"
    q = soup.find("div", class_="ui-pdp-price__subtitles")
    data["Price in Installments"] = q.text.strip() if q else "Installments not found"
    img = soup.find("img", class_="ui-pdp-image")
    data["Image"] = img["src"] if img and img.get("src") else "Image not found"
    return data

# ---------- UTIL ----------
def looks_blocked(text):
    if not text:
        return True
    t = text.lower()
    markers = [
        "protegemos a nuestros usuarios",
        "no podemos procesar tu solicitud",
        "hemos detectado actividad inusual",
        "captcha",
        "estamos teniendo inconvenientes",
        "lo sentimos",
    ]
    return any(m in t for m in markers)

def chunk_by_size(items, size):
    return [items[i:i+size] for i in range(0, len(items), size)]

# ---------- SCRAPING CORE ----------
BASE_SC = dict(
    asp=True,
    render_js=True,
    wait_for_selector="h1.ui-pdp-title",
    proxy_pool="public_residential_pool",
    country="ar",
    lang=["es-AR", "es"],
    session_sticky_proxy=True,
    retry=False,
    timeout=BASE_TIMEOUT_MS,
    cost_budget=BASE_COST_BUDGET,
)

def build_cfg(url, session_id, overrides=None):
    sc = BASE_SC.copy()
    sc["session"] = session_id
    if overrides:
        sc.update(overrides)
    return ScrapeConfig(url=url, **sc)

def scrape_once(idx, url, display_session, session_id, client, attempt=1, overrides=None):
    cfg = build_cfg(url, session_id, overrides)
    try:
        logger.info(f"[{display_session}] START {idx}/{TOTAL} (attempt {attempt})")
        res = client.scrape(cfg)
        html = res.content
        headers = getattr(res.response, "headers", {}) or {}
        api_cost = headers.get("X-Scrapfly-Api-Cost", "n/a")

        parsed = parse_product(html)
        parsed["_url"] = url
        parsed["_index"] = idx
        parsed["_session"] = display_session
        parsed["_attempt"] = attempt
        parsed["_api_cost"] = api_cost
        parsed["_timestamp"] = datetime.utcnow().isoformat() + "Z"

        # Soft retry if looks incomplete/shielded
        if overrides is None:  # only in normal path; deep-rescue manages its own flow
            if (parsed.get("Title") == "Title not found" or looks_blocked(html)) and attempt < 3:
                time.sleep(1.2 * attempt)
                next_over = {"rendering_wait": 7000, "auto_scroll": True} if attempt == 1 else {
                    "rendering_wait": 12000, "auto_scroll": True,
                    "timeout": HEAVY_TIMEOUT_MS, "cost_budget": HEAVY_COST_BUDGET
                }
                return scrape_once(idx, url, display_session, session_id, client, attempt+1, next_over)

        status = "OK" if parsed.get("Title") not in ("ERROR", "Title not found") else "FAIL"
        if status == "OK":
            logger.info(f"[{display_session}] OK {idx}/{TOTAL} (cost: {api_cost})")
        else:
            logger.warning(f"[{display_session}] FAIL {idx}/{TOTAL}")
        return parsed

    except ScrapflyScrapeError as e:
        code = getattr(e, "error_code", "") or ""
        transient = any(k in code for k in ["TIMEOUT", "ASP", "PROXY", "DRIVER", "RENDER"])
        if overrides is None and transient and attempt < 3:
            time.sleep(1.5 * attempt)
            next_over = {"rendering_wait": 7000, "auto_scroll": True} if attempt == 1 else {
                "rendering_wait": 12000, "auto_scroll": True,
                "timeout": HEAVY_TIMEOUT_MS, "cost_budget": HEAVY_COST_BUDGET
            }
            return scrape_once(idx, url, display_session, session_id, client, attempt+1, next_over)
        logger.error(f"[{display_session}] HARD FAIL {idx}/{TOTAL} after {attempt} attempts")
        return {
            "_url": url, "_index": idx, "_session": display_session, "_attempt": attempt,
            "_timestamp": datetime.utcnow().isoformat() + "Z", "Title": "ERROR",
            "Price": "", "Competitor": "", "Price in Installments": "", "Image": "",
            "_error": f"{code or type(e).__name__}"
        }
    except Exception as e:
        if overrides is None and attempt < 2:
            time.sleep(1.0)
            return scrape_once(idx, url, display_session, session_id, client, attempt+1, overrides)
        logger.error(f"[{display_session}] UNEXPECTED {idx}/{TOTAL}: {type(e).__name__}")
        return {
            "_url": url, "_index": idx, "_session": display_session, "_attempt": attempt,
            "_timestamp": datetime.utcnow().isoformat() + "Z", "Title": "ERROR",
            "Price": "", "Competitor": "", "Price in Installments": "", "Image": "",
            "_error": f"UNEXPECTED {type(e).__name__}"
        }

# ---------- ASYNC WORKERS ----------
async def session_worker(display_session, session_items, out):
    client = ScrapflyClient(key=SCRAPFLY_API_KEY)
    session_id = f"{display_session}-{uuid.uuid4()}"  # hidden; not logged
    await asyncio.sleep(random.uniform(0.3, 1.2))  # stagger session start

    for idx, url in session_items:
        result = await asyncio.to_thread(scrape_once, idx, url, display_session, session_id, client, 1, None)
        out.append(result)
        await asyncio.sleep(random.uniform(*THINK_TIME_RANGE))

async def run_all(indexed_urls):
    batches = chunk_by_size(indexed_urls, URLS_PER_SESSION)
    results = []
    sem = asyncio.Semaphore(MAX_PARALLEL_SESSIONS)

    async def guarded_worker(name, chunk):
        async with sem:
            await session_worker(name, chunk, results)

    tasks = []
    for i, chunk in enumerate(batches, start=1):
        tasks.append(asyncio.create_task(guarded_worker(f"S{i}", chunk)))
    await asyncio.gather(*tasks)
    return results

# ---------- RESCUE PASS (sequential, heavy settings) ----------
def rescue_sequential(indexed_urls):
    logger.info(f"[RESCUE] {len(indexed_urls)} items...")
    client = ScrapflyClient(key=SCRAPFLY_API_KEY)
    out = []
    for j, (idx, url) in enumerate(indexed_urls, start=1):
        sess = "RESCUE"
        over = {
            "rendering_wait": 12000, "auto_scroll": True,
            "timeout": HEAVY_TIMEOUT_MS, "cost_budget": HEAVY_COST_BUDGET
        }
        res = scrape_once(idx, url, sess, f"{sess}-{uuid.uuid4()}", client, attempt=3, overrides=over)
        out.append(res)
        time.sleep(1.0)
    return out

# ---------- DEEP RESCUE (session prewarm + alternate strategies) ----------
def prewarm_session(client, session_id):
    # Light fetch of home to grab cookies/anti-bot tokens
    cfg = build_cfg(
        "https://www.mercadolibre.com.ar/",
        session_id,
        overrides={"rendering_wait": 3000, "timeout": BASE_TIMEOUT_MS}
    )
    try:
        client.scrape(cfg)
    except Exception:
        pass  # prewarm is best-effort

def deep_rescue(indexed_urls):
    if not indexed_urls:
        return []

    logger.info(f"[DEEP] {len(indexed_urls)} stubborn items...")
    out = []
    client = ScrapflyClient(key=SCRAPFLY_API_KEY)

    for (idx, url) in indexed_urls:
        # Try A: heavy with selector, after prewarm
        sessA = "DEEP-A"
        sidA = f"{sessA}-{uuid.uuid4()}"
        prewarm_session(client, sidA)
        overA = {"rendering_wait": 15000, "auto_scroll": True,
                 "timeout": DEEP_TIMEOUT_MS, "cost_budget": DEEP_COST_BUDGET}
        rA = scrape_once(idx, url, sessA, sidA, client, attempt=4, overrides=overA)
        if rA.get("Title") not in ("ERROR", "Title not found"):
            logger.info(f"[DEEP] OK by A {idx}/{TOTAL}")
            out.append(rA)
            time.sleep(1.0)
            continue

        # Try B: selectorless long render, after new prewarm
        sessB = "DEEP-B"
        sidB = f"{sessB}-{uuid.uuid4()}"
        prewarm_session(client, sidB)
        overB = {"wait_for_selector": None, "rendering_wait": 20000, "auto_scroll": True,
                 "timeout": DEEP_TIMEOUT_MS, "cost_budget": DEEP_COST_BUDGET}
        rB = scrape_once(idx, url, sessB, sidB, client, attempt=5, overrides=overB)
        out.append(rB)
        time.sleep(1.5)  # a little extra pause between deep attempts

    return out

# ---------- MERGE / IO ----------
def urls_needing_rescue(rows):
    bad = []
    seen = set()
    for r in rows:
        if r.get("Title") in ("ERROR", "Title not found"):
            idx, u = r.get("_index"), r.get("_url")
            if idx and u and idx not in seen:
                bad.append((idx, u))
                seen.add(idx)
    return bad

def merge_results(initial_rows, rescue_rows, deep_rows, total):
    # prefer success from deepest pass; return in original order by index
    best = {}
    for r in initial_rows + rescue_rows + deep_rows:
        i = r.get("_index")
        if i is None:
            continue
        cur = best.get(i)
        cur_bad = (cur is None) or (cur.get("Title") in ("ERROR", "Title not found"))
        new_good = r.get("Title") not in ("ERROR", "Title not found")
        if cur_bad and new_good:
            best[i] = r
        elif new_good:
            best[i] = r
        elif cur is None:
            best[i] = r
    return [best[i] for i in range(1, total + 1) if i in best]

def write_json(obj, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    logger.info(f"Wrote -> {path}")
    print(f"Wrote -> {path}")

# ---------- ENTRY POINT ----------
def run_scrapping():
    if not SCRAPFLY_API_KEY:
        raise RuntimeError("SCRAPFLY_API_KEY is missing. Add it to your .env or environment.")

    with open(URLS_JSON_PATH, "r", encoding="utf-8") as f:
        urls = json.load(f)["urls"]

    global TOTAL
    TOTAL = len(urls)
    indexed = list(enumerate(urls, start=1))
    logger.info(f"Starting scrape of {TOTAL} URLs (progress-only logs; URLs hidden)...")

    # Pass 1: async sessions
    rows = asyncio.run(run_all(indexed))

    # Pass 2: rescue (heavy, sequential)
    rescue_targets = urls_needing_rescue(rows)
    logger.info(f"First pass finished. Failures needing rescue: {len(rescue_targets)}")
    rescue_rows = rescue_sequential(rescue_targets) if rescue_targets else []

    # Pass 3: deep rescue (prewarm + alternate tactics)
    deep_targets = urls_needing_rescue(rows + rescue_rows)
    logger.info(f"Rescue pass finished. Stubborn for deep: {len(deep_targets)}")
    deep_rows = deep_rescue(deep_targets) if deep_targets else []

    # Merge & write
    final_rows = merge_results(rows, rescue_rows, deep_rows, TOTAL)
    write_json(final_rows, RESULTS_JSON_PATH)

    # Save remaining failures list for quick review
    still_bad = urls_needing_rescue(final_rows)
    write_json([{"index": i, "url": u} for (i, u) in still_bad], FAILED_JSON_PATH)
    logger.info(f"All done. Remaining failures: {len(still_bad)}")
