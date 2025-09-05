import os, json
from collections import OrderedDict

BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, '../database')

SCRAP_RESULTS_PATH = os.path.join(DATABASE_DIR, 'scrap_results.json')
FAILED_SCRAP_PATH  = os.path.join(DATABASE_DIR, 'scrapping_failed_urls.json')
OUTPUT_PATH        = os.path.join(DATABASE_DIR, 'merged_results.json')

KEEP_FIELDS = [
    "_timestamp",
    "Title",
    "Price",
    "Competitor",
    "Price in Installments",
    "Image",
    "url",
]

def load_json_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # allow single-object files, normalize to list
    if isinstance(data, dict):
        data = [data]
    return data

def select_fields(obj):
    """Return only the fields of interest (missing -> omitted)."""
    out = {}
    for k in KEEP_FIELDS:
        if k in obj and obj[k] is not None:
            out[k] = obj[k]
    return out

def cleaning():
    # 1) load
    scrap_rows  = load_json_list(SCRAP_RESULTS_PATH)
    failed_rows = load_json_list(FAILED_SCRAP_PATH)

    # 2) build base map from scrap_results.json
    by_url = OrderedDict()
    for r in scrap_rows:
        url = r.get("_url") or r.get("url")
        if not url:
            continue
        # map _url -> url and keep only selected fields
        base = {
            "url": url,
            "_timestamp": r.get("_timestamp"),
            "Title": r.get("Title"),
            "Price": r.get("Price"),
            "Competitor": r.get("Competitor"),
            "Price in Installments": r.get("Price in Installments"),
            "Image": r.get("Image"),
        }
        by_url[url] = select_fields(base)

    # 3) overlay from scrapping_failed_urls.json
    #    rule: if the same URL appears and status == "OK", override
    for r in failed_rows:
        url = r.get("url") or r.get("_url")
        if not url:
            continue
        status = (r.get("status") or "").upper()
        candidate = {
            "url": url,
            "_timestamp": r.get("_timestamp"),
            "Title": r.get("Title"),
            "Price": r.get("Price"),
            "Competitor": r.get("Competitor"),
            "Price in Installments": r.get("Price in Installments"),
            "Image": r.get("Image"),
        }
        candidate = select_fields(candidate)

        if url in by_url:
            # only override if we actually scraped it on the retry
            if status == "OK":
                by_url[url] = candidate or by_url[url]
        else:
            # if this URL wasn't in the original, include it regardless
            by_url[url] = candidate

    # 4) emit list (ordered by first-seen URL)
    merged_list = list(by_url.values())

    os.makedirs(DATABASE_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged_list, f, ensure_ascii=False, indent=2)

    print(f"Wrote -> {OUTPUT_PATH}")
    print(f"Items: {len(merged_list)}")
