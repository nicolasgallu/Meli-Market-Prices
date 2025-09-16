import os, json

# ──────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
DATABASE_DIR = os.path.join(BASE_DIR, '../database')

SCRAP_RESULTS_PATH = os.path.join(DATABASE_DIR, 'scrap_results.json')
FAILED_SCRAP_PATH = os.path.join(DATABASE_DIR, 'scrapping_failed_urls.json')
OUTPUT_PATH = os.path.join(DATABASE_DIR, 'merged_results.json')

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def load_json_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]

# ──────────────────────────────────────────────────────────────
# CORE
# ──────────────────────────────────────────────────────────────
def build_union():
    scrap_rows  = load_json_list(SCRAP_RESULTS_PATH)
    failed_rows = load_json_list(FAILED_SCRAP_PATH)
    cleaned = []

    # from scrap_results.json → pick needed fields
    for r in scrap_rows:
        url = r.get("url") or r.get("_url")
        if not url:
            continue
        cleaned.append({
            "url": url,
            "_timestamp": r.get("_timestamp"),
            "Title": r.get("Title"),
            "Price": r.get("Price"),
            "Competitor": r.get("Competitor"),
            "Price in Installments": r.get("Price in Installments"),
            "Image": r.get("Image"),
            "Cost":r.get("_api_cost")
        })

    # from scrapping_failed_urls.json → only successes
    for r in failed_rows:
        if r is None:
            continue
        if r.get("_status") != "Success":
            continue
        url = r.get("url") or r.get("_url")
        if not url:
            continue
        cleaned.append({
            "url": url,
            "_timestamp": r.get("_timestamp"),
            "Title": r.get("Title"),
            "Price": r.get("Price"),
            "Competitor": r.get("Competitor"),
            "Price in Installments": r.get("Price in Installments"),
            "Image": r.get("Image"),
            "Cost":r.get("_api_cost")
        })

    # write output
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

