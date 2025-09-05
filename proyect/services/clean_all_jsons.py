import os
import json

# List of JSON files to clean
JSON_FILES = [
    os.path.join(os.path.dirname(__file__), '../database/urls.json'),
    os.path.join(os.path.dirname(__file__), '../database/failed_urls.json'),
    os.path.join(os.path.dirname(__file__), '../database/scrapping_failed_urls.json'),
    os.path.join(os.path.dirname(__file__), '../database/scrap_results.json'),
    os.path.join(os.path.dirname(__file__), '../database/merged_results.json'),
]

def clean_all_jsons():
    for file_path in JSON_FILES:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)
    print("All JSON files have been cleaned.")
    