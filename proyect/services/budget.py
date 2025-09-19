import requests

def remain_budget(api_key):
    url = f"https://api.scrapfly.io/account?key={api_key}"
    response = requests.get(url)
    credits = response.json().get("subscription").get("usage").get("scrape").get("remaining")
    print(credits)