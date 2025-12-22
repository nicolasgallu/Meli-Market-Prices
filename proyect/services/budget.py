import requests

def remain_budget(api_key):
    url = f"https://api.scrapfly.io/account?key={api_key}"
    response = requests.get(url)
    credist_left = response.json().get("subscription").get("usage").get("scrape").get("remaining")
    subscription_started_at = response.json().get("subscription").get("period").get("start")
    subscription_ended_at = response.json().get("subscription").get("period").get("end")
    data = f"Scrapping Finalizado\ncredits_left: {credist_left}\nsubscription_started_at: {subscription_started_at}\nsubscription_ended_at: {subscription_ended_at}"
    return data,credist_left
