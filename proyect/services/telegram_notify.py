
import requests

def send_message(url_wh, mensaje, telegram_chat_ids):
    return requests.post(url=url_wh, json={"mensaje": mensaje,'telegram_chat_ids':telegram_chat_ids})

