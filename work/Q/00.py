import requests
import random
import time

sku_id = "MLB3604947701"
url = f"https://api.mercadolivre.com/reviews?item_id={sku_id}&limit=5&offset=0"

HEADERS = {
    "User-Agent": random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ]),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close"
}

resp = requests.get(url, headers=HEADERS, timeout=10)
if resp.status_code == 200:
    data = resp.json()
    print("Total reviews:", data.get("paging", {}).get("total"))
    print("First review:", data.get("reviews", [])[0])
else:
    print("Failed:", resp.status_code, resp.text[:200])
