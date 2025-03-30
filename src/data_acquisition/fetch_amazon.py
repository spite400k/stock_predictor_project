import requests
import json

AMAZON_API_URL = "https://api.amazon.com/product"
ACCESS_KEY = "YOUR_AMAZON_ACCESS_KEY"

PRODUCT_ID = "B08GGGBKRQ"

def fetch_amazon_stock():
    params = {
        "item_id": PRODUCT_ID, 
        "country": "JP", 
        "access_key": ACCESS_KEY
    }

    response = requests.get(AMAZON_API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        # print(json.dumps(data, indent=4))
        return data
    else:
        print(f"Error: {response.status_code}, {response.text}")  # エラーメッセージを表示


if __name__ == "__main__":
    data = fetch_amazon_stock()
    # print(json.dumps(data, indent=4))
