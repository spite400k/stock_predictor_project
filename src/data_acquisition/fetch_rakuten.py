import json
import os
import sys
import requests
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../common")))
from logger import log_error,log_response

# .env ファイルの読み込み（環境変数の設定）
load_dotenv()

# RAKUTEN_API_URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601" # 楽天APIのエンドポイント 商品検索
RAKUTEN_API_URL = os.getenv("RAKUTEN_API_URL") # 楽天APIのエンドポイント 商品ランキング
RAKUTEN_APP_ID = os.getenv("RAKUTEN_APP_ID")

def fetch_rakuten_stock():
    # params = {
    #     "applicationId": RAKUTEN_APP_ID,
    #     "keyword": "PS5",
    #     "hits": 10,  # 取得件数
    #     "availability": 1,  # 在庫ありのみ取得
    # }
    params = {
        "applicationId": RAKUTEN_APP_ID,
        "format": "json",
        "genreId": "0"  # 全ジャンルのランキング取得
    }

    try:
        response = requests.get(RAKUTEN_API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            # print(json.dumps(data, indent=4).encode("utf-8").decode("unicode_escape"))
            log_response("yahoo_data",data)
            items = extract_items_data(data["Items"])  # 商品データを抽出
            # print(items)
            return items
        else:
            error_message=(f"Error: {response.status_code}, {response.text}")
            log_error(error_message)
            print(error_message)
    except Exception as e:
        error_message = f"An exception occurred: {str(e)}"
        log_error(error_message)
    finally:
        print(f"処理を継続")
    


def extract_items_data(items):
    """JSON内のItemデータから必要な情報を抽出する"""

    extracted_items = []
    for item_wrapper in items:
        item = item_wrapper["Item"]
        extracted_items.append({
            "product_id": item["itemCode"],          # 商品コード
            "product_name": item["itemName"],        # 商品名
            "description": item["itemCaption"],      # 商品説明
            "site": "楽天",                           # 固定値
            "seller_site_id": item["shopCode"],      # 販売元ID
            "seller_site_name": item["shopName"],    # 販売元名
            "stock_status": item["availability"] == 1,  # 在庫ステータス
            "price": item["itemPrice"]               # 価格
        })

    return extracted_items



if __name__ == "__main__":
    data = fetch_rakuten_stock()
    print(json.dumps(data, indent=4))
    