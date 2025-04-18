"""
スクリプト名: rakuten_ranking_fetcher.py

目的:
楽天API（ジャンル別商品ランキングAPI）を利用して、楽天市場の人気商品情報を取得し、
商品コード、商品名、販売店情報、価格、在庫状況などの必要な項目を抽出・整形する。
抽出した情報は後続処理（ログ出力やDB登録など）に利用可能な形式で返す。
"""


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
            log_response("rakuten",data)
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
            "price": item["itemPrice"],               # 価格
            "jan_code": item.get("jan", None)       # JANコード（存在しない場合はNoneを設定）
        })

    return extracted_items



if __name__ == "__main__":
    data = fetch_rakuten_stock()
    print(json.dumps(data, indent=4))
    