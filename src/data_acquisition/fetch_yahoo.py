"""
スクリプト名: yahoo_ranking_stock_fetcher.py

目的:
Yahoo!ショッピングの「高評価トレンドランキングAPI」を利用して注目商品情報を取得し、
さらに itemSearch API を活用して各商品の在庫状況（inStock）を取得・統合。
最終的に商品名、価格、URL、画像URL、在庫ステータスなどの情報を持った商品データを作成する。

用途:
Yahoo!ショッピング上で人気商品かつ在庫があるアイテムを効率よく把握し、
販売戦略やマーケティング施策に活用するためのデータ収集・分析ベースを構築する。
"""


import json
import os
import sys
from time import sleep
import requests
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../common")))
from logger import log_error,log_response

# .env ファイルの読み込み（環境変数の設定）
load_dotenv()

# Yahoo ショッピング API のエンドポイント
YAHOO_API_URL = os.getenv("YAHOO_API_URL")  # 高評価トレンドランキング
YAHOO_APP_ID = os.getenv("YAHOO_APP_ID")  # 取得したClient ID（アプリケーションID）

# 在庫情報を取得するためのAPIエンドポイント
ITEM_SEARCH_API_URL = "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch" # https://developer.yahoo.co.jp/webapi/shopping/v3/itemsearch.html

def fetch_yahoo_stock():
    """Yahoo! ショッピングのランキングから商品を取得し、在庫情報を追加する"""
    params = {
        "appid": YAHOO_APP_ID,
        # "category_id": "1"  # 例: 家電のカテゴリ
    }

    try:
        # API にリクエストを送信
        response = requests.get(YAHOO_API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            log_response("yahoo_data",data)
            items = extract_items_data(data["high_rating_trend_ranking"]["ranking_data"])  # 商品データを抽出

            # 在庫情報を取得して商品のデータを更新
            for item in items:
                item["stock_status"] = fetch_stock_status(item["product_id"])  # 在庫情報を追加

                sleep(1)
            
            # print(json.dumps(items, indent=4, ensure_ascii=False))  # 日本語を適切に表示
            return items
        else:
            error_message=(f"Error: {response.status_code}, {response.text}")
            log_error(error_message)
            print(error_message)
    except Exception as e:
        error_message = f"An exception occurred: {str(e)}"
        log_error(error_message)
        print(error_message)
    finally:
        print(f"処理を継続")
    
    

def extract_items_data(items):
    """JSON内のItemデータから必要な情報を抽出する"""
    extracted_items = []
    for item in items:
        trn_tracked_item_stock = item["item_information"]
        extracted_items.append({
            "product_id": trn_tracked_item_stock["code"],  # 商品コード
            "product_name": trn_tracked_item_stock["name"],  # 商品名
            "description": trn_tracked_item_stock.get("description", trn_tracked_item_stock["name"]),  # 商品説明
            "site": "Yahoo! Shopping",  # 販売サイト
            "seller_site_id": item["seller"]["id"],  # 販売元ID
            "seller_site_name": item["seller"]["name"],  # 販売元サイト名
            "price": trn_tracked_item_stock["regular_price"],  # 価格
            "product_url": trn_tracked_item_stock["url"],  # 商品URL
            "image_url": item["image"]["medium"],  # 商品画像URL
            "jan_code": trn_tracked_item_stock.get("jan_code", None)  # JANコード（あれば取得）
        })

    return extracted_items


def fetch_stock_status(item_code):
    """在庫情報を取得するために itemSearch API を使用"""
    params = {
        "appid": YAHOO_APP_ID,
        "query": item_code,  # 商品コードで検索
        "results": 1  # 1件のみ取得
    }
    
    response = requests.get(ITEM_SEARCH_API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        
        # 検索結果が存在する場合
        if "hits" in data and len(data["hits"]) > 0:
            stock_info = data["hits"][0]  # 最初の検索結果を取得
            return stock_info.get("inStock")   # Trueなら在庫あり, Falseなら在庫なし
        else:
            return False
    else:
        print(f"Error: {response.status_code}, {response.text}")  # エラーメッセージを表示
        error_message=(f"Error: {response.status_code}, {response.text}")
        log_error(error_message)
        return False
    


if __name__ == "__main__":
    fetch_yahoo_stock()
