"""
スクリプト名: supabase_ranked_stock_updater.py

目的:
外部ソース（Amazon、楽天、Yahoo!など）から収集した商品データを、
Supabaseの「trn_ranked_item_stock」テーブルに対して個別または一括で登録（INSERT）する。

用途:
ランキングや在庫調査などで取得した商品情報を一元的にデータベースに保存し、
在庫管理、販売傾向の可視化、ダッシュボード構築などに活用する基盤を提供する。
"""

import datetime
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# .env ファイルの読み込み
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Supabaseに在庫データを登録
# Supabaseに在庫データを登録
def update_stock_in_supabase(
    product_id,
    product_name,
    site,
    stock_status,
    description="",
    seller_site_id="",
    seller_site_name="",
    price=0,
    jan_code=None  # 🆕 追加
):
    timestamp = datetime.datetime.now().isoformat()

    data = {
        "product_id": product_id,
        "product_name": product_name,
        "description": description,
        "site": site,
        "seller_site_id": seller_site_id,
        "seller_site_name": seller_site_name,
        "stock_status": stock_status,
        "price": price,
        "insert_time": timestamp,
        "update_time": timestamp,
        "jan_code": jan_code,  # 🆕 追加
    }

    response = supabase.table("trn_ranked_item_stock").insert(data).execute()

    if response.data:
        return data
    else:
        print(f"❌ エラー trn_ranked_item_stock: {response.status_code}, {response.text}")
        return None


# リスト形式のデータをまとめて登録
def insert_stock_data(data_list):
    if data_list:
        for d in data_list:
            update_stock_in_supabase(
                product_id=d["product_id"],
                product_name=d["product_name"],
                site=d["site"],
                stock_status=d["stock_status"],
                description=d.get("description", ""),
                seller_site_id=d.get("seller_site_id", ""),
                seller_site_name=d.get("seller_site_name", ""),
                price=d.get("price", 0),
                jan_code=d.get("jan_code")  # 🆕 追加
            )


# 実行テスト用サンプル
if __name__ == "__main__":
    amazon_data = {
        "product_id": "AMZ123",
        "product_name": "PS3",
        "site": "Amazon",
        "stock_status": True,
        "description": "Sony PlayStation 3",
        "seller_site_id": "amz",
        "seller_site_name": "Amazon JP",
        "price": 20000,
        "jan_code": "4905524664509"  # 🆕 追加
    }

    rakuten_data = {
        "product_id": "RAK456",
        "product_name": "PS4",
        "site": "Rakuten",
        "stock_status": True,
        "description": "Sony PlayStation 4",
        "seller_site_id": "rak",
        "seller_site_name": "Rakuten Store",
        "price": 30000,
        "jan_code": "4948872414033"  # 🆕 追加
    }

    yahoo_data = {
        "product_id": "YAH789",
        "product_name": "PS5",
        "site": "Yahoo",
        "stock_status": True,
        "description": "Sony PlayStation 5",
        "seller_site_id": "yah",
        "seller_site_name": "Yahoo Shopping",
        "price": 50000,
        "jan_code": "4948872415207"  # 🆕 追加
    }

    data_sources = [amazon_data, rakuten_data, yahoo_data]
    insert_stock_data(data_sources)
