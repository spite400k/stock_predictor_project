"""
スクリプト名: rakuten_item_sync.py

目的:
Supabase上の「mst_site_item」テーブルから楽天商品コードを取得し、楽天APIを用いて商品情報を取得・整形。
その後、商品情報をSupabaseの「trn_tracked_item_stock」テーブルにアップサート（追加または更新）する。
主に在庫状況・価格などの追跡に利用するデータの最新化を目的とする。
"""


import datetime
import json
import os
import sys
import time
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# 共通モジュール読み込み
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../common")))
from logger import log_error

# --- 環境変数の読み込み ---
load_dotenv()

# --- Supabase 初期化 ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 楽天API 設定 ---
RAKUTEN_API_URL = os.getenv("RAKUTEN_API_URL")
RAKUTEN_APP_ID = os.getenv("RAKUTEN_APP_ID")
SITE = "楽天"  # 固定値

def fetch_mst_site_item_rows():
    """Supabaseのmst_site_itemテーブルから楽天の情報を取得"""
    try:
        response = supabase.table("mst_site_item") \
            .select("seller_site_id, seller_site_name, product_id, jan_code") \
            .eq("site", SITE) \
            .execute()
        return response.data
    except Exception as e:
        log_error(f"Supabase mst_site_item取得失敗: {str(e)}")
        return []


def fetch_item_from_rakuten(shop_code, item_code):
    """楽天APIから商品情報を取得"""
    full_item_code = f"{shop_code}:{item_code}"
    params = {
        "applicationId": RAKUTEN_APP_ID,
        "format": "json",
        "itemCode": full_item_code
    }

    try:
        response = requests.get(RAKUTEN_API_URL, params=params)
        if response.status_code != 200:
            log_error(f"楽天APIエラー: {response.status_code} {response.text}")
            return None

        data = response.json()
        if "Items" not in data or not data["Items"]:
            log_error(f"商品データが見つかりません: {full_item_code}")
            return None

        item = data["Items"][0]["Item"]
        return {
            "product_id": item.get("itemCode"),
            "product_name": item.get("itemName"),
            "description": item.get("itemCaption"),
            "site": SITE,
            "seller_site_id": shop_code,
            "seller_site_name": item.get("shopName", ""),  # 楽天APIから取得できる場合のみ
            "stock_status": item.get("availability") == 1,  # 1なら在庫あり
            "price": item.get("itemPrice")
        }

    except Exception as e:
        log_error(f"楽天API通信エラー: {str(e)}")
        return None

def upsert_product_to_supabase(product_data):
    """Supabaseに商品情報をアップサート（新規追加または更新）"""
    try:
        site = product_data["site"]
        seller_site_id = product_data["seller_site_id"]
        product_id = product_data["product_id"]

        # 既存レコードを確認
        existing = supabase.table("trn_tracked_item_stock") \
            .select("id") \
            .eq("site", site) \
            .eq("seller_site_id", seller_site_id) \
            .eq("product_id", product_id) \
            .limit(1) \
            .execute()

        if existing.data:
            trn_tracked_item_stock_id = existing.data[0]["id"]
            product_data["updated_at"] = datetime.datetime.now().isoformat()

            supabase.table("trn_tracked_item_stock") \
                .update(product_data) \
                .eq("id", trn_tracked_item_stock_id) \
                .execute()
        else:
            supabase.table("trn_tracked_item_stock").insert(product_data).execute()

    except Exception as e:
        log_error(f"Supabase trn_tracked_item_stock upsert失敗: {str(e)}")

def main_rakuten():
    print("🔍 Supabaseから検索条件を取得中...")
    rows = fetch_mst_site_item_rows()

    if not rows:
        print("⚠️ データが見つかりません。処理を終了します。")
        return

    for row in rows:
        shop_code = row["seller_site_id"]
        item_code = row["product_id"]
        print(f"📦 商品取得中: {shop_code}:{item_code}")
        item_data = fetch_item_from_rakuten(shop_code, item_code)

        if item_data:
            # seller_site_nameがmst_site_itemに含まれている場合は補完
            item_data["seller_site_name"] = row.get("seller_site_name", item_data.get("seller_site_name", ""))
            # item_data["seller_site_id"] = item_data.get("seller_site_id", "") or ""
            upsert_product_to_supabase(item_data)
            print(f"✅ 登録完了: {item_data['product_id']}")
        else:
            print(f"❌ スキップ: {shop_code}:{item_code}")

        time.sleep(1)

if __name__ == "__main__":
    main_rakuten()
