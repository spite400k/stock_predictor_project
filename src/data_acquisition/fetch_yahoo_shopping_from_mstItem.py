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

# --- Yahoo!ショッピングAPI 設定 ---
YAHOO_API_URL = "https://shopping.yahooapis.jp/ShoppingWebService/V1/json/itemSearch"
YAHOO_APP_ID = os.getenv("YAHOO_APP_ID")
SITE = "Yahoo! Shopping"  # 固定値

def fetch_stock_summary_rows():
    """Supabaseのstock_summaryテーブルからyahooの情報を取得"""
    try:
        response = supabase.table("stock_summary") \
            .select("seller_site_id, seller_site_name, product_id") \
            .eq("site", SITE) \
            .execute()
        return response.data
    except Exception as e:
        log_error(f"Supabase stock_summary取得失敗: {str(e)}")
        return []

def fetch_item_from_yahoo(shop_code, item_code, shop_name):
    """Yahoo!ショッピングAPIから商品情報を取得"""
    if not shop_code or not item_code:
        log_error(f"無効なshop_codeまたはitem_code: {shop_code}:{item_code}")
        return None

    full_item_code = f"{shop_code}:{item_code}"
    params = {
        "appid": YAHOO_APP_ID,
        "query": item_code,  # 検索クエリとして商品IDを使用
        "shopcode": shop_code,  # 店舗コードを指定
        "hits": 1  # 1件だけ取得（適宜調整可能）
    }

    try:
        print(f"📡 Yahoo APIリクエスト: {YAHOO_API_URL}?appid={params['appid']}&query={params['query']}&shopcode={params['shopcode']}")
        response = requests.get(YAHOO_API_URL, params=params)

        if response.status_code != 200:
            log_error(f"YahooAPIエラー: {response.status_code} {response.text}")
            return None

        data = response.json()
        resultset = data.get("ResultSet", {})
        result_data = resultset.get("0", {})

        # 結果がなければスキップ
        if "Result" not in result_data or not result_data["Result"]:
            log_error(f"商品データが見つかりません: {full_item_code}")
            return None

        item = result_data["Result"]["Item"]

        return {
            "product_id": item.get("itemcode"),
            "product_name": item.get("itemname"),
            "description": item.get("description"),
            "site": SITE,
            "seller_site_id": shop_code,
            "seller_site_name": shop_name,  # 追加: seller_site_nameを渡す
            "stock_status": item.get("availability") == 1,
            "price": item.get("price")
        }

    except Exception as e:
        log_error(f"YahooAPI通信エラー: {str(e)}")
        return None



def upsert_product_to_supabase(product_data):
    """Supabaseに商品情報をアップサート（INSERTまたはUPDATE）"""
    try:
        site = product_data["site"]
        seller_site_id = product_data["seller_site_id"]
        seller_site_name = product_data["seller_site_name"]
        product_id = product_data["product_id"]

        # レコードの存在確認
        existing = supabase.table("product_info") \
            .select("id") \
            .eq("site", site) \
            .eq("seller_site_id", seller_site_id) \
            .eq("product_id", product_id) \
            .limit(1) \
            .execute()

        if existing.data:
            # UPDATE処理
            record_id = existing.data[0]["id"]
            product_data["updated_at"] = "now()"  # 更新時間（PostgreSQLのnow()）
            supabase.table("product_info") \
                .update(product_data) \
                .eq("id", record_id) \
                .execute()
        else:
            # INSERT処理
            supabase.table("product_info").insert(product_data).execute()

    except Exception as e:
        log_error(f"Supabase INSERT/UPDATE 失敗: {str(e)}")


def main_yahoo():
    print("🔍 Supabaseから検索条件を取得中...")
    rows = fetch_stock_summary_rows()

    if not rows:
        print("⚠️ データが見つかりません。処理を終了します。")
        return

    for row in rows:
        shop_code = row["seller_site_id"]
        item_code = row["product_id"]
        shop_name = row["seller_site_name"]  # seller_site_nameを取得
        print(f"📦 商品取得中: {shop_code}:{item_code}")
        item_data = fetch_item_from_yahoo(shop_code, item_code, shop_name)

        if item_data:
            upsert_product_to_supabase(item_data)
            print(f"✅ 登録完了: {item_data['product_id']}")
        else:
            print(f"❌ スキップ: {shop_code}:{item_code}")

        time.sleep(1)  # 1秒待機（API制限対策）

    print("🎉 全商品処理完了")

if __name__ == "__main__":
    main_yahoo()
