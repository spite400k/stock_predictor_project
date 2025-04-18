"""
スクリプト名: yahoo_item_sync.py

目的:
Supabaseの「mst_site_item」テーブルからYahoo!ショッピングの商品IDと店舗コードを取得し、
Yahoo!ショッピングAPIを使って該当商品の詳細情報（商品名、説明、在庫、価格など）を取得。
取得した情報をSupabaseの「trn_tracked_item_stock」テーブルにアップサート（INSERTまたはUPDATE）することで、
商品の在庫状況や価格の最新情報を管理・追跡する。
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

# --- Yahoo!ショッピングAPI 設定 ---
YAHOO_API_URL = os.getenv("YAHOO_API_ITEM_URL")
YAHOO_APP_ID = os.getenv("YAHOO_APP_ID")
SITE = "Yahoo! Shopping"  # 固定値

def fetch_mst_site_item_rows():
    """Supabaseのmst_site_itemテーブルからyahooの情報を取得"""
    try:
        response = supabase.table("mst_site_item") \
            .select("seller_site_id, seller_site_name, product_id, jan_code") \
            .eq("site", SITE) \
            .execute()
        return response.data
    except Exception as e:
        log_error(f"Supabase mst_site_item取得失敗: {str(e)}")
        return []

def fetch_item_from_yahoo(shop_code, item_code, shop_name, jan_code=None):
    """Yahoo!ショッピングAPIから商品情報を取得（item_codeまたはjan_codeベース）"""

    # item_code または jan_code のいずれかは必須
    if not item_code and not jan_code:
        log_error(f"無効なitem_codeおよびjan_code: {item_code}:{jan_code}")
        return None

    query_value = item_code if item_code else jan_code
    if not query_value:
        log_error(f"検索用のqueryがありません: item_code={item_code}, jan_code={jan_code}")
        return None

    full_item_code = f"{shop_code}:{item_code}"

    # APIリクエストのパラメータ設定
    # item_codeがある場合はitem_codeを、ない場合はjan_codeを使用
    params = {
        "appid": YAHOO_APP_ID,
        "query": query_value,
        "hits": 1,
    }
    # shopcodeがある場合はshopcodeを追加
    if shop_code:
        params["shopcode"] = shop_code

    try:
        print(f"📡 Yahoo APIリクエスト: {YAHOO_API_URL}?query={params['query']}")
        response = requests.get(YAHOO_API_URL, params=params)

        if response.status_code != 200:
            log_error(f"YahooAPIエラー: {response.status_code} {response.text}")
            return None

        data = response.json()
        resultset = data.get("ResultSet", {})
        result_data = resultset.get("0", {})

        # 結果がなければスキップ
        if "Result" not in result_data or not result_data["Result"]:
            log_error(f"商品データが見つかりません: item_code={full_item_code},jan_code={jan_code}")
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
            "price": item.get("price"),
            "jan_code": jan_code
        }

    except Exception as e:
        log_error(f"YahooAPI通信エラー: {str(e)}")
        return None



def upsert_product_to_supabase(product_data):
    """Supabaseに商品情報をアップサート（INSERTまたはUPDATE）"""
    try:
        site = product_data["site"]
        seller_site_id = product_data["seller_site_id"]
        # seller_site_name = product_data["seller_site_name"]
        product_id = product_data["product_id"]

        # レコードの存在確認
        existing = supabase.table("trn_tracked_item_stock") \
            .select("id") \
            .eq("site", site) \
            .eq("seller_site_id", seller_site_id) \
            .eq("product_id", product_id) \
            .limit(1) \
            .execute()

        if existing.data:
            # UPDATE処理
            record_id = existing.data[0]["id"]
            product_data["updated_at"] = datetime.datetime.now().isoformat()
            supabase.table("trn_tracked_item_stock") \
                .update(product_data) \
                .eq("id", record_id) \
                .execute()
        else:
            # INSERT処理
            supabase.table("trn_tracked_item_stock").insert(product_data).execute()

    except Exception as e:
        log_error(f"Supabase trn_tracked_item_stock INSERT/UPDATE 失敗: {str(e)}")


def main_yahoo():
    print("🔍 Supabaseから検索条件を取得中...")
    rows = fetch_mst_site_item_rows()

    if not rows:
        print("⚠️ データが見つかりません。処理を終了します。")
        return

    for row in rows:
        shop_code = row.get("seller_site_id", "")
        item_code = row.get("product_id", "")
        jan_code = row.get("jan_code", "")
        shop_name = row.get("seller_site_name", "")

        print(f"📦 商品取得中: {shop_code}:{item_code or 'JAN:' + jan_code}")
        item_data = fetch_item_from_yahoo(shop_code, item_code, shop_name, jan_code)

        if item_data:
            upsert_product_to_supabase(item_data)
            print(f"✅ 登録完了: {item_data['product_id']}")
        else:
            print(f"❌ スキップ: {shop_code}:{item_code or 'JAN:' + jan_code}")

        time.sleep(1)# 1秒待機（API制限対策）


    print("🎉 全商品処理完了")

if __name__ == "__main__":
    main_yahoo()
