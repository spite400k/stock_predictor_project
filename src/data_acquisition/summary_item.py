# ファイル名例: aggregate_site_item.py

from dotenv import load_dotenv
from supabase import create_client, Client
import os
from collections import defaultdict
import datetime

# .env 読み込み
load_dotenv()

# Supabase 初期化
SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://aaueetvhrbyqswejvmlc.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFhdWVldHZocmJ5cXN3ZWp2bWxjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDI0ODExMTMsImV4cCI6MjA1ODA1NzExM30.4F5lyIxl5vl5G2V3b6tJI_3lNU3ApN2_i-D-DSsef5w"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def aggregate_and_upsert_site_item():
    """ trn_ranked_item_stockからデータを取得して、mst_site_itemに集計保存する """

    batch_size = 1000
    offset = 0
    all_rows = []

    # データ取得
    while True:
        response = (
            supabase.table("trn_ranked_item_stock")
            .select("*")
            .order("site", desc=False)
            .order("seller_site_id", desc=False)
            .order("seller_site_name", desc=False)
            .order("product_id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        
        if not response.data:
            break
        
        all_rows.extend(response.data)
        print(f"取得件数: {len(response.data)} (現在までの累計: {len(all_rows)})")
        offset += batch_size

    if not all_rows:
        print("データ取得エラー: データが存在しません。")
        return

    # 集計処理
    summary_dict = defaultdict(int)
    for row in all_rows:
        key = (
            row.get("site"),
            row.get("seller_site_id"),
            row.get("seller_site_name"),
            row.get("product_id"),
            row.get("jan_code"),
        )
        summary_dict[key] += 1

    upsert_count = 0
    # 集計結果を mst_site_item に upsert
    for (site, seller_site_id, seller_site_name, product_id, jan_code), count in summary_dict.items():
        insert_data = {
            "site": site,
            "seller_site_id": seller_site_id,
            "seller_site_name": seller_site_name,
            "product_id": product_id,
            "jan_code": jan_code,
            "count": count,
            "summary_time": datetime.datetime.now().isoformat()
        }

        res = supabase.table("mst_site_item").upsert(insert_data).execute()
        if not res.data:
            print(f"⚠️ INSERT失敗: {insert_data}")
        else:
            upsert_count += 1

    print(f"✅ 完了！mst_site_item に集計結果を保存しました。（{upsert_count}件）")


if __name__ == "__main__":
    aggregate_and_upsert_site_item()
