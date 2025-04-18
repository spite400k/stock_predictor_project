from dotenv import load_dotenv
from supabase import create_client, Client
import os
from collections import defaultdict
from datetime import datetime

# .env 読み込み
load_dotenv()

# Supabase 初期化
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# データ取得バッチ処理
batch_size = 1000
offset = 0
all_rows = []

while True:
    response = supabase.table("trn_ranked_item_stock").select("*").range(offset, offset + batch_size - 1).execute()
    if not response.data:
        break
    all_rows.extend(response.data)
    print(f"取得件数: {len(response.data)}")
    offset += batch_size

if not all_rows:
    print("データ取得エラー: データが存在しません。")
    exit()

# 集計処理
summary_dict = defaultdict(int)

for row in all_rows:
    key = (
        row.get("site"),
        row.get("seller_site_id"),
        row.get("seller_site_name"),
        row.get("product_id"),
    )
    summary_dict[key] += 1

# 結果を mst_site_item に upsert
for (site, seller_site_id, seller_site_name, product_id), count in summary_dict.items():
    insert_data = {
        "site": site,
        "seller_site_id": seller_site_id,
        "seller_site_name": seller_site_name,
        "product_id": product_id,
        "count": count,
        "summary_time": datetime.utcnow().isoformat()
    }

    res = supabase.table("mst_site_item").upsert(insert_data).execute()
    if not res.data:
        print(f"⚠️ INSERT失敗: {insert_data}")

print("✅ 完了！mst_site_item に集計結果を保存しました。")
