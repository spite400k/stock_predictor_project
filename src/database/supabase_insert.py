import datetime
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# .env ファイルの読み込み（環境変数の設定）
load_dotenv()

# Supabase API キー & URL
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Supabase クライアントを作成
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 在庫データを Supabase に追加する関数
def update_stock_in_supabase(product_id, product_name, site, stock_status, description="", seller_site="", price=0):
    timestamp = datetime.datetime.now().isoformat()
    data = {
        "product_id": product_id,
        "product_name": product_name,
        "description": description,  # デフォルト空文字
        "site": site,
        "seller_site": seller_site,  # デフォルト空文字
        "stock_status": stock_status,
        "price": price,  # デフォルト 0
        "insert_time": timestamp,  # 明示的に送信
        "update_time": timestamp,  # 明示的に送信
    }

    response = supabase.table("stock_history").insert(data).execute()

    if response.data:
        # print(f"✅ {site} の {product_name} データが正常に挿入されました")
        return data
    else:
        print(f"❌ エラー: {response.status_code}, {response.text}")  # エラーメッセージを表示
        return None

# 在庫データをリストで追加
def insert_stock_data(data_list):
    if data_list:
        for d in data_list:
            update_stock_in_supabase(
                product_id=d["product_id"],
                product_name=d["product_name"],
                site=d["site"],
                stock_status=d["stock_status"],
                description=d.get("description", ""),
                seller_site=d.get("seller_site", ""),
                price=d.get("price", 0),
            )


# 実行テスト
if __name__ == "__main__":
    amazon_data = {
        "product_id": "AMZ123",
        "product_name": "PS3",
        "site": "Amazon",
        "stock_status": True,
        "description": "Sony PlayStation 3",
        "sellerSite": "Amazon JP",
        "price": 20000,
    }

    rakuten_data = {
        "product_id": "RAK456",
        "product_name": "PS4",
        "site": "Rakuten",
        "stock_status": True,
        "description": "Sony PlayStation 4",
        "sellerSite": "Rakuten Store",
        "price": 30000,
    }

    yahoo_data = {
        "product_id": "YAH789",
        "product_name": "PS5",
        "site": "Yahoo",
        "stock_status": True,
        "description": "Sony PlayStation 5",
        "sellerSite": "Yahoo Shopping",
        "price": 50000,
    }

    data_sources = [amazon_data, rakuten_data, yahoo_data]
    insert_stock_data(data_sources)
