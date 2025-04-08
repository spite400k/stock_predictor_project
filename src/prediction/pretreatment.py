import os
import sys
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../common")))
from logger import log_info

# .env ファイルの読み込み
load_dotenv()

# Supabase の接続情報
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Supabase クライアントの作成
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def pretreatment():
    """
    在庫データを取得し、前処理を行い、
    在庫切れや補充のタイミングを判定してデータを挿入する。
    """
    data = fetch_stock_data()

    if data:
        df = pd.DataFrame(data)
        
        # 日付データの変換
        df["insert_time"] = pd.to_datetime(df["insert_time"], errors='coerce')
        df["update_time"] = pd.to_datetime(df["update_time"], errors='coerce')

        # 在庫ステータスを数値に変換（無効な値は 0 に置き換え）
        df["stock_status"] = pd.to_numeric(df["stock_status"], errors="coerce").fillna(0).astype(int)

        # 日付の欠損処理（NaT は最小日付に設定）
        df["insert_time"].fillna(df["insert_time"].min(), inplace=True)
        df["update_time"].fillna(df["update_time"].min(), inplace=True)

        # 在庫切れ・補充のタイミングを判定するカラムを追加
        df.sort_values(by=["product_id", "insert_time"], inplace=True)

        df["prev_stock_status"] = df.groupby("product_id")["stock_status"].shift(1)

        # 在庫切れ・補充の時間を設定
        df["stockout_time"] = df.loc[(df["prev_stock_status"] == 1) & (df["stock_status"] == 0), "insert_time"]
        df["restock_time"] = df.loc[(df["prev_stock_status"] == 0) & (df["stock_status"] == 1), "insert_time"]

        # 同じ product_id 内で stockout_time と restock_time を前の行から引き継ぐ
        df["stockout_time"] = df.groupby("product_id")["stockout_time"].fillna(method="ffill")
        df["restock_time"] = df.groupby("product_id")["restock_time"].fillna(method="ffill")

        # 重複データの削除（同じ product_id と insert_time のデータを削除）
        df = df.drop_duplicates(subset=["product_id", "insert_time"], keep="last")

        # データ挿入関数を呼び出す
        insert_stock_data(df)
    else:
        log_info("データ取得に失敗しました。")

def fetch_stock_data():
    """
    Supabase から在庫データを取得し、リストとして返す。
    """
    batch_size = 1000  # 1回の取得件数
    offset = 0
    all_records = []

    try:
        while True:
            response = supabase.table("stock_history").select("*").range(offset, offset + batch_size - 1).execute()
            if not response.data:
                break  # データがなければ終了
            
            all_records.extend(response.data)
            offset += batch_size  # 次の範囲へ

        if all_records:
            return all_records
        else:
            log_info("⚠ データが取得できませんでした。")
            return []

    except Exception as e:
        log_info(f"❌ データ取得中にエラー発生: {e}")
        return []

def insert_stock_data(df):
    """
    DataFrame のデータを Supabase の `stock_history_pretreatment` テーブルに挿入する。
    """
    batch_size = 1000  # 1回の挿入件数

    # 日付データを文字列に変換（Supabase の JSON 形式に対応）
    df["prev_stock_status"] = df["prev_stock_status"].fillna(0)
    df["insert_time"] = df["insert_time"].astype(str)
    df["update_time"] = df["update_time"].astype(str)
    df["stockout_time"] = df["stockout_time"].astype(str).where(df['stockout_time'].notna(), None)
    df["restock_time"] = df["restock_time"].astype(str).where(df['restock_time'].notna(), None)

    # DataFrame を辞書のリストに変換
    records = df.to_dict(orient="records")

    # 1000件ずつバッチ処理
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        response = supabase.table("stock_history_pretreatment").upsert(batch).execute()

        if "data" in response and response.data:
            log_info(f"✅ {len(batch)} 件のデータ挿入が完了しました。")
        else:
            log_info(f"❌ データ挿入エラー: {response}")

# 実行テスト
if __name__ == "__main__":
    pretreatment()
