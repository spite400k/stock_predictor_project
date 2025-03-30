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
        df["insert_time"] = pd.to_datetime(df["insert_time"])
        df["update_time"] = pd.to_datetime(df["update_time"])

        # 在庫ステータスを数値に変換
        df["stock_status"] = df["stock_status"].astype(int)

        # 在庫切れ・補充のタイミングを判定するカラムを追加
        df.sort_values(by=["product_id", "insert_time"], inplace=True)

        df["prev_stock_status"] = df.groupby("product_id")["stock_status"].shift(1)

        # 在庫切れ・補充の時間を設定
        df["stockout_time"] = df.apply(
            lambda row: row["insert_time"] if row["prev_stock_status"] == 1 and row["stock_status"] == 0 else None, axis=1
        )
        df["restock_time"] = df.apply(
            lambda row: row["insert_time"] if row["prev_stock_status"] == 0 and row["stock_status"] == 1 else None, axis=1
        )

        # 同じ product_id 内で stockout_time と restock_time を前の行から引き継ぐ
        df["stockout_time"] = df.groupby("product_id")["stockout_time"].fillna(method="ffill")
        df["restock_time"] = df.groupby("product_id")["restock_time"].fillna(method="ffill")

        # 不要なカラムを削除
        # df.drop(columns=["prev_stock_status"], inplace=True)

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
    # 日付データを文字列に変換（Supabase の JSON 形式に対応）
    df["prev_stock_status"] = df["prev_stock_status"].fillna(0)
    df["insert_time"] = df["insert_time"].astype(str)
    df["update_time"] = df["update_time"].astype(str)
    df["stockout_time"] = df["stockout_time"].astype(str).where(df['stockout_time'].notna(), None)
    df["restock_time"] = df["restock_time"].astype(str).where(df['restock_time'].notna(), None)

    # DataFrame を辞書のリストに変換
    records = df.to_dict(orient="records")

    # Supabase にデータを一括挿入
    response = supabase.table("stock_history_pretreatment").upsert(records).execute()

    if "data" in response and response.data:
        log_info("✅ データの挿入が完了しました。")
    else:
        log_info(f"❌ データ挿入エラー: {response}")

# 実行テスト
if __name__ == "__main__":
    pretreatment()
