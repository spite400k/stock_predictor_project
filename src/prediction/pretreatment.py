from datetime import datetime
import math
import json
import os
import sys
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client, Client  # supabase-py使ってる前提
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../common")))
from logger import log_info

# .env ファイルの読み込み
load_dotenv()

# Supabase の接続情報
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ISO8601形式に揃える関数
def to_isoformat(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        try:
            # すでにISOならそのまま
            dt = datetime.fromisoformat(value)
            return dt.isoformat()
        except ValueError:
            return value
    return value

# レコード1件をクリーン化する関数
def clean_record(record):
    # NaTや空文字列をNoneにする
    for key in ["stockout_time", "restock_time"]:
        if str(record.get(key)) in ["NaT", "nat", "NaN", "", None]:
            record[key] = None

    # prev_stock_statusを整数化（またはNone）
    if "prev_stock_status" in record:
        if record["prev_stock_status"] in ["", None, math.nan]:
            record["prev_stock_status"] = None
        elif isinstance(record["prev_stock_status"], float) and not math.isnan(record["prev_stock_status"]):
            record["prev_stock_status"] = int(record["prev_stock_status"])

    # 日付系をISO8601に統一
    for key in ["insert_time", "update_time", "stockout_time", "restock_time"]:
        if record.get(key):
            record[key] = to_isoformat(record[key])

    return record


# バッチで一括登録する関数
def insert_stock_data(df):
    batch_size = 500  # バッチサイズはSupabaseの制限に合わせる
    records = df.to_dict(orient="records")

    log_info(f"★送信予定データ件数: {len(records)}件")

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        batch = [clean_record(r) for r in batch]

        # バッチの最初と最後をダンプして確認
        log_info("★送信する1件目のデータ:")
        log_info(json.dumps(batch[0], indent=2, ensure_ascii=False, default=str))

        try:
            response = supabase.table("trn_ranked_item_stock_pretreatment").upsert(batch).execute()
            log_info(f"✅ バッチ {i//batch_size+1}: 登録成功！")
        except Exception as e:
            log_info(f"❌ バッチ {i//batch_size+1}: 登録失敗")
            log_info(f"エラー内容: {e}")
            # エラー原因をちゃんと見るため、詳細表示
            if hasattr(e, 'args') and e.args:
                log_info(f"エラー内容: {e.args}")
                print("エラー詳細:", e.args)

def fetch_stock_data():
    """
    Supabase から在庫データを取得し、リストとして返す。
    """
    batch_size = 1000  # 1回の取得件数
    offset = 0
    all_records = []

    try:
        while True:
            response = supabase.table("trn_ranked_item_stock").select("*").range(offset, offset + batch_size - 1).execute()
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


# 実行テスト
if __name__ == "__main__":
    pretreatment()