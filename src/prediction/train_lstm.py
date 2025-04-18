import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from supabase import Client, create_client
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import uuid

# .env ファイルの読み込み
load_dotenv()

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

# Supabase の設定
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_stock_data():
    """Supabase から在庫データを取得"""
    try:
        response = supabase.table("trn_ranked_item_stock_pretreatment").select("update_time, site, seller_site, product_id, stock_status").execute()
        if "error" in response and response["error"]:
            print(f"❌ HTTPエラー: {response['error']}")
            return None
        
        if response.data:
            df = pd.DataFrame(response.data)
            df["update_time"] = pd.to_datetime(df["update_time"])
            df.sort_values(["site", "seller_site", "product_id", "update_time"], inplace=True)
            return df
        else:
            print("⚠ データが取得できませんでした。")
            return None
    except Exception as e:
        print(f"❌ データ取得エラー: {e}")
        return None

def prepare_data(df):
    """LSTM 用のデータ前処理"""
    X = np.array(df["stock_status"]).reshape(-1, 1, 1)  # (サンプル数, 時系列の長さ, 特徴量の数)
    y = np.array(df["stock_status"]).reshape(-1, 1)  # (サンプル数, 1)
    return X, y

def build_lstm_model():
    """LSTM モデルの構築"""
    model = Sequential([
        LSTM(50, return_sequences=False, input_shape=(1, 1)),
        Dense(25, activation="relu"),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse")
    return model

def save_forecast_to_supabase(df, predictions, site, seller_site, product_id):
    """予測データを Supabase に保存"""
    try:
        existing_times = {
            (row["forecast_datetime"], row["site"], row["seller_site"], row["product_id"])
            for row in supabase.table("stock_forecast_lstm").select("forecast_datetime, site, seller_site, product_id").execute().data
        }

        records = []
        
        # 予測と df の行数が一致するか確認
        if len(predictions) != len(df):
            print(f"⚠ 予測データと実際のデータの長さが一致しません: {len(predictions)} != {len(df)}")
            return  # 一致しない場合は保存しない
        
        df = df.reset_index(drop=True)
        # 予測をデータフレームのインデックス順に保存
        for i, row in df.iterrows():
            if i >= len(predictions):  # predictionsの範囲外にアクセスしないように確認
                print(f"⚠ インデックス {i} は予測結果の範囲を超えています。")
                break
            
            forecast_datetime = row["update_time"].isoformat()  # `forecast_datetime`に変更
            forecast = float(predictions[i][0])  # LSTM の予測結果

            if (forecast_datetime, site, seller_site, product_id) in existing_times:
                print(f"⚠ 重複データをスキップ: {forecast_datetime} - {site} - {seller_site} - {product_id}")
                continue

            records.append({
                "id": str(uuid.uuid4()),
                "forecast_datetime": forecast_datetime,  # `forecast_datetime`に変更
                "forecast": forecast,
                "site": site,
                "seller_site": seller_site,
                "product_id": product_id
            })

        if records:
            response = supabase.table("stock_forecast_lstm").insert(records).execute()
            if "error" in response and response["error"]:
                print(f"❌ HTTPエラー: {response['error']}")
            else:
                print(f"✅ {site} - {seller_site} - {product_id} の予測データを Supabase に保存しました！")
                
                # 予測結果をファイルにも保存
                forecast_df = pd.DataFrame(records)
                forecast_df.to_csv(f"forecast_{site}_{seller_site}_{product_id}.csv", index=False)
                print(f"✅ 予測結果をファイルに保存しました！")
        else:
            print("⚠ すべてのデータが既存のエントリと重複していたため、保存をスキップしました。")
    except Exception as e:
        print(f"❌ データ保存エラー: {e}")


def main():
    df = fetch_stock_data()
    if df is None or df.empty:
        print("⚠ データがないため、処理を中断します。")
        return

    grouped = df.groupby(["site", "seller_site", "product_id"])
    for (site, seller_site, product_id), group in grouped:
        X, y = prepare_data(group)
        
        # LSTMモデルを構築 & 学習
        model = build_lstm_model()
        model.fit(X, y, batch_size=8, epochs=10, verbose=1)
        
        # 予測
        predictions = model.predict(X)
        
        # 予測結果を保存
        save_forecast_to_supabase(group, predictions, site, seller_site, product_id)

if __name__ == "__main__":
    main()
