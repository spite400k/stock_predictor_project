import os
from dotenv import load_dotenv
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta
import uuid
from supabase import create_client, Client

# .env ファイルの読み込み
load_dotenv()

# Supabaseの設定
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_stock_data():
    """ stock_history_pretreatment からデータ取得 """
    try:
        response = supabase.table("stock_history_pretreatment").select("*").execute()
        
        if "error" in response and response["error"]:
            print(f"❌ HTTPエラー: {response['error']}")
            return None
        
        if response.data:
            df = pd.DataFrame(response.data)

            # update_time のカラム名確認
            print("カラム名:", df.columns)

            # update_time カラムが存在するか確認
            if 'update_time' not in df.columns:
                raise ValueError("❌ 'update_time' カラムがデータフレームに存在しません")

            df["update_time"] = pd.to_datetime(df["update_time"])
            df["stockout_time"] = pd.to_datetime(df["stockout_time"])
            df["restock_time"] = pd.to_datetime(df["restock_time"])
            
            df.sort_values(["site", "seller_site", "product_id", "update_time"], inplace=True)
            df.set_index("update_time", inplace=True)

            df["stockout_duration"] = (df["restock_time"] - df["stockout_time"]).dt.days.fillna(0)
            df["restock_duration"] = (df["stockout_time"] - df["restock_time"].shift(1)).dt.days.fillna(0)
            
            df["day_of_week"] = df.index.dayofweek
            df["month"] = df.index.month
            
            return df
        else:
            print("⚠ データが取得できませんでした。")
            return None
    except Exception as e:
        print(f"❌ データ取得エラー: {e}")
        return None

def train_arima_and_forecast(df, site=None, seller_site=None, product_id=None):
    """ ARIMAモデルを学習し、10ステップ先を予測 """
    # 🔹 インデックスを時系列データに修正
    if 'update_time' in df.columns:
        df["update_time"] = pd.to_datetime(df["update_time"])
        df.set_index("update_time", inplace=True)
    else:
        raise ValueError("❌ 'update_time' カラムが見つかりません")

    # 🔹 インデックスの頻度を推定して補完
    inferred_freq = pd.infer_freq(df.index)
    df = df.asfreq(inferred_freq if inferred_freq else "D")
    df.ffill(inplace=True)

    # 🔹 インデックスが DatetimeIndex かチェック
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("❌ 'update_time' が DatetimeIndex になっていません！")

    # 🔹 在庫のトレンドを計算
    df["stock_trend"] = df["stock_status"].astype(float).rolling(window=7, min_periods=1).mean()

    # 🔹 ARIMA モデルのオーダーを決定
    order = (1,0,0) if len(df) < 30 else (5,1,0)

    try:
        # 🔹 ARIMA モデルを学習
        model = ARIMA(df["stock_trend"], order=order)
        model_fit = model.fit(method_kwargs={"solver": "lbfgs"})
    except Exception as e:
        print(f"❌ ARIMAモデルの学習中にエラー: {e}")
        return None

    # 🔹 10日先までの予測
    future_dates = [df.index[-1] + timedelta(days=i) for i in range(1, 11)]
    forecast_values = model_fit.forecast(steps=10)

    # 🔹 予測結果をデータフレームに格納
    forecast_df = pd.DataFrame({"update_time": future_dates, "forecast": forecast_values})
    
    # site, seller_site, product_id を追加
    forecast_df["site"] = site
    forecast_df["seller_site"] = seller_site
    forecast_df["product_id"] = product_id
    
    return forecast_df


def save_forecast_to_supabase(forecast_df):
    """ 予測データを stock_forecast テーブルに保存 """
    try:
        # 既存の予測データの確認 (update_time, site, seller_site, product_id の組み合わせ)
        existing_times = {
            (row["update_time"], row["site"], row["seller_site"], row["product_id"])
            for row in supabase.table("stock_forecast").select("update_time, site, seller_site, product_id").execute().data
        }

        records = []
        for row in forecast_df.itertuples(index=False):
            if (row.update_time.isoformat(), row.site, row.seller_site, row.product_id) in existing_times:
                print(f"⚠ 重複データをスキップ: {row.update_time} - {row.site} - {row.seller_site} - {row.product_id}")
                continue
            records.append({
                "id": str(uuid.uuid4()),
                "update_time": row.update_time.isoformat(),
                "forecast": row.forecast,
                "site": row.site,
                "seller_site": row.seller_site,
                "product_id": row.product_id
            })

        if records:
            response = supabase.table("stock_forecast").insert(records).execute()
            if "error" in response and response["error"]:
                print(f"❌ HTTPエラー: {response['error']}")
            else:
                print("✅ 予測データを Supabase に保存しました！")
        else:
            print("⚠ すべてのデータが既存のエントリと重複していたため、保存をスキップしました。")
    except Exception as e:
        print(f"❌ データ保存エラー: {e}")

def main():
    df = fetch_stock_data()
    if df is not None and not df.empty:
        grouped = df.groupby(["site", "seller_site", "product_id"])
        all_forecasts = []
        
        for (site, seller_site, product_id), group in grouped:
            forecast_df = train_arima_and_forecast(group, site, seller_site, product_id)
            if forecast_df is not None:
                all_forecasts.append(forecast_df)
        
        if all_forecasts:
            save_forecast_to_supabase(pd.concat(all_forecasts))
    else:
        print("⚠ データがないため、予測を実行しません。")

if __name__ == "__main__":
    main()
