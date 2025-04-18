import os
from dotenv import load_dotenv
import pandas as pd
from datetime import timedelta
import matplotlib.pyplot as plt
from supabase import create_client, Client
from pmdarima import auto_arima

# .env ファイルの読み込み
load_dotenv()

# Supabaseの設定
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_stock_data():
    """ trn_ranked_item_stock_pretreatment からデータ取得 """
    try:
        response = supabase.table("trn_ranked_item_stock_pretreatment").select("*").execute()

        if "error" in response and response["error"]:
            print(f"❌ HTTPエラー: {response['error']}")
            return None

        if response.data:
            df = pd.DataFrame(response.data)

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
    """ auto_arima を使って自動モデル選定・予測し、グラフ保存 """

    if df.index.nunique() < 3:
        print(f"⚠ データ数が少なすぎるためスキップ: site={site}, seller_site={seller_site}, product_id={product_id}")
        return None

    inferred_freq = pd.infer_freq(df.index)
    df = df.asfreq(inferred_freq if inferred_freq else "D")
    df.ffill(inplace=True)

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("❌ 'update_time' が DatetimeIndex になっていません！")

    df["stock_trend"] = df["stock_status"].astype(float).rolling(window=7, min_periods=1).mean()

    try:
        # 🔹 auto_arima による自動モデル選定
        model = auto_arima(
            df["stock_trend"],
            seasonal=False,
            stepwise=True,
            suppress_warnings=True,
            error_action='ignore',
            trace=False
        )

        forecast_values = model.predict(n_periods=10)
        future_dates = [df.index[-1] + timedelta(days=i) for i in range(1, 11)]

        forecast_df = pd.DataFrame({
            "update_time": future_dates,
            "forecast": forecast_values,
            "site": site,
            "seller_site": seller_site,
            "product_id": product_id
        })

        # 📊 グラフの保存
        plt.figure(figsize=(10, 6))
        plt.plot(df.index, df["stock_trend"], label="実測値（過去）")
        plt.plot(forecast_df["update_time"], forecast_df["forecast"], label="予測値", linestyle="--", color="red")
        plt.title(f"在庫予測: {site} / {seller_site} / {product_id}")
        plt.xlabel("日付")
        plt.ylabel("在庫トレンド")
        plt.legend()
        plt.grid(True)

        save_dir = "forecast_images2"
        os.makedirs(save_dir, exist_ok=True)
        filename = f"{save_dir}\{site}_{seller_site}_{product_id}.png".replace("/", "_")
        plt.savefig(filename)
        plt.close()
        print(f"✅ グラフ画像を保存しました: {filename}")

        return forecast_df

    except Exception as e:
        print(f"❌ auto_arima の学習エラー: {e}")
        return None


def save_forecast_to_supabase(forecast_df):
    """ 予測データを Supabase に保存 """
    try:
        records = []
        for row in forecast_df.itertuples(index=False):
            records.append({
                "forecast_datetime": row.update_time.isoformat(),
                "forecast": row.forecast,
                "site": row.site,
                "seller_site": row.seller_site,
                "product_id": row.product_id
            })

        if records:
            response = supabase.table("stock_forecast_arima") \
                .upsert(records, on_conflict=["forecast_datetime", "site", "seller_site", "product_id"]) \
                .execute()

            if "error" in response and response["error"]:
                print(f"❌ HTTPエラー: {response['error']}")
            else:
                print("✅ 予測データを Supabase に保存（または更新）しました！")
        else:
            print("⚠ 保存するデータがありません。")
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

        valid_forecasts = [df for df in all_forecasts if df is not None and not df.empty]
        if valid_forecasts:
            save_forecast_to_supabase(pd.concat(valid_forecasts))
        else:
            print("⚠ 有効な予測データが存在しないため、保存をスキップしました。")
    else:
        print("⚠ データがないため、予測を実行しません。")


if __name__ == "__main__":
    main()
