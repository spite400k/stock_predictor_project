import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta
import streamlit as st
from pmdarima import auto_arima  # auto_arimaを使用

# Supabaseの設定
from dotenv import load_dotenv
from supabase import create_client, Client

# .env ファイルの読み込み
load_dotenv()

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
            # update_time のカラム名確認
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
    """ auto_arima モデルを学習し、予測 """
    if df.index.nunique() < 3:
        print(f"⚠ データ数が少なすぎるためスキップ: site={site}, seller_site={seller_site}, product_id={product_id}")
        return None

    df = df.asfreq("D", method='ffill')

    try:
        # auto_arima モデルで自動的に最適なパラメータを選定
        model = auto_arima(df["stock_status"], seasonal=False, stepwise=True, trace=True)
        forecast_values = model.predict(n_periods=10)

        # 予測結果をDataFrameに格納
        future_dates = [df.index[-1] + timedelta(days=i) for i in range(1, 11)]
        forecast_df = pd.DataFrame({"update_time": future_dates, "forecast": forecast_values})
        forecast_df["site"] = site
        forecast_df["seller_site"] = seller_site
        forecast_df["product_id"] = product_id
        
        return forecast_df
    except Exception as e:
        print(f"❌ ARIMAモデルの学習中にエラー: {e}")
        return None

def plot_forecast(forecast_df):
    """ 予測結果をプロット """
    fig = go.Figure()

    # 予測データのプロット
    fig.add_trace(go.Scatter(x=forecast_df['update_time'], y=forecast_df['forecast'], mode='lines', name='予測'))

    fig.update_layout(
        title="在庫予測",
        xaxis_title="日付",
        yaxis_title="在庫数",
        template="plotly_dark"
    )
    st.plotly_chart(fig)

def main():
    st.title("在庫予測アプリ")
    st.write("在庫の予測結果を表示します。")

    df = fetch_stock_data()
    if df is not None and not df.empty:
        grouped = df.groupby(["site", "seller_site", "product_id"])
        all_forecasts = []

        for (site, seller_site, product_id), group in grouped:
            forecast_df = train_arima_and_forecast(group, site, seller_site, product_id)
            if forecast_df is not None:
                all_forecasts.append(forecast_df)

        if all_forecasts:
            valid_forecasts = [df for df in all_forecasts if df is not None and not df.empty]
            if valid_forecasts:
                combined_forecasts = pd.concat(valid_forecasts)
                plot_forecast(combined_forecasts)
                st.write("予測結果を表示しました。")
            else:
                st.warning("有効な予測データがありません。")
        else:
            st.warning("予測を実行できませんでした。")
    else:
        st.warning("データがないため、予測を実行できません。")

if __name__ == "__main__":
    main()
