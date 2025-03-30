import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

df = pd.read_csv("stock_data.csv")
X = np.array(df["stock_status"]).reshape(-1,1)
y = np.array(df["stock_status"]).reshape(-1,1)

# LSTMモデル構築
model = Sequential([
    LSTM(50, return_sequences=True, input_shape=(X.shape[1], 1)),
    LSTM(50, return_sequences=False),
    Dense(25),
    Dense(1)
])

# モデル学習
model.compile(optimizer="adam", loss="mse")
model.fit(X, y, batch_size=1, epochs=10)

# 予測
predictions = model.predict(X)
