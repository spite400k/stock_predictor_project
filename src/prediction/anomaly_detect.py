import pandas as pd
from sklearn.ensemble import IsolationForest

df = pd.read_csv("stock_data.csv")
X = df["stock_status"].values.reshape(-1, 1)

model = IsolationForest(contamination=0.1)
df["anomaly"] = model.fit_predict(X)

df.to_csv("anomaly_results.csv")
