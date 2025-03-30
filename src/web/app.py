from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/forecast', methods=['GET'])
def get_forecast():
    df = pd.read_csv("stock_forecast.csv")
    return jsonify(df.to_dict())

if __name__ == '__main__':
    app.run(debug=True)
