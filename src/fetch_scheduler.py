import datetime
import time

from common.logger import log_info, log_response  # インポート

from database.supabase_insert import insert_stock_data
# from data_acquisition.fetch_amazon import fetch_amazon_stock
from data_acquisition.fetch_rakuten import fetch_rakuten_stock
from data_acquisition.fetch_yahoo import fetch_yahoo_stock

from data_acquisition.fetch_rakuten_from_mstItem import main_rakuten
from data_acquisition.fetch_yahoo_shopping_from_mstItem import main_yahoo

# while True:
# amazon_data = fetch_amazon_stock()
# amazon_data = [{"product_name": "PS5", "site": "Amazon", "stock_status": True}]
# # レスポンスをログファイルに保存
# log_response("amazon_data",amazon_data)
# #insert_stock_data(amazon_data)
# log_info(f" 📦 amazon在庫データ更新完了")

rakuten_data = fetch_rakuten_stock()
# レスポンスをログファイルに保存
# log_response("rakuten_data",rakuten_data)
insert_stock_data(rakuten_data)
log_info(f" 📦 rakuten在庫データ更新完了")

yahoo_data = fetch_yahoo_stock()
# レスポンスをログファイルに保存
# log_response("yahoo_data",yahoo_data)
insert_stock_data(yahoo_data)
log_info(f" 📦 yahoo在庫データ更新完了")

main_rakuten()
log_info(f" 📦 rakuten 過去にランキングに上がった商品ごとの在庫データ更新完了")

main_yahoo()
log_info(f" 📦 yashoo 過去にランキングに上がった商品ごとの在庫データ更新完了")


log_info(f" 📦 すべての在庫データ更新完了")
log_info("-" * 50 + "\n")

# # time.sleep(3600)  # 1時間ごとに実行
