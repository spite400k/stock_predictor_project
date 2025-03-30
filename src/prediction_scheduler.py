from common.logger import log_info, log_response
from prediction import train_arima
from prediction.pretreatment import pretreatment  # インポート

# while True:
pretreatment()
log_info(f" 📦 前処理完了")

train_arima()
log_info(f" 📦 ARIMA完了")


# time.sleep(3600)  # 1時間ごとに実行
