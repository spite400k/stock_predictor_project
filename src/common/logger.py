import datetime
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler

# # ログディレクトリのパス
# LOG_DIR = r"C:\Users\kazuk\python\stock_predictor_project\logs"
# os.makedirs(LOG_DIR, exist_ok=True)  # ディレクトリが存在しない場合は作成
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)  # ディレクトリが存在しない場合は作成


# 各ログファイルのパス
rakuten_response_log_file = os.path.join(LOG_DIR, "response_rakuten_log.log")
yahoo_response_log_file = os.path.join(LOG_DIR, "response_yahoo_log.log")
error_log_file = os.path.join(LOG_DIR, "error_log.log")
info_log_file = os.path.join(LOG_DIR, "info_log.log")

# ロガーの設定
def setup_logger(name, log_file):
    logger = logging.getLogger(name)
    
    # すでにハンドラが設定されている場合は追加しない
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(logging.INFO)
    
    # 日付ごとにローテーションし、過去30日分保持
    handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    handler.suffix = "%Y-%m-%d"  # ローテーションしたログに日付を追加
    handler.extMatch = r"^\d{4}-\d{2}-\d{2}$"  # ローテーション時に適用される日付フォーマット
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

rakuten_response_logger = setup_logger("rakuten_response_logger", rakuten_response_log_file)
yahoo_response_logger = setup_logger("yahoo_response_logger", yahoo_response_log_file)
error_logger = setup_logger("error_logger", error_log_file)
info_logger = setup_logger("info_logger", info_log_file)

# 古いログを削除する関数
def delete_old_logs():
    cutoff_time = time.time() - (30 * 24 * 60 * 60)  # 30日前のタイムスタンプ
    for log_file in os.listdir(LOG_DIR):
        log_path = os.path.join(LOG_DIR, log_file)
        if os.path.isfile(log_path) and os.path.getmtime(log_path) < cutoff_time:
            os.remove(log_path)

# 日付が変わったら実行する処理
last_checked_date = datetime.date.today()

def check_and_cleanup_logs():
    global last_checked_date
    today = datetime.date.today()
    
    if today > last_checked_date:
        delete_old_logs()
        last_checked_date = today

# ログ記録関数
def log_response(data_type, response):
    check_and_cleanup_logs()

    if data_type.lower() == "rakuten":
        rakuten_response_logger.info(f"[{data_type}] Response: {response}")
    elif data_type.lower() == "yahoo_data":
        yahoo_response_logger.info(f"[{data_type}] Response: {response}")
    else:
        # どちらにも該当しない場合は、info_logに記録
        info_logger.info(f"[{data_type}] Response: {response}")

def log_error(error_message):
    check_and_cleanup_logs()
    error_logger.error(f"ERROR: {error_message}")
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ERROR: {error_message}")

def log_info(info_message):
    check_and_cleanup_logs()
    info_logger.info(f"INFO: {info_message}")
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} INFO: {info_message}")
