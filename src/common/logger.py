import datetime
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler

# ログディレクトリのパス
LOG_DIR = r"C:\Users\kazuk\python\stock_predictor_project\logs"
#LOG_DIR = r"./logs"
os.makedirs(LOG_DIR, exist_ok=True)  # ディレクトリが存在しない場合は作成

# 各ログファイルのパス
response_log_file = os.path.join(LOG_DIR, "response_log.txt")
error_log_file = os.path.join(LOG_DIR, "error_log.txt")
info_log_file = os.path.join(LOG_DIR, "info_log.txt")

# ロガーの設定
def setup_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(log_file, when="D", interval=1, backupCount=30, encoding="utf-8")  # 30日分保持
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

response_logger = setup_logger("response_logger", response_log_file)
error_logger = setup_logger("error_logger", error_log_file)
info_logger = setup_logger("info_logger", info_log_file)

# 古いログを削除する関数
def delete_old_logs():
    cutoff_time = time.time() - (30 * 24 * 60 * 60)  # 30日前のタイムスタンプ
    for log_file in os.listdir(LOG_DIR):
        log_path = os.path.join(LOG_DIR, log_file)
        if os.path.isfile(log_path) and os.path.getmtime(log_path) < cutoff_time:
            os.remove(log_path)

# ログ記録関数
def log_response(data_type, response):
    response_logger.info(f"[{data_type}] Response: {response}")
    delete_old_logs()

def log_error(error_message):
    error_logger.error(f"ERROR: {error_message}")
    delete_old_logs()
    timestamp = datetime.datetime.now().isoformat()
    print(f"{timestamp} {error_message}")

def log_info(info_message):
    info_logger.info(f"INFO: {info_message}")
    delete_old_logs()

    timestamp = datetime.datetime.now().isoformat()
    print(f"{timestamp} {info_message}")

