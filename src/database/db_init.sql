CREATE TABLE stock_history (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    description  VARCHAR(5000),
    site VARCHAR(20),
    seller_site VARCHAR(50),
    stock_status BOOLEAN,-- 在庫状況 (True: 在庫あり, False: 在庫なし)
    price INT,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE stock_history_pretreatment (
    id SERIAL PRIMARY KEY,            -- 一意の識別子
    product_id VARCHAR(50) NOT NULL,  -- 商品ID
    product_name VARCHAR(255),        -- 商品名
    description VARCHAR(5000),        -- 商品の説明
    site VARCHAR(20),                 -- 販売サイト (例: Amazon, Yahoo!)
    seller_site VARCHAR(50),          -- 販売元サイト
    prev_stock_status DECIMAL(10,2) NOT NULL,    -- 在庫状況 (True: 在庫あり, False: 在庫なし)
    stock_status DECIMAL(10,2) NOT NULL,    -- 在庫状況 (True: 在庫あり, False: 在庫なし)
    stockout_time TIMESTAMP,          -- 在庫切れになったタイミング
    restock_time TIMESTAMP,            -- 補充されたタイミング
    price INT,                        -- 価格
    day_of_week INT,
    month INT,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- データ登録時間
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- データ更新時間
);
CREATE TABLE stock_forecast_arima (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- 一意の識別子
    site VARCHAR(20) NOT NULL,                      -- 販売サイト (例: Amazon, Yahoo!)
    seller_site VARCHAR(50) NOT NULL,               -- 販売元サイト
    product_id VARCHAR(50) NOT NULL,                -- 商品ID
    forecast_datetime TIMESTAMP NOT NULL,                 -- 予測の対象日
    forecast FLOAT NOT NULL,                        -- 在庫予測
    created_at TIMESTAMP DEFAULT now(),             -- データ登録時間
    UNIQUE (site, seller_site, product_id) -- 各商品の特定日に対して一意制約
);


CREATE TABLE stock_forecast_lstm (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- 一意の識別子（UUID）
    site CHARACTER VARYING(20) NOT NULL,  -- EC サイト名（例: Amazon, Rakuten）
    seller_site CHARACTER VARYING(50) NOT NULL,  -- 販売者のサイト（例: 特定のショップ名）
    product_id CHARACTER VARYING(50) NOT NULL,  -- 商品の識別 ID
    forecast_time TIMESTAMP  NOT NULL,  -- 予測の対象となる日時
    forecast NUMERIC(10, 2) NOT NULL,  -- LSTM による在庫予測値
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- レコード作成日時（デフォルトで現在時刻）
    UNIQUE (site, seller_site, product_id) -- 各商品の特定日に対して一意制約
);

CREATE TABLE stock_summary (
  id serial PRIMARY KEY,
  site character varying(20) NOT NULL,
  seller_site character varying(50) NOT NULL,
  product_id character varying(50) NOT NULL,
  count integer NOT NULL,
  summary_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
