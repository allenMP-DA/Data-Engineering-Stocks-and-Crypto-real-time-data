import json
import os
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from datetime import datetime

# -----------------------------
# CONFIG (Docker-safe)
# -----------------------------
# MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-stocks-transaction"

LOCAL_DIR_STOCKS = "/tmp/minio_downloads/stocks"
LOCAL_DIR_CRYPTO = "/tmp/minio_downloads/crypto"

MYSQL_CONN = "mysql+pymysql://user:password@mysql:3306/stocks_db"


# -----------------------------
# EXTRACT FROM MINIO
# -----------------------------
def extract_from_minio(prefix, local_dir):
    os.makedirs(local_dir, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    local_files = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            local_file = os.path.join(local_dir, os.path.basename(key))

            if os.path.exists(local_file):
                continue

            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")

            local_files.append(local_file)

    return local_files


# -----------------------------
# TRANSFORM STOCKS
# -----------------------------
def transform_stocks(ti):
    files = ti.xcom_pull(task_ids="extract_stocks")

    records = []

    for file in files:
        with open(file, "r") as f:
            data = json.load(f)

        if not data.get("c") or not data.get("fetched_at"):
            continue

        records.append(data)

    df = pd.DataFrame(records)

    if df.empty:
        return []

    df = df.rename(
        columns={
            "c": "current_price",
            "d": "change_amount",
            "dp": "change_percent",
            "h": "day_high",
            "l": "day_low",
            "o": "day_open",
            "pc": "prev_close",
            "t": "market_timestamp",
        }
    )

    df["fetched_at"] = pd.to_datetime(df["fetched_at"], unit="s")
    df["market_timestamp"] = pd.to_datetime(df["market_timestamp"], unit="s")
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], unit="s").dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    df["market_timestamp"] = pd.to_datetime(
        df["market_timestamp"], unit="s"
    ).dt.strftime("%Y-%m-%dT%H:%M:%S")

    return df.to_dict(orient="records")


# -----------------------------
# TRANSFORM CRYPTO
# -----------------------------
def transform_crypto(ti):
    files = ti.xcom_pull(task_ids="extract_crypto")

    records = []

    for file in files:
        with open(file, "r") as f:
            data = json.load(f)

        if not data.get("usd") or not data.get("last_updated_at"):
            continue

        records.append(data)

    df = pd.DataFrame(records)

    if df.empty:
        return []

    df["last_updated_at"] = pd.to_datetime(df["last_updated_at"], unit="s")
    df["last_updated_at"] = df["last_updated_at"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    return df.to_dict(orient="records")


# -----------------------------
# LOAD STOCKS
# -----------------------------
def load_stocks(ti):
    json_data = ti.xcom_pull(task_ids="transform_stocks")

    if not json_data:
        return

    df = pd.DataFrame(json_data)

    engine = create_engine(MYSQL_CONN)

    df.to_sql("stock_quotes", engine, if_exists="append", index=False)

    print("Stocks loaded successfully")


# -----------------------------
# LOAD CRYPTO
# -----------------------------
def load_crypto(ti):
    json_data = ti.xcom_pull(task_ids="transform_crypto")

    if not json_data:
        return

    df = pd.DataFrame(json_data)
    df["last_updated_at"] = pd.to_datetime(df["last_updated_at"])

    engine = create_engine(MYSQL_CONN)

    df.to_sql("crypto_quotes", engine, if_exists="append", index=False)

    print("Crypto loaded successfully")


## -------
# SILVER STOCK TABLE
## -------
def load_silver_stocks():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT IGNORE INTO SILVER_STOCKS
    SELECT
        symbol,
        current_price,
        ROUND(day_high, 2) AS day_high,
        ROUND(day_low, 2) AS day_low,
        ROUND(day_open, 2) AS day_open,
        ROUND(prev_close, 2) AS prev_close,
        change_amount,
        ROUND(change_percent, 4) AS change_percent,
        market_timestamp,
        fetched_at
    FROM stock_quotes
    WHERE current_price IS NOT NULL;
    """

    with engine.begin() as conn:
        conn.execute(text(query))


## ------------
# Silver crypto data
# -------------
def load_silver_crypto():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT IGNORE INTO SILVER_CRYPTO
    SELECT 
        crypto, 
	    ROUND(usd, 5) as usd_price,
	    ROUND(usd_market_cap, 5) as market_cap,
	    usd_24h_vol, 
	    ROUND(usd_24h_change, 2) as change_24hr,
	    last_updated_at
from crypto_quotes 
where usd IS NOT NULL;
    """

    with engine.begin() as conn:
        conn.execute(text(query))


## ------------
# Gold stocks data
# -------------


def load_gold_stocks_kpi():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT INTO GOLD_STOCKS_KPI (
    symbol,
    current_price,
    change_amount,
    change_percent
)
SELECT
    symbol,
    current_price,
    change_amount,
    change_percent
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY fetched_at DESC
           ) AS rn
    FROM SILVER_STOCKS
) t
WHERE rn = 1
ON DUPLICATE KEY UPDATE
    current_price = VALUES(current_price),
    change_amount = VALUES(change_amount),
    change_percent = VALUES(change_percent);
    """

    with engine.begin() as conn:
        conn.execute(text(query))


def load_gold_stocks_treechart():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT INTO GOLD_STOCKS_TREECHART
    	WITH source AS (
	  SELECT
	    symbol,
	    CAST(current_price AS DOUBLE) AS current_price_dbl,
	    market_timestamp
	  FROM SILVER_STOCKS
	  -- optionally filter invalid rows:
	  WHERE CAST(current_price AS DOUBLE) IS NOT NULL
	),
	latest_day AS (
	  -- if market_timestamp is epoch seconds (NUMBER/INT):
	  SELECT CAST(DATE(MAX(market_timestamp)) AS DATE) AS max_day
	  FROM source
	),
	latest_prices AS (
	  SELECT
	    symbol,
	    AVG(current_price_dbl) AS avg_price
	  FROM source
	  JOIN latest_day ld
	    ON CAST(DATE(market_timestamp) AS DATE) = ld.max_day
	  GROUP BY symbol
	),
	all_time_volatility AS (
	  SELECT
	    symbol,
	    STDDEV_POP(current_price_dbl) AS volatility,             
	    CASE
	      WHEN AVG(current_price_dbl) = 0 THEN NULL
	      ELSE STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0)
	    END AS relative_volatility
	  FROM source
	  GROUP BY symbol
	)
	SELECT
	  lp.symbol,
	  lp.avg_price,
	  v.volatility,
	  v.relative_volatility
	FROM latest_prices lp
	JOIN all_time_volatility v ON lp.symbol = v.symbol
	ORDER BY lp.symbol
    ON DUPLICATE KEY UPDATE
    avg_price = VALUES(avg_price),
    volatility = VALUES(volatility),
    relative_volatility = VALUES(relative_volatility);

    """
    with engine.begin() as conn:
        conn.execute(text(query))


def load_gold_stocks_candlestick():
    engine = create_engine(MYSQL_CONN)

    query = """
   INSERT INTO GOLD_STOCKS_CANDLESTICK (
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
)
WITH enriched AS (
    SELECT
        symbol,
        CAST(market_timestamp AS DATE) AS trade_date,
        day_low,
        day_high,
        current_price,
        FIRST_VALUE(current_price) OVER (
            PARTITION BY symbol, CAST(market_timestamp AS DATE)
            ORDER BY market_timestamp
        ) AS candle_open,
        LAST_VALUE(current_price) OVER (
            PARTITION BY symbol, CAST(market_timestamp AS DATE)
            ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_close
    FROM SILVER_STOCKS
),
candles AS (
    SELECT
        symbol,
        trade_date AS candle_time,
        MIN(day_low) AS candle_low,
        MAX(day_high) AS candle_high,
        ANY_VALUE(candle_open) AS candle_open,
        ANY_VALUE(candle_close) AS candle_close,
        AVG(current_price) AS trend_line
    FROM enriched
    GROUP BY symbol, trade_date
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY candle_time DESC
           ) rn
    FROM candles
)
SELECT
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
FROM ranked
WHERE rn <= 12
ON DUPLICATE KEY UPDATE
    candle_low = VALUES(candle_low),
    candle_high = VALUES(candle_high),
    candle_open = VALUES(candle_open),
    candle_close = VALUES(candle_close),
    trend_line = VALUES(trend_line);
    """
    with engine.begin() as conn:
        conn.execute(text(query))


## ------------
# Gold crypto data
# -------------


def load_gold_crypto_kpi():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT INTO GOLD_CRYPTO_KPI (
    crypto,
    current_price,
    market_cap,
    trading_volume_24h,
    market_cap_ratio
)
SELECT 
    crypto,
    usd_price,
    market_cap,
    ROUND(usd_24h_vol, 2),
    ROUND(usd_24h_vol, 2) / NULLIF(market_cap, 0)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY crypto
               ORDER BY last_updated_at DESC
           ) AS rn
    FROM SILVER_CRYPTO
) t
WHERE rn = 1
ON DUPLICATE KEY UPDATE
    current_price = VALUES(current_price),
    market_cap = VALUES(market_cap),
    trading_volume_24h = VALUES(trading_volume_24h),
    market_cap_ratio = VALUES(market_cap_ratio);
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def load_gold_crypto_treechart():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT INTO GOLD_CRYPTO_TREECHART
    WITH source AS (
    SELECT 
        crypto,
        CAST(usd_price AS DOUBLE) AS current_price,
        last_updated_at
    FROM SILVER_CRYPTO
    WHERE CAST(usd_price AS DOUBLE) IS NOT NULL
),

latest_day AS (
    SELECT DATE(MAX(last_updated_at)) AS max_day
    FROM source
),

latest_price AS (
    SELECT
        s.crypto,
        AVG(s.current_price) AS avg_price
    FROM source s
    JOIN latest_day ld
        ON DATE(s.last_updated_at) = ld.max_day
    GROUP BY s.crypto
),

all_time_volatility AS (	
    SELECT
        crypto,
        STDDEV_POP(current_price) AS volatility,
        STDDEV_POP(current_price) / NULLIF(AVG(current_price), 0) AS relative_volatility
    FROM source
    GROUP BY crypto
)

SELECT 
    lp.crypto,
    lp.avg_price,
    v.volatility,
    v.relative_volatility
FROM latest_price lp
JOIN all_time_volatility v 
    ON lp.crypto = v.crypto
ON DUPLICATE KEY UPDATE
    avg_price = VALUES(avg_price),
    volatility = VALUES(volatility),
    relative_volatility = VALUES(relative_volatility);
    """
    with engine.begin() as conn:
        conn.execute(text(query))


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="market_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:
    # ---------------- STOCKS ----------------
    extract_stocks = PythonOperator(
        task_id="extract_stocks",
        python_callable=extract_from_minio,
        op_kwargs={
            "prefix": "stocks/",
            "local_dir": LOCAL_DIR_STOCKS,
        },
        do_xcom_push=True,
    )

    transform_stocks_task = PythonOperator(
        task_id="transform_stocks",
        python_callable=transform_stocks,
    )

    load_stocks_task = PythonOperator(
        task_id="load_stocks",
        python_callable=load_stocks,
    )

    silver_stocks_task = PythonOperator(
        task_id="silver_stocks",
        python_callable=load_silver_stocks,
    )

    gold_stocks_kpi_task = PythonOperator(
        task_id="gold_stocks_kpi",
        python_callable=load_gold_stocks_kpi,
    )

    gold_stocks_treechart_task = PythonOperator(
        task_id="gold_stocks_treechart",
        python_callable=load_gold_stocks_treechart,
    )

    gold_stocks_candlestick_task = PythonOperator(
        task_id="gold_stocks_candlestick",
        python_callable=load_gold_stocks_candlestick,
    )

    # ---------------- CRYPTO ----------------
    extract_crypto = PythonOperator(
        task_id="extract_crypto",
        python_callable=extract_from_minio,
        op_kwargs={
            "prefix": "crypto/",
            "local_dir": LOCAL_DIR_CRYPTO,
        },
        do_xcom_push=True,
    )

    transform_crypto_task = PythonOperator(
        task_id="transform_crypto",
        python_callable=transform_crypto,
    )

    load_crypto_task = PythonOperator(
        task_id="load_crypto",
        python_callable=load_crypto,
    )

    silver_crypto_task = PythonOperator(
        task_id="silver_crypto",
        python_callable=load_silver_crypto,
    )

    gold_crypto_kpi_task = PythonOperator(
        task_id="gold_crypto_kpi",
        python_callable=load_gold_crypto_kpi,
    )

    gold_crypto_treechart_task = PythonOperator(
        task_id="gold_crypto_treechart",
        python_callable=load_gold_crypto_treechart,
    )

    # ---------------- PIPELINE FLOW ----------------
    (
        extract_stocks
        >> transform_stocks_task
        >> load_stocks_task
        >> silver_stocks_task
        >> [
            gold_stocks_kpi_task,
            gold_stocks_treechart_task,
            gold_stocks_candlestick_task,
        ]
    )
    (
        extract_crypto
        >> transform_crypto_task
        >> load_crypto_task
        >> silver_crypto_task
        >> [gold_crypto_kpi_task, gold_crypto_treechart_task]
    )
