import time
import json
import boto3
from kafka import KafkaConsumer

# Minio Client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
)

bucket_name = "bronze-stocks-transaction"
# Producing: {'c': 308.33, 'd': -0.49, 'dp': -0.1587, 'h': 311.82, 'l': 307.67, 'o': 309.56, 'pc': 308.82, 't': 1779825600, 'symbol': 'AAPL', 'fetched_at': 1779857936}

stocks_consumer = KafkaConsumer(
    "stocks-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-stocks-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
print("Consumer streaming and saving to Minio")

for message in stocks_consumer:
    record = (
        message.value
    )  # sample >> "symbol": "TSLA","price": 375.22,"fetched_at": 1777445144
    symbol = record.get("symbol", "UNKNOWN")  # get only symbol >> TSLA
    ts = record.get(
        "fetched_at", int(time.time())
    )  # same with this get only fetched_at  >> current time  >>1777445144
    key = f"stocks/{symbol}/{ts}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json",
    )
    print(f"Saved record for {symbol}) = s3://{bucket_name}/{key}")
