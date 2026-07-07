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

# Extracting: {'binancecoin': {'usd': 653.1, 'usd_market_cap': 87922528655.27834, 'usd_24h_vol': 1015962948.1227927, 'usd_24h_change': -0.8272797302795882, 'last_updated_at': 1779857925
crypto_consumer = KafkaConsumer(
    "crypto-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-crypto-consumer1",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
print("Consumer streaming and saving to Minio")

for message in crypto_consumer:
    record = message.value
    crypto = record.get("crypto")
    ts = record.get("last_updated_at", int(time.time()))
    key = f"crypto/{crypto}/{ts}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json",
    )
    print(f"Saved crypto for {crypto}) = s3://{bucket_name}/{key}")
