import time
import json
import requests
from kafka import KafkaProducer

FB_API_KEY = "d8bap89r01qhrj7qsc60d8bap89r01qhrj------"
CG_API_KEY = "CG-axK27LhxyfukiA4Qxg------"
FINNHUB_URL = "https://finnhub.io/api/v1/quote"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
CRYPTO_SYMBOLS = ["bitcoin", "ethereum", "tether", "binancecoin", "smooth-love-potion"]

# Initial Producer
producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


# https://finnhub.io/api/v1/quote?symbol=AAPL&token=d8akqc9r01qpujl2cvogd8akqc9r01qpujl2cvp0
# Retrieve Data
def fetch_quotes(symbol):
    url_1 = f"{FINNHUB_URL}?symbol={symbol}&token={FB_API_KEY}"
    try:
        response = requests.get(url_1)
        response.raise_for_status()
        data = response.json()  # convert json reposnse to python dictionary
        data["symbol"] = symbol  # Add stock symbol
        data["fetched_at"] = int(time.time())  # Add fetch timestamp
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


# Retrieve Data from crypto
# https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&x_cg_demo_api_key=YOUR_KEY
def fetch_symbols(crypto):
    url_2 = "https://api.coingecko.com/api/v3/simple/price"

    params = {
        "crypto": crypto,
        "ids": crypto,
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    try:
        response = requests.get(url_2, params=params)
        if response.status_code == 429:
            print("Crypto rate limited — sleeping 30s")
            time.sleep(30)
            return None
        response.raise_for_status()
        data = response.json()
        data = data[crypto]  # removes "bitcoin" wrapper
        data["crypto"] = crypto
        return data
    except Exception as i:
        print(f"Error fetching{crypto}: {i}")
        return None


while True:
    for symbol in SYMBOLS:
        quote = fetch_quotes(symbol)  # eg. fetch_quote(AAPL)
        if quote:
            print(f"Producing: {quote}")
            producer.send("stocks-quotes", value=quote)
    time.sleep(6)

    for crypto in CRYPTO_SYMBOLS:
        coin_price = fetch_symbols(crypto)

        if coin_price:
            print(f"Extracting: {coin_price}")
            producer.send("crypto-quotes", value=coin_price)
        else:
            print(f"Skipping {crypto} (no data)")
    time.sleep(6)
