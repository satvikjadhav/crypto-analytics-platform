import requests
import time
import logging
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv("/opt/producers/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_ENDPOINT = "https://api.coingecko.com/api/v3/coins/markets"
HEADERS = {"x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY", "")}

producer = Producer(
    {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    }
)


def delivery_report(err, msg):
    if err:
        logging.error("Delivery failed: %s", err)


def fetch_all_coins():
    all_coins = []
    page = 1

    while True:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
        }
        response = requests.get(BASE_ENDPOINT, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_coins.extend(batch)
        logging.info("Page %d: fetched %d coins (total: %d)", page, len(batch), len(all_coins))
        page += 1
        time.sleep(1.5)  # ~30 req/min free tier limit

    return all_coins


coins = fetch_all_coins()
ts = int(time.time() * 1000)

for coin in coins:
    record = {
        "coin_id": coin["id"],
        "symbol": coin["symbol"].upper(),
        "name": coin["name"],
        "current_price": float(coin["current_price"] or 0),
        "market_cap": int(coin["market_cap"] or 0),
        "market_cap_rank": int(coin["market_cap_rank"] or 0),
        "total_volume": float(coin["total_volume"] or 0),
        "price_change_24h": coin["price_change_24h"],
        "price_change_pct_24h": coin["price_change_percentage_24h"],
        "circulating_supply": coin["circulating_supply"],
        "ath": float(coin["ath"] or 0),
        "ingestion_ts": ts,
    }
    producer.produce(
        "crypto.market_meta",
        key=record["symbol"].encode(),
        value=json.dumps(record).encode(),
        callback=delivery_report,
    )

producer.flush()
logging.info("Flushed %d records to Kafka", len(coins))
