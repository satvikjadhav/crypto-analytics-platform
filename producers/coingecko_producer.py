import requests
import time
import logging
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv("/opt/producers/.env")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")

ENDPOINT = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=50"
)
HEADERS = {"x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY", "")}

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    # e.g. 10.0.x.x:29092  ← Kafka EC2 private IP
})

def delivery_report(err, msg):
    if err:
        logging.error("Delivery failed: %s", err)

coins = requests.get(ENDPOINT, headers=HEADERS, timeout=10).json()
ts    = int(time.time() * 1000)

for coin in coins:
    record = {
        "coin_id":              coin["id"],
        "symbol":               coin["symbol"].upper(),
        "name":                 coin["name"],
        "current_price":        float(coin["current_price"] or 0),
        "market_cap":           int(coin["market_cap"] or 0),
        "market_cap_rank":      int(coin["market_cap_rank"] or 0),
        "total_volume":         float(coin["total_volume"] or 0),
        "price_change_24h":     coin["price_change_24h"],           # nullable
        "price_change_pct_24h": coin["price_change_percentage_24h"], # nullable
        "circulating_supply":   coin["circulating_supply"],          # nullable
        "ath":                  float(coin["ath"] or 0),
        "ingestion_ts":         ts,
    }
    producer.produce(
        "crypto.market_meta",
        key=record["symbol"].encode(),
        value=json.dumps(record).encode(),
        callback=delivery_report,
    )

producer.flush()
