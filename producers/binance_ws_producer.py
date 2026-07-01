import websocket
import json
import logging
import os
import time
import signal
import sys
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


load_dotenv("/opt/producers/.env")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Coinbase Advanced Trade WebSocket — no geo-restrictions
WS_URL = "wss://advanced-trade-ws.coinbase.com"

PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "BNB-USD"]

SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": PRODUCTS,
    "channel": "market_trades",
})

# ── Schema Registry ──────────────────────────────────────────────────
schema_registry_client = SchemaRegistryClient({
    "url": os.getenv("SCHEMA_REGISTRY_URL")
})

with open("/opt/airflow/repo/schemas/trade_event.avsc") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda obj, ctx: obj,
)

# ── Kafka producer ───────────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 500,
})


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed [%s]: %s", msg.topic(), err)


def on_open(ws):
    log.info("Connected — subscribing to market_trades")
    ws.send(SUBSCRIBE_MSG)


def on_message(ws, message):
    try:
        wrapper = json.loads(message)

        if wrapper.get("channel") != "market_trades":
            return

        for trade in wrapper.get("events", []):
            for t in trade.get("trades", []):
                event = {
                    "symbol":         t["product_id"].replace("-", ""),
                    "price":          float(t["price"]),
                    "quantity":       float(t["size"]),
                    "trade_time":     int(time.time() * 1000),
                    "is_buyer_maker": t["side"] == "SELL",
                    "ingestion_ts":   int(time.time() * 1000),
                }

                value_bytes = avro_serializer(
                    event,
                    SerializationContext("crypto.trades", MessageField.VALUE),
                )

                producer.produce(
                    topic="crypto.trades",
                    key=event["symbol"].encode(),
                    value=value_bytes,
                    on_delivery=delivery_report,
                )
                producer.poll(0)

    except (KeyError, ValueError) as exc:
        log.warning("Parse error: %s", exc)


def shutdown(signum, frame):
    log.info("Shutting down — flushing producer...")
    producer.flush(timeout=10)
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    backoff = 1
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=lambda ws, e: log.error("WS error: %s", e),
            on_close=lambda ws, code, msg: log.warning("Closed (code=%s)", code),
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        log.warning("Reconnecting in %ds…", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()
