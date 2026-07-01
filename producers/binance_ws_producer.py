import websocket
import json
import logging
import os
import time
import signal
import sys
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv("/opt/producers/.env")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

STREAM_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@trade/ethusdt@trade/bnbusdt@trade"
    "/solusdt@trade/adausdt@trade"
)

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 500,
})


def on_message(ws, message):
    try:
        wrapper = json.loads(message)
        data = wrapper.get("data", {})
        if data.get("e") != "trade":
            return
        event = {
            "symbol":         data["s"],
            "price":          float(data["p"]),
            "quantity":       float(data["q"]),
            "trade_time":     int(data["T"]),
            "is_buyer_maker": bool(data["m"]),
            "ingestion_ts":   int(time.time() * 1000),
        }
        producer.produce(
            topic="crypto.trades",
            key=event["symbol"].encode(),
            value=json.dumps(event).encode(),
            on_delivery=delivery_report,
        )
        producer.poll(0)
    except (KeyError, ValueError) as exc:
        log.warning("Parse error: %s", exc)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed [%s]: %s", msg.topic(), err)


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
            STREAM_URL,
            on_open=lambda ws: log.info("Connected"),
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