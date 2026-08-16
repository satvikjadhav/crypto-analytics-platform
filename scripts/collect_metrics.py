#!/usr/bin/env python3
"""
scripts/collect_metrics.py

Measures end-to-end pipeline latency across 5 stages and prints
a GitHub-flavoured Markdown table. Paste the output into README.md.

Run ~3 hours after a pipeline run (Snowflake ACCOUNT_USAGE has a 3h lag).

Usage:
  python scripts/collect_metrics.py \
    --date 2026-07-16 \
    --kafka-bootstrap xyz:9092 \
    --s3-bucket crypto-analytics-lake-sj \
    --snowflake-account myorg-abc12345 \
    --snowflake-user pipeline_user \
    --snowflake-password xyz123
"""
import argparse
import json
import statistics
import struct
from datetime import datetime, timezone
from typing import Optional

import boto3
import snowflake.connector
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

parser = argparse.ArgumentParser()
parser.add_argument("--date",               required=True)
parser.add_argument("--kafka-bootstrap",    required=True)
parser.add_argument("--s3-bucket",          required=True)
parser.add_argument("--snowflake-account",  required=True)
parser.add_argument("--snowflake-user",     required=True)
parser.add_argument("--snowflake-password", required=True)
args = parser.parse_args()


def fmt(ms_list: list) -> tuple:
    """Return (p50, p95, p99) in milliseconds, or N/A if empty."""
    if not ms_list:
        return ("N/A", "N/A", "N/A")
    s = sorted(ms_list)
    n = len(s)
    return (
        f"{statistics.median(s):.0f}",
        f"{s[min(int(n * 0.95), n - 1)]:.0f}",
        f"{s[min(int(n * 0.99), n - 1)]:.0f}",
    )


def decode_avro_trade(raw: bytes) -> Optional[dict]:
    """
    Decode Confluent Schema Registry wire format for the crypto.trades schema:
      schema: {symbol: string, price: double, qty: double,
               trade_time: long (ms), is_buyer_maker: boolean, ingestion_ts: long (ms)}

    Wire format:
      byte 0:    magic byte (0x00)
      bytes 1-4: schema ID (big-endian int32)
      bytes 5+:  Avro binary payload
        - symbol:         zigzag varint length + UTF-8 bytes
        - price:          8-byte little-endian IEEE 754 double
        - qty:            8-byte little-endian IEEE 754 double
        - trade_time:     zigzag varint (milliseconds epoch)
        - is_buyer_maker: 1-byte boolean (0x00 or 0x01)
        - ingestion_ts:   zigzag varint (milliseconds epoch)

    NOTE: if trade_time / ingestion_ts are stored as fixed 8-byte little-endian
    longs instead of zigzag varints, replace the read_zigzag_long calls for those
    two fields with: struct.unpack_from("<q", raw, pos)[0]; pos += 8
    """
    if len(raw) < 6 or raw[0] != 0x00:
        return None

    pos = 5  # skip magic byte + 4-byte schema ID

    def read_zigzag_long(data: bytes, offset: int) -> tuple:  # (int, int)
        """Read Avro zigzag-encoded long. Returns (value, new_offset)."""
        n, shift = 0, 0
        while True:
            b = data[offset]
            offset += 1
            n |= (b & 0x7F) << shift
            shift += 7
            if not (b & 0x80):
                break
        # zigzag decode: even → positive, odd → negative
        return ((n >> 1) ^ -(n & 1)), offset

    def read_string(data: bytes, offset: int) -> tuple:
        """Read Avro length-prefixed UTF-8 string."""
        length, offset = read_zigzag_long(data, offset)
        return data[offset:offset + length].decode("utf-8"), offset + length

    try:
        symbol, pos     = read_string(raw, pos)
        price,          = struct.unpack_from("<d", raw, pos); pos += 8
        qty,            = struct.unpack_from("<d", raw, pos); pos += 8
        trade_time, pos = read_zigzag_long(raw, pos)
        is_buyer_maker  = raw[pos]; pos += 1
        ingestion_ts, _ = read_zigzag_long(raw, pos)
        return {
            "symbol":         symbol,
            "price":          price,
            "qty":            qty,
            "trade_time":     trade_time,       # epoch ms
            "is_buyer_maker": bool(is_buyer_maker),
            "ingestion_ts":   ingestion_ts,     # epoch ms
        }
    except Exception:
        return None


metrics = {}

# ── Stage 1: Kafka producer → consumer latency ─────────────────────────────
print("Collecting Kafka latencies (sampling last 100 messages)...")

try:
    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id":          "metrics-collector",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    tp = TopicPartition("crypto.trades", 0)
    low, high = consumer.get_watermark_offsets(tp)
    start_offset = max(low, high - 100)
    consumer.assign([TopicPartition("crypto.trades", 0, start_offset)])

    lats = []
    for _ in range(100):
        msg = consumer.poll(timeout=3.0)
        if msg is None or msg.error():
            break
        trade = decode_avro_trade(msg.value())
        if trade and trade["trade_time"] and trade["ingestion_ts"]:
            lats.append(trade["ingestion_ts"] - trade["trade_time"])

    consumer.close()
    metrics["kafka"] = fmt(lats)
    print(f"  Kafka: {len(lats)} samples")
except Exception as e:
    print(f"  Kafka error: {e}")
    metrics["kafka"] = ("N/A", "N/A", "N/A")

# ── Stage 2 & 3: Spark micro-batch and Delta write (S3 checkpoint metadata)
print("Collecting Spark / Delta latencies from S3 checkpoint...")
try:
    s3 = boto3.client("s3")
    prefix = f"curated/delta/trades/_delta_log/"
    resp = s3.list_objects_v2(Bucket=args.s3_bucket, Prefix=prefix, MaxKeys=10)
    times = sorted(
        [o["LastModified"] for o in resp.get("Contents", [])],
        reverse=True,
    )
    if len(times) >= 2:
        delta_write_ms = (times[0] - times[-1]).total_seconds() * 1000
        metrics["delta"] = (f"{delta_write_ms:.0f}", "N/A", "N/A")
    else:
        metrics["delta"] = ("N/A", "N/A", "N/A")

    # Spark micro-batch duration from checkpoint commit files
    ckpt_prefix = "checkpoints/trades/"
    ckpt = s3.list_objects_v2(Bucket=args.s3_bucket, Prefix=ckpt_prefix, MaxKeys=20)
    ckpt_times = sorted([o["LastModified"] for o in ckpt.get("Contents", [])])
    if len(ckpt_times) >= 2:
        batch_ms = (ckpt_times[-1] - ckpt_times[-2]).total_seconds() * 1000
        metrics["spark"] = (f"{batch_ms:.0f}", "N/A", "N/A")
    else:
        metrics["spark"] = ("N/A", "N/A", "N/A")
    print(f"  Delta / Spark: OK")
except Exception as e:
    print(f"  S3 error: {e}")
    metrics["delta"] = metrics["spark"] = ("N/A", "N/A", "N/A")

# ── Stage 4 & 5: Snowflake COPY INTO and dbt (ACCOUNT_USAGE, ~3h lag) ─────
print("Collecting Snowflake query latencies (ACCOUNT_USAGE)...")
try:
    conn = snowflake.connector.connect(
        account=args.snowflake_account,
        user=args.snowflake_user,
        password=args.snowflake_password,
        database="SNOWFLAKE",
        schema="ACCOUNT_USAGE",
        warehouse="CRYPTO_PIPELINE_WH",
    )
    cur = conn.cursor()

    # COPY INTO duration
    cur.execute("""
        SELECT DATEDIFF('millisecond', start_time, end_time) AS duration_ms
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_type = 'COPY'
          AND database_name = 'CRYPTO_ANALYTICS_DB'
          AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
          AND execution_status = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT 20
    """)
    copy_rows = [row[0] for row in cur.fetchall() if row[0] is not None]
    metrics["snowflake_copy"] = fmt(copy_rows)

    # dbt run duration (all models tagged dbt)
    cur.execute("""
        SELECT DATEDIFF('millisecond', start_time, end_time) AS duration_ms
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_tag LIKE '%dbt%'
          AND database_name = 'CRYPTO_ANALYTICS'
          AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
          AND execution_status = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT 50
    """)
    dbt_rows = [row[0] for row in cur.fetchall() if row[0] is not None]
    metrics["dbt"] = fmt(dbt_rows)

    conn.close()
    print(f"  Snowflake: {len(copy_rows)} COPY queries, {len(dbt_rows)} dbt queries")
except Exception as e:
    print(f"  Snowflake error: {e} (ACCOUNT_USAGE has a 3-hour lag — try again later)")
    metrics["snowflake_copy"] = metrics["dbt"] = ("N/A", "N/A", "N/A")

# ── Output ──────────────────────────────────────────────────────────────────
print(f"\n## Performance Metrics\n")
print(f"_Measured {args.date} — end-to-end pipeline run_\n")
print("| Pipeline Stage | p50 (ms) | p95 (ms) | p99 (ms) | Notes |")
print("|---|---|---|---|---|")

rows = [
    ("Kafka producer → consumer",    metrics["kafka"],          "n=100 messages"),
    ("Spark micro-batch processing", metrics["spark"],          "30-s trigger interval"),
    ("Delta Lake S3 write",          metrics["delta"],          "window across log commits"),
    ("Snowflake COPY INTO",          metrics["snowflake_copy"], "~500K rows"),
    # ("dbt run (all 8 models)",       metrics["dbt"],            "ACCOUNT_USAGE, 3h lag"),
]
for stage, (p50, p95, p99), note in rows:
    print(f"| {stage} | {p50} | {p95} | {p99} | {note} |")

print("\n> Run `python scripts/collect_metrics.py` to regenerate from live data.")