import os
import logging
from dotenv import load_dotenv
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from delta.tables import DeltaTable

# config
load_dotenv("/opt/producers/.env")
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
 
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC     = "crypto.market_meta"
 
BUCKET     = os.getenv("BUCKET", "your-bucket-name")
DELTA_PATH = f"s3a://{BUCKET}/curated/delta/market_meta/"

# schema for market_meta
PAYLOAD_SCHEMA = StructType([
    StructField("coin_id",              StringType(), nullable=False),
    StructField("symbol",               StringType(), nullable=False),
    StructField("name",                 StringType(), nullable=True),
    StructField("current_price",        DoubleType(), nullable=True),
    StructField("market_cap",           LongType(),   nullable=True),
    StructField("market_cap_rank",      LongType(),   nullable=True),
    StructField("total_volume",         DoubleType(), nullable=True),
    StructField("price_change_24h",     DoubleType(), nullable=True),
    StructField("price_change_pct_24h", DoubleType(), nullable=True),
    StructField("circulating_supply",   DoubleType(), nullable=True),
    StructField("ath",                  DoubleType(), nullable=True),
    StructField("ingestion_ts",         LongType(),   nullable=False),
])

# spark session and log
