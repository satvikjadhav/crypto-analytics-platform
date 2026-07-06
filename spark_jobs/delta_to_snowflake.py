"""
Delta Lake → Snowflake EL job.
Reads today's partition from the trades Delta table and appends to raw.trades.
Overwrites raw.market_meta with the latest CoinGecko snapshot.
Triggered daily by the load_to_snowflake Airflow task.
"""

import os
import json
import boto3
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Snowflake credentials from secrets manager
sm = boto3.client("secretsmanager", region_name = os.getenv("AWS_REGION", "us-est-1"))
secret = json.loads(
    sm.get_secret_value(SecretId="crypto-analytics/snowflake/pipeline")["SecretString"]
)

BUCKET = os.getenv("S3_BUCKET")
TODAY = str(date.today()) # 2026-07-05

# Snowflake connection options
SF_OPTIONS = {
    "sfURL":            f"{secret['account']}.snowflakecomputing.com",
    "sfUser":           secret["username"],
    "sfPassword":       secret["password"],
    "sfDatabase":       secret["database"],
    "sfWarehouse":      secret["warehouse"],
    "sfRole":           secret["role"],
    "sfSchema":         "RAW",
    # Temp S3 staging area — uses the CRYPTO_S3_INTEGRATION
    "tempdir":          f"s3a://{BUCKET}/tmp/snowflake-stage/",
    "keep_column_case": "off",
}