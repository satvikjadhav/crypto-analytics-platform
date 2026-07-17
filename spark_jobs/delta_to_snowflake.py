import os
import json
import boto3
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import snowflake.connector

# Snowflake credentials from Secrets Manager
sm = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
secret = json.loads(
    sm.get_secret_value(SecretId="crypto-analytics/snowflake/pipeline")["SecretString"]
)
sts = boto3.client("sts", region_name=os.getenv("AWS_REGION", "us-east-1"))
account_id = sts.get_caller_identity()["Account"]
role_name   = os.getenv("SNOWFLAKE_S3_ROLE_NAME", "crypto-snowflake-s3-role")

BUCKET = os.getenv("S3_BUCKET")
TODAY  = str(date.today())         # 2026-07-05

# Snowflake connection optoins
SF_OPTIONS = {
    "sfURL":            f"{secret['account']}.snowflakecomputing.com",
    "sfUser":           secret["username"],
    "sfPassword":       secret["password"],
    "sfDatabase":       secret["database"],
    "sfWarehouse":      secret["warehouse"],
    "sfRole":           secret["role"],
    "sfSchema":         "RAW",
    # Temp S3 staging area — uses the CRYPTO_S3_INTEGRATION from Week 1 Terraform
    "tempdir":          f"s3://{BUCKET}/tmp/snowflake-stage/",
    "keep_column_case": "off",
    "s3_staging_dir_encryption": "false",
    "extra_copy_options": f"CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::{account_id}:role/{role_name}')",
}

spark = (
    SparkSession.builder
    .appName("DeltaToSnowflake")
    # registers Delta Lake's SQL extensions so Spark understands Delta-specific SQL syntax
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # replaces Spark's default catalog with Delta's, so it can resolve Delta table metadata, schema, and versioning
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # IAM instance profile — no hardcoded AWS keys
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

try:
    # delete today's partition first
    conn = snowflake.connector.connect(
        account=secret["account"],
        user=secret["username"],
        password=secret["password"],
        database=secret["database"],
        warehouse=secret["warehouse"],
        role=secret["role"],
    )
    cur = conn.cursor()
    cur.execute(f"DELETE FROM RAW.TRADES WHERE DATE(TRADE_TIME) = '{TODAY}'")
    
    # Load trades: today's partition only
    TRADES_COLS = [
        "symbol",
        "price",
        "quantity",
        "trade_time",
        "is_buyer_maker",
        "ingestion_ts",
        "date",
    ]
    
    # Load today's partition
    trades_df = (
        spark.read.format("delta")
        .load(f"s3a://{BUCKET}/curated/delta/trades/")
        .filter(F.col("date") == F.lit(TODAY))
        .withColumn("trade_time", (F.col("trade_time") / 1000).cast("timestamp"))
        .withColumn("ingestion_ts", (F.col("ingestion_ts") / 1000).cast("timestamp"))
        .select(*TRADES_COLS)
    )

    # cache trades_df
    trades_df.cache()

    # materialize
    count = trades_df.count()

    # write today's partition to snowflake
    (
        trades_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SF_OPTIONS)
        .option("dbtable", "RAW.TRADES")
        .mode("append")
        .save()
    )

    print(f"[INFO] Loaded {count} trade rows for {TODAY} into RAW.TRADES")

    trades_df.unpersist()

    # Load market_meta: latest snapshot per coin
    # market_meta goes to s3 from the coingecko producer

    MARKET_META_COLS = [
        "coin_id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "market_cap_rank",
        "total_volume",
        "price_change_24h",
        "price_change_pct_24h",
        "circulating_supply",
        "ath",
        "ingestion_ts",
    ]

    market_raw = (
        spark.read.format("delta")
        .load(f"s3a://{BUCKET}/curated/delta/market_meta/")
    )

    window = Window.partitionBy("coin_id").orderBy(F.col("ingestion_ts").desc())

    market_df = (
        market_raw
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(*MARKET_META_COLS)
        .withColumn("ingestion_ts", (F.col("ingestion_ts") / 1000).cast("timestamp"))
    )

    # cache market_df
    market_df.cache()

    count = market_df.count()

    (
        market_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SF_OPTIONS)
        .option("dbtable", "RAW.MARKET_META")
        .mode("overwrite")
        .save()
    )

    print(f"[INFO] Loaded {count} market_meta rows into RAW.MARKET_META")

    market_df.unpersist()

except Exception as e:
    print(f"[Error] Job Failed: {e}")
    raise
finally:
    spark.stop()
