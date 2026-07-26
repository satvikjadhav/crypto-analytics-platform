import os
import logging
from dotenv import load_dotenv
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from delta.tables import DeltaTable


# docker exec spark-master /opt/spark/bin/spark-submit \
#    --master spark://spark-master:7077 \
#    --conf spark.jars.ivy=/tmp/.ivy2 \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#    /opt/spark/jobs/market_meta_ingest.py

# config
load_dotenv("/opt/spark/.env")
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
 
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC     = "crypto.market_meta"
 
BUCKET     = os.getenv("S3_BUCKET", "s3_bucket_name")
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
def build_spark() -> SparkSession:

    PACKAGES = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        "io.delta:delta-core_2.12:2.4.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    return (
        SparkSession.builder.appName("market_meta_delta_writer")
        # .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# read the latest batch form kafka
def read_from_kafka(spark: SparkSession):
    log.info("Reading from Kafka topic: %s", KAFKA_TOPIC)
 
    raw_df = (
        spark.read                          # batch read, not streaming
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # full topic each run
        .option("endingOffsets",   "latest")    # up to current end
        .option("failOnDataLoss",  "false")
        .load()
    )
 
    # Deserialise JSON value; key is the symbol (bytes) but we get it
    # from the payload so we don't need to decode it separately.
    parsed_df = (
        raw_df
        .select(
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(
                F.col("value").cast("string"),
                PAYLOAD_SCHEMA,
            ).alias("data")
        )
        .select("offset", "kafka_ts", "data.*")
    )
 
    # Keep only the latest message per symbol (highest offset wins)
    # This is the key dedup step: the producer may have emitted the
    # same coin multiple times across runs.
    window = (
        F.window_rank()                         # PySpark 3.4+
        if hasattr(F, "window_rank")
        else None
    )
 
    from pyspark.sql.window import Window
    w = Window.partitionBy("symbol").orderBy(F.col("offset").desc())
 
    deduped_df = (
        parsed_df
        .withColumn("_rank", F.rank().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "offset", "kafka_ts")
    )
 
    count = deduped_df.count()
    log.info("Parsed and deduped %d distinct coins from Kafka.", count)
    return deduped_df

# Upsert (MERGE) into Delta table
def upsert_to_delta(spark: SparkSession, df) -> None:
    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        log.info("Delta table found -- running MERGE …")
        delta_tbl = DeltaTable.forPath(spark, DELTA_PATH)
 
        (
            delta_tbl.alias("tgt")
            .merge(
                df.alias("src"),
                "tgt.symbol = src.symbol",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("MERGE complete.")
 
    else:
        log.info("No Delta table yet -- creating at %s …", DELTA_PATH)
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(DELTA_PATH)
        )
        log.info("Initial Delta table created.")

# Entry Point
def main():
    spark = build_spark()
    try:
        df = read_from_kafka(spark)
        upsert_to_delta(spark, df)
    finally:
        spark.stop()
        log.info("SparkSession stopped.")
 
if __name__ == "__main__":
    main()
