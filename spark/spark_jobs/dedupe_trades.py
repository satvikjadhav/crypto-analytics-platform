import os
from pyspark.sql import SparkSession

BUCKET = os.getenv("S3_BUCKET")
TRADES_PATH = f"s3a://{BUCKET}/curated/delta/trades/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/trades/"


spark = (
    SparkSession.builder.appName("DedupeTradesDelta")
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

print(f"[INFO] Reading Delta table from {TRADES_PATH}")
df = spark.read.format("delta").load(TRADES_PATH)

total = df.count()
print(f"[INFO] Total rows before dedup: {total}")

deduped = df.dropDuplicates(['symbol', 'price', 'quantity', 'trade_time', 'is_buyer_maker', 'ingestion_ts', 'trade_timestamp', 'date'])

deduped_count = deduped.count()
print(f"[INFO] Total rows after dedup:  {deduped_count}")
print(f"[INFO] Duplicates removed:      {total - deduped_count}")

print("[INFO] Overwriting Delta table with deduped data...")
(
    deduped.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("date", "symbol")
    .save(TRADES_PATH)
)

print("[INFO] Dedup complete. Running VACUUM to clean up old files...")
spark.sql(f"VACUUM delta.`{TRADES_PATH}` RETAIN 0 HOURS")

print("[INFO] Done.")
spark.stop()

