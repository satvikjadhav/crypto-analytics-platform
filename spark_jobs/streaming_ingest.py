import os
import json
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    "io.delta:delta-core_2.12:2.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.apache.spark:spark-avro_2.12:3.4.0",
])

BUCKET              = os.getenv("S3_BUCKET")
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SUBJECT             = "crypto.trades-value"

# ── Fetch Avro schema from Confluent Schema Registry ─────────────────────────
url = f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest"
with urllib.request.urlopen(url) as resp:
    schema_json = json.loads(resp.read().decode())["schema"]
print(f"[INFO] Fetched schema for subject '{SUBJECT}':\n{schema_json}\n")
# ─────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("CryptoTradesIngest")
    .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "crypto.trades")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 50_000)
    .load()
)

# ── Strip 5-byte Confluent header, deserialize Avro, derive timestamp cols ───
parsed = (
    raw_stream
    .select(
        from_avro(
            F.expr("substring(value, 6, length(value) - 5)"),
            schema_json,
        ).alias("d")
    )
    .select("d.*")
    .withColumn("trade_timestamp", (F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("date", F.to_date("trade_timestamp"))
)

good = parsed.filter(F.col("symbol").isNotNull())
bad  = parsed.filter(F.col("symbol").isNull())

# ── Delta Lake sink ───────────────────────────────────────────────────────────
delta_query = (
    good
    .writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy("date", "symbol")
    .option("checkpointLocation", f"s3a://{BUCKET}/checkpoints/trades/")
    .trigger(processingTime="30 seconds")
    .start(f"s3a://{BUCKET}/curated/delta/trades/")
)

# Dead-letter queue sink (optional) — uncomment to enable

# dlq_query = (
#     bad
#     .select(
#         F.col("raw_value").alias("value"),          # ← original bytes, re-publishable
#         F.lit("parse_failure").alias("reason"),
#         F.current_timestamp().alias("dlq_ts"),
#     )
#     .writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)  # ← use the resolved var, not re-calling getenv
#     .option("topic", "crypto.trades.dlq")
#     .option("checkpointLocation", f"s3a://{BUCKET}/checkpoints/trades-dlq/")
#     .trigger(processingTime="30 seconds")
#     .start()
# )


delta_query.awaitTermination(180)