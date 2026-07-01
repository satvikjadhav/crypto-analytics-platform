import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    "io.delta:delta-core_2.12:2.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

trade_schema = StructType([
    StructField("symbol",         StringType(),  False),
    StructField("price",          DoubleType(),  False),
    StructField("quantity",       DoubleType(),  False),
    StructField("trade_time",     LongType(),    False),
    StructField("is_buyer_maker", BooleanType(), False),
    StructField("ingestion_ts",   LongType(),    False),
])

spark = (
    SparkSession.builder
    .appName("CryptoTradesIngest")
    .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # IAM instance profile — NO hardcoded access/secret keys
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# On EC2 this resolves to: 10.0.1.45:9092

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

parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("trade_timestamp", (F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("date", F.to_date("trade_timestamp"))
)

good = parsed.filter(F.col("symbol").isNotNull())
bad = parsed.filter(F.col("symbol").isNull())