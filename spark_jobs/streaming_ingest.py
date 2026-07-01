import os
from pyspark.sql import SparkSession

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    "io.delta:delta-core_2.12:2.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
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