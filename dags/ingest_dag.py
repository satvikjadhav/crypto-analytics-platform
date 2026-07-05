from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner":            "satvik",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

@dag(
    dag_id="crypto_ingestion",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "ingestion"],
)
def ingest_dag():
    fetch_market_meta = BashOperator(
        task_id="fetch_coingecko",
        bash_command=(
            "timeout 295 python3 "
            "/opt/airflow/repo/producers/coingecko_producer.py "
            "|| true"
        ),
        # Inherits KAFKA_BOOTSTRAP_SERVERS from Airflow container env
    )

    submit_spark = SparkSubmitOperator(
        task_id="spark_streaming_ingest",
        application="/opt/spark/jobs/streaming_ingest.py",
        conn_id="spark_ec2",   # points to Spark EC2 private IP — set up in Task 2
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.spark:spark-avro_2.12:3.4.0"
        ),
        conf={
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.jars.ivy": "/tmp/.ivy2",
        },
        execution_timeout=timedelta(minutes=25),
    )

    fetch_market_meta >> submit_spark

ingest_dag()
