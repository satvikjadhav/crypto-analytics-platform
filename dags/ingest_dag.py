from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "satvik",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


@dag(
    dag_id="crypto_ingestion",
    schedule="@once",
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

    submit_spark = SSHOperator(
        task_id="spark_streaming_ingest",
        ssh_conn_id="spark_ec2_ssh",
        command="""
            # Load env vars from the file on the host, then pass them into docker exec
            set -a && source /opt/spark/.env && set +a

            docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
            --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS} \
            --conf spark.executorEnv.S3_BUCKET=${S3_BUCKET} \
            --conf spark.executorEnv.SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} \
            --conf spark.executorEnv.AWS_REGION=${AWS_REGION} \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-avro_2.12:3.4.0 \
            /opt/spark/jobs/streaming_ingest.py
        """,
        cmd_timeout=300,
        conn_timeout=30,
    )

    fetch_market_meta >> submit_spark


ingest_dag()
