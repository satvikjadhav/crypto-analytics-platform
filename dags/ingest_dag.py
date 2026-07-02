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
    start_date=datetime(2024,1,1),
    catchup=False,
    default_args=default_args,
    tags=['crypto', 'ingestion']
)

def ingest_dag():
    ...