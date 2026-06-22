from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "satvik",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="hello_world",
    description="Smoke test — confirms scheduler and executor are alive",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["infra", "smoke-test"],
) as dag:

    ping = BashOperator(
        task_id="pipeline_alive",
        bash_command='echo "Pipeline alive -- $(date)"',
    )
