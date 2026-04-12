from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "learning-user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

BASE = "/opt/airflow/etl-project"
RUNNER = f"{BASE}/spark-job/local_batch_streaming_runner.py"

with DAG(
    dag_id="bank_cards_streaming_dag",
    description="Streaming pipeline from local bank_data.json to PostgreSQL and Cassandra",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["streaming", "bank_cards", "postgres", "cassandra"],
) as dag:
    start = EmptyOperator(task_id="start")

    bank_cards_stream = BashOperator(
        task_id="bank_cards_stream_to_pg_cassandra",
        bash_command=(
            f"python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_bank_cards_streaming_json_pg_cassandra.json "
            "--dry-run"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> bank_cards_stream >> end

