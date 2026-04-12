from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "learning-user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BASE = "/opt/airflow/etl-project"
RUNNER = f"{BASE}/spark-job/local_batch_streaming_runner.py"

with DAG(
    dag_id="bank_cards_and_trips_batch_dag",
    description="Batch pipelines from local bank/trip files to PostgreSQL and Cassandra",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "bank_cards", "trips", "postgres", "cassandra"],
) as dag:
    start = EmptyOperator(task_id="start")

    bank_cards_csv = BashOperator(
        task_id="bank_cards_csv_to_pg_cassandra",
        bash_command=(
            f"python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_bank_cards_batch_csv_pg_cassandra.json "
            "--dry-run"
        ),
    )

    bank_cards_pipe = BashOperator(
        task_id="bank_cards_pipe_to_pg_cassandra",
        bash_command=(
            f"python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_bank_cards_pipe_batch_pg_cassandra.json "
            "--dry-run"
        ),
    )

    city_trips = BashOperator(
        task_id="city_trips_batch_to_pg_cassandra",
        bash_command=(
            f"python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_city_trips_batch_pg_cassandra.json "
            "--dry-run"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> bank_cards_csv >> bank_cards_pipe >> city_trips >> end

