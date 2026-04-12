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

with DAG(
    dag_id="local_streaming_pg_cassandra_dag",
    description="Streaming pipeline: local JSONL source -> PostgreSQL + Cassandra",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["learning", "streaming", "postgres", "cassandra"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_streaming = BashOperator(
        task_id="run_local_streaming_pipeline",
        bash_command=(
            "python /opt/airflow/etl-project/spark-job/local_batch_streaming_runner.py "
            "--config /opt/airflow/etl-project/appconfig/dev/pipeline_streaming_local_pg_cassandra.json "
            "--dry-run"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> run_streaming >> end

