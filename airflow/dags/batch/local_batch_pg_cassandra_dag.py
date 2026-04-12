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

with DAG(
    dag_id="local_batch_pg_cassandra_dag",
    description="Batch pipeline: local CSV source -> PostgreSQL + Cassandra",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["learning", "batch", "postgres", "cassandra"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_batch = BashOperator(
        task_id="run_local_batch_pipeline",
        bash_command=(
            "python /opt/airflow/etl-project/spark-job/local_batch_streaming_runner.py "
            "--config /opt/airflow/etl-project/appconfig/dev/pipeline_batch_local_pg_cassandra.json "
            "--dry-run"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> run_batch >> end

