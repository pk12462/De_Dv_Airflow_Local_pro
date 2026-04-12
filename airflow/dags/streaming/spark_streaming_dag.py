from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.spark_streaming_operator import SparkStreamingOperator


default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_streaming_dag",
    description="Run Spark Structured Streaming pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["streaming", "spark"],
) as dag:
    start = EmptyOperator(task_id="start")

    submit_stream = SparkStreamingOperator(
        task_id="submit_spark_streaming_job",
        config_path="pipelines/configs/streaming_pipeline_config.yaml",
    )

    end = EmptyOperator(task_id="end")

    start >> submit_stream >> end
