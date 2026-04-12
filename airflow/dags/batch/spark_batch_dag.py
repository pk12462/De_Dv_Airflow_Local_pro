from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.spark_batch_operator import SparkBatchOperator


default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="spark_batch_dag",
    description="Daily Spark batch ETL",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "spark"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_batch = SparkBatchOperator(
        task_id="run_spark_batch",
        config_path="pipelines/configs/batch_pipeline_config.yaml",
    )

    end = EmptyOperator(task_id="end")

    start >> run_batch >> end
