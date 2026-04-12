from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.sql_batch_operator import SqlBatchOperator


default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sql_batch_dag",
    description="Daily SQL ETL extract/transform/load",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "sql"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_sql = SqlBatchOperator(
        task_id="run_sql_batch",
        config_path="pipelines/configs/batch_pipeline_config.yaml",
    )

    end = EmptyOperator(task_id="end")

    start >> run_sql >> end
