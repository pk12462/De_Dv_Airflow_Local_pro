from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.pandas_batch_operator import PandasBatchOperator


default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pandas_batch_dag",
    description="Daily Pandas batch ETL",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "pandas"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_pandas = PandasBatchOperator(
        task_id="run_pandas_batch",
        config_path="pipelines/configs/batch_pipeline_config.yaml",
    )

    end = EmptyOperator(task_id="end")

    start >> run_pandas >> end
