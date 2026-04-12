from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

ETL_BASE_DIR = "/opt/airflow/etl-project"
RUNNER = "/opt/airflow/etl-project/spark-job/pyspark.py"


def _validate_configs(**context):
    """Validate all environment configs before running the pipeline."""
    import sys, os
    sys.path.insert(0, f"{ETL_BASE_DIR}/spark-job")
    from schema_validator import validate_all_envs
    from pathlib import Path
    validate_all_envs(Path(ETL_BASE_DIR), environments=[context["params"]["env"]])


with DAG(
    dag_id="cust_risk_triggers_etl",
    description="Config-driven UDAIP batch ETL: Synapse → Transform → REST API",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    params={"env": "dev"},
    tags=["etl", "batch", "cust-risk", "udaip"],
) as dag:

    start = EmptyOperator(task_id="start")

    validate = PythonOperator(
        task_id="validate_pipeline_configs",
        python_callable=_validate_configs,
    )

    run_dev = BashOperator(
        task_id="run_cust_risk_batch_dev",
        bash_command=(
            f"python {RUNNER} "
            "--env dev "
            f"--base-dir {ETL_BASE_DIR} "
            "--run-date {{ ds }}"
        ),
    )

    run_it = BashOperator(
        task_id="run_cust_risk_batch_it",
        bash_command=(
            f"python {RUNNER} "
            "--env it "
            f"--base-dir {ETL_BASE_DIR} "
            "--run-date {{ ds }}"
        ),
    )

    run_uat = BashOperator(
        task_id="run_cust_risk_batch_uat",
        bash_command=(
            f"python {RUNNER} "
            "--env uat "
            f"--base-dir {ETL_BASE_DIR} "
            "--run-date {{ ds }}"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> validate >> run_dev >> run_it >> run_uat >> end

