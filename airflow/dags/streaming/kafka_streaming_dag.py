from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.kafka_operator import KafkaOperator


default_args = {
    "owner": "de-dv-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kafka_streaming_dag",
    description="Run Kafka consumer/producer streaming app",
    start_date=datetime(2026, 1, 1),
    schedule="@continuous",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["streaming", "kafka"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_consumer = KafkaOperator(
        task_id="run_kafka_consumer",
        mode="consumer",
        config_path="pipelines/configs/streaming_pipeline_config.yaml",
        timeout_seconds=300,
    )

    run_producer = KafkaOperator(
        task_id="run_kafka_producer",
        mode="producer",
        config_path="pipelines/configs/streaming_pipeline_config.yaml",
        timeout_seconds=300,
    )

    end = EmptyOperator(task_id="end")

    start >> run_consumer >> run_producer >> end
