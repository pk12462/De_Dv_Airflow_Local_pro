from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from operators.file_watcher_operator import FileWatcherOperator
from operators.holiday_calendar_operator import HolidayCalendarOperator
from operators.pipeline_config_utils import normalize_pipeline_config
from operators.pipeline_runner_operator import PipelineRunnerOperator
from operators.target_load_check_operator import TargetLoadCheckOperator

CONFIG_DIR = Path("/opt/airflow/etl-project/appconfig/dev")
CONFIG_PATTERN = "pipeline_*_pg_cassandra.json"
HOLIDAYS_FILE = "/opt/airflow/plugins/config/public_holidays.json"

DEFAULT_ARGS = {
    "owner": "learning-user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def _schedule_for_mode(mode: str) -> str:
    if mode == "streaming":
        return "*/15 * * * *"
    return "@daily"


def _load_pipeline_name(config_path: Path) -> tuple[str, str]:
    with config_path.open("r", encoding="utf-8-sig") as f:
        payload = normalize_pipeline_config(json.load(f))
    pipeline_name = str(payload.get("pipelineName", config_path.stem)).replace("-", "_")
    mode = str(payload.get("mode", "batch")).lower()
    return pipeline_name, mode


def _build_dag(config_path: Path, pipeline_name: str, mode: str) -> DAG:
    dag_id = f"{pipeline_name}_dag"

    with DAG(
        dag_id=dag_id,
        description=f"Auto DAG for {pipeline_name} ({mode})",
        start_date=datetime(2026, 1, 1),
        schedule=_schedule_for_mode(mode),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["learning", mode, "postgres", "cassandra"],
    ) as dag:
        holiday_check = HolidayCalendarOperator(
            task_id="holiday_check",
            holidays_file=HOLIDAYS_FILE,
            calendar_key="IN",
        )

        file_watch = FileWatcherOperator(
            task_id="file_watch_source",
            config_path=str(config_path),
            timeout_sec=0,
            poke_interval_sec=15,
        )

        run_pipeline = PipelineRunnerOperator(
            task_id="run_pipeline",
            config_path=str(config_path),
            dry_run=False,
        )

        validate_target_load = TargetLoadCheckOperator(
            task_id="validate_target_load",
            config_path=str(config_path),
            min_rows=1,
        )

        holiday_check >> file_watch >> run_pipeline >> validate_target_load

    return dag


for _cfg in sorted(CONFIG_DIR.glob(CONFIG_PATTERN)):
    _pipeline_name, _mode = _load_pipeline_name(_cfg)
    _dag_id = f"{_pipeline_name}_dag"
    globals()[_dag_id] = _build_dag(_cfg, _pipeline_name, _mode)

