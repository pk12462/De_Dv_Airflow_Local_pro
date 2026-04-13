from __future__ import annotations

import json
from pathlib import Path
from typing import Any

AIRFLOW_ROOT = Path("/opt/airflow")


def load_pipeline_config(config_path: str) -> dict[str, Any]:
    path = resolve_workspace_path(config_path)
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def resolve_workspace_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return AIRFLOW_ROOT / path


def resolve_source_path(config: dict[str, Any]) -> Path:
    source = config.get("source", {})
    source_path = source.get("path")
    if not source_path:
        raise ValueError("source.path is required in pipeline config")
    return resolve_workspace_path(str(source_path))

