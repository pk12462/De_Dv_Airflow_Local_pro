from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator

from operators.pipeline_config_utils import load_pipeline_config, resolve_source_path, resolve_workspace_path


class FileWatcherOperator(BaseOperator):
    template_fields = ("source_path", "config_path")

    def __init__(
        self,
        source_path: str | None = None,
        config_path: str | None = None,
        timeout_sec: int = 0,
        poke_interval_sec: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_path = source_path
        self.config_path = config_path
        self.timeout_sec = timeout_sec
        self.poke_interval_sec = poke_interval_sec

    def execute(self, context: dict[str, Any]) -> str:
        path = self._resolve_path()
        deadline = time.time() + self.timeout_sec

        while True:
            if self._is_data_ready(path):
                self.log.info("Source is available: %s", path)
                return str(path)

            if self.timeout_sec <= 0 or time.time() >= deadline:
                raise AirflowSkipException(f"Source file is not available: {path}")

            self.log.info("Waiting for source path %s", path)
            time.sleep(self.poke_interval_sec)

    def _resolve_path(self) -> Path:
        if self.source_path:
            return resolve_workspace_path(self.source_path)
        if self.config_path:
            cfg = load_pipeline_config(self.config_path)
            return resolve_source_path(cfg)
        raise ValueError("Either source_path or config_path must be provided")

    @staticmethod
    def _is_data_ready(path: Path) -> bool:
        if path.is_file():
            return path.stat().st_size > 0
        if path.is_dir():
            return any(p.is_file() and p.stat().st_size > 0 for p in path.iterdir())
        return False

