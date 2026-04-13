from __future__ import annotations

import subprocess
from typing import Any

from airflow.models import BaseOperator

from operators.pipeline_config_utils import resolve_workspace_path


class PipelineRunnerOperator(BaseOperator):
    template_fields = ("config_path", "runner_path")

    def __init__(
        self,
        config_path: str,
        runner_path: str = "/opt/airflow/etl-project/spark-job/local_batch_streaming_runner.py",
        python_bin: str = "python",
        dry_run: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.runner_path = runner_path
        self.python_bin = python_bin
        self.dry_run = dry_run

    def execute(self, context: dict[str, Any]) -> None:
        config = resolve_workspace_path(self.config_path)
        runner = resolve_workspace_path(self.runner_path)

        cmd = [self.python_bin, str(runner), "--config", str(config)]
        if self.dry_run:
            cmd.append("--dry-run")

        self.log.info("Executing pipeline command: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)

