from __future__ import annotations

import subprocess
from typing import Any

from airflow.models import BaseOperator


class SparkStreamingOperator(BaseOperator):
    template_fields = ("config_path",)

    def __init__(self, config_path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context: dict) -> None:
        cmd = [
            "python",
            "-m",
            "streaming_apps.spark_streaming.spark_streaming_app",
            "--config",
            self.config_path,
        ]
        self.log.info("Executing Spark streaming app: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)

