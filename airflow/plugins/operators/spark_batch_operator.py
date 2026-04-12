from __future__ import annotations

import subprocess
from typing import Any

from airflow.models import BaseOperator


class SparkBatchOperator(BaseOperator):
    template_fields = ("config_path",)

    def __init__(self, config_path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context: dict) -> None:
        logical_date = context["ds"]
        cmd = [
            "python",
            "-m",
            "batch_apps.spark_batch.spark_batch_app",
            "--config",
            self.config_path,
            "--date",
            logical_date,
        ]
        self.log.info("Executing Spark batch app: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)

