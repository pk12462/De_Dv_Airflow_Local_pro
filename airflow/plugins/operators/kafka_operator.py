from __future__ import annotations

import subprocess
from typing import Any

from airflow.models import BaseOperator


class KafkaOperator(BaseOperator):
    template_fields = ("config_path",)

    def __init__(self, mode: str, config_path: str, timeout_seconds: int = 300, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.mode = mode
        self.config_path = config_path
        self.timeout_seconds = timeout_seconds

    def execute(self, context: dict) -> None:
        if self.mode not in {"consumer", "producer"}:
            raise ValueError("mode must be 'consumer' or 'producer'")

        module = (
            "streaming_apps.kafka.consumer.kafka_consumer"
            if self.mode == "consumer"
            else "streaming_apps.kafka.producer.kafka_producer"
        )

        cmd = ["python", "-m", module]
        self.log.info("Executing Kafka %s app: %s", self.mode, " ".join(cmd))
        subprocess.run(cmd, check=True, timeout=self.timeout_seconds)

