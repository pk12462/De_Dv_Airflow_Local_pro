"""
Base Pipeline
=============
Abstract base class defining the pipeline contract.
All concrete pipelines (streaming, batch, SQL) extend this class.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BasePipeline(abc.ABC):
    """
    Abstract base pipeline.

    Lifecycle::

        load_config()  →  validate()  →  run()

    on_failure() is called automatically if run() raises an exception.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.pipeline_name = config.get("pipeline", {}).get("name", self.__class__.__name__)
        self.version = config.get("pipeline", {}).get("version", "1.0.0")
        logger.info("Pipeline initialized: %s v%s", self.pipeline_name, self.version)

    @classmethod
    def from_yaml(cls, config_path: str) -> "BasePipeline":
        """Load pipeline config from a YAML file."""
        from pipelines.base.pipeline_utils import PipelineUtils
        config = PipelineUtils.load_yaml(config_path)
        return cls(config)

    @abc.abstractmethod
    def validate(self) -> None:
        """
        Validate pipeline configuration before execution.
        Raise ValueError for any invalid or missing settings.
        """

    @abc.abstractmethod
    def run(self, **kwargs: Any) -> None:
        """Execute the pipeline."""

    def on_failure(self, exc: Exception) -> None:
        """
        Called when run() raises an exception.
        Override to implement alerting, cleanup, or DLQ routing.
        """
        logger.error(
            "Pipeline %s FAILED: %s",
            self.pipeline_name,
            exc,
            exc_info=True,
        )

    def execute(self, **kwargs: Any) -> None:
        """Top-level entrypoint: validate → run, with failure handling."""
        try:
            self.validate()
            self.run(**kwargs)
            logger.info("Pipeline %s completed successfully", self.pipeline_name)
        except Exception as exc:  # noqa: BLE001
            self.on_failure(exc)
            raise

