"""
Base Spark Batch App
====================
Abstract base class for Spark batch ETL jobs.
Enforces the Extract → Transform → Load lifecycle contract.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseSparkBatchApp(abc.ABC):
    """
    Abstract base for Spark batch ETL applications.

    Lifecycle::

        build_session()  →  extract()  →  transform()  →  load()

    Subclasses must implement all four abstract methods.
    """

    def __init__(self, app_name: str, config: dict[str, Any]) -> None:
        self.app_name = app_name
        self.config = config
        self.spark: Any = None
        logger.info("Initializing Spark Batch app: %s", app_name)

    @abc.abstractmethod
    def build_session(self) -> Any:
        """Create and return a configured SparkSession."""

    @abc.abstractmethod
    def extract(self) -> Any:
        """Read source data and return a Spark DataFrame."""

    @abc.abstractmethod
    def transform(self, df: Any) -> Any:
        """Apply business transformations and return a new DataFrame."""

    @abc.abstractmethod
    def load(self, df: Any) -> None:
        """Write the transformed DataFrame to the target sink."""

    def run(self, logical_date: str | None = None) -> None:
        """Execute the full ETL pipeline."""
        logger.info("Starting batch job: %s | logical_date=%s", self.app_name, logical_date)
        self.spark = self.build_session()
        raw_df = self.extract()
        transformed_df = self.transform(raw_df)
        self.load(transformed_df)
        logger.info("Batch job completed: %s", self.app_name)

    def stop(self) -> None:
        """Stop the SparkSession."""
        if self.spark:
            self.spark.stop()
            logger.info("SparkSession stopped")

