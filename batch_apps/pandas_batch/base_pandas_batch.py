"""
Base Pandas Batch App
=====================
Abstract base class for Pandas-based batch ETL jobs.
Suitable for small-to-medium workloads that fit in worker memory.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]


class BasePandasBatchApp(abc.ABC):
    """
    Abstract base for Pandas batch ETL applications.

    Lifecycle::

        extract()  →  transform()  →  load()
    """

    def __init__(self, app_name: str, config: dict[str, Any]) -> None:
        self.app_name = app_name
        self.config = config
        logger.info("Initializing Pandas Batch app: %s", app_name)

    @abc.abstractmethod
    def extract(self) -> "pd.DataFrame":
        """Read source data and return a pandas DataFrame."""

    @abc.abstractmethod
    def transform(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Apply business transformations and return a new DataFrame."""

    @abc.abstractmethod
    def load(self, df: "pd.DataFrame") -> None:
        """Write the transformed DataFrame to the target sink."""

    def run(self, logical_date: str | None = None) -> None:
        """Execute the full ETL pipeline."""
        if pd is None:
            raise ImportError("pandas is not installed. Run: pip install pandas")
        logger.info("Starting Pandas batch job: %s | date=%s", self.app_name, logical_date)
        raw_df = self.extract()
        transformed_df = self.transform(raw_df)
        self.load(transformed_df)
        logger.info("Pandas batch job completed: %s", self.app_name)

