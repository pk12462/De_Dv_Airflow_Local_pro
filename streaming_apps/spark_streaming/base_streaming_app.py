"""
Base Spark Streaming App
========================
Abstract base defining the Spark Structured Streaming lifecycle.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseSparkStreamingApp(abc.ABC):
    """
    Abstract lifecycle for a Spark Structured Streaming application.

    Lifecycle::

        build_session()  →  read_stream()  →  process()  →  write_stream()

    Subclasses must implement all four abstract methods.
    """

    def __init__(self, app_name: str, config: dict[str, Any]) -> None:
        self.app_name = app_name
        self.config = config
        self.spark: Any = None
        self.streaming_query: Any = None
        logger.info("Initializing Spark Streaming app: %s", app_name)

    @abc.abstractmethod
    def build_session(self) -> Any:
        """Create and return a configured SparkSession."""

    @abc.abstractmethod
    def read_stream(self) -> Any:
        """Return a streaming DataFrame from the configured source."""

    @abc.abstractmethod
    def process(self, stream_df: Any) -> Any:
        """Apply transformations to the streaming DataFrame."""

    @abc.abstractmethod
    def write_stream(self, processed_df: Any) -> Any:
        """Write the processed stream to the configured sink."""

    def run(self) -> None:
        """Execute the full streaming pipeline lifecycle."""
        logger.info("Starting streaming pipeline: %s", self.app_name)
        self.spark = self.build_session()
        raw_stream = self.read_stream()
        processed = self.process(raw_stream)
        self.streaming_query = self.write_stream(processed)

        if self.streaming_query:
            logger.info("Streaming query started. Awaiting termination...")
            self.streaming_query.awaitTermination()

    def stop(self) -> None:
        """Gracefully stop the streaming query and SparkSession."""
        if self.streaming_query:
            self.streaming_query.stop()
            logger.info("Streaming query stopped")
        if self.spark:
            self.spark.stop()
            logger.info("SparkSession stopped")

