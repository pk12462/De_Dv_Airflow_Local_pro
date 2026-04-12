"""
Stream Transformations
======================
Reusable stateless and stateful transformations for Spark Structured Streaming.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class StreamTransforms:
    """Collection of reusable Spark Structured Streaming transformations."""

    def __init__(self, spark_config: dict[str, Any] | None = None) -> None:
        self.config = spark_config or {}

    def deduplicate(
        self,
        df: Any,
        id_col: str = "event_id",
        watermark_col: str = "event_ts",
        watermark_delay: str = "10 minutes",
    ) -> Any:
        """
        Remove duplicate events using watermarking and dropDuplicates.
        Prevents reprocessing of already-seen event IDs within the watermark window.
        """
        from pyspark.sql import functions as F

        return (
            df.withWatermark(watermark_col, watermark_delay)
            .dropDuplicates([id_col])
        )

    def apply_watermark(self, df: Any, event_time_col: str, delay: str = "10 minutes") -> Any:
        """Apply watermark for late data handling."""
        return df.withWatermark(event_time_col, delay)

    def tumbling_window_agg(
        self,
        df: Any,
        event_time_col: str,
        window_duration: str = "5 minutes",
        group_col: str = "event_type",
    ) -> Any:
        """
        Aggregate events using a tumbling (non-overlapping) time window.
        Returns count per (window, group_col).
        """
        from pyspark.sql import functions as F

        return (
            df.withWatermark(event_time_col, "10 minutes")
            .groupBy(F.window(F.col(event_time_col), window_duration), F.col(group_col))
            .agg(F.count("*").alias("event_count"))
        )

    def sliding_window_agg(
        self,
        df: Any,
        event_time_col: str,
        window_duration: str = "10 minutes",
        slide_duration: str = "5 minutes",
        group_col: str = "event_type",
    ) -> Any:
        """Aggregate events using a sliding (overlapping) time window."""
        from pyspark.sql import functions as F

        return (
            df.withWatermark(event_time_col, "10 minutes")
            .groupBy(
                F.window(F.col(event_time_col), window_duration, slide_duration),
                F.col(group_col),
            )
            .agg(F.count("*").alias("event_count"))
        )

    def enforce_schema(self, df: Any, required_columns: list[str]) -> Any:
        """Drop rows missing any of the required columns."""
        from pyspark.sql import functions as F

        condition = F.lit(True)
        for col in required_columns:
            condition = condition & F.col(col).isNotNull()
        return df.filter(condition)

    def add_processing_metadata(self, df: Any) -> Any:
        """Append processing timestamp and pipeline version columns."""
        from pyspark.sql import functions as F

        return (
            df.withColumn("processed_at", F.current_timestamp())
            .withColumn("pipeline_version", F.lit("1.0.0"))
        )

    def filter_by_event_types(self, df: Any, allowed_types: list[str], col: str = "event_type") -> Any:
        """Keep only rows whose event_type is in allowed_types."""
        from pyspark.sql import functions as F

        return df.filter(F.col(col).isin(allowed_types))

