"""
Batch Transformations
=====================
Reusable Spark DataFrame transformations for batch ETL jobs:
  - Type casting, aggregations, joins
  - SCD Type 2 (slowly changing dimensions)
  - Deduplication, audit columns, null handling
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class BatchTransforms:
    """Collection of reusable Spark batch DataFrame transformations."""

    # ── Type Casting ──────────────────────────────────────────────────────────

    def cast_timestamp_columns(self, df: Any, ts_columns: list[str]) -> Any:
        """Cast specified columns from string/long → TimestampType."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType

        for col in ts_columns:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(TimestampType()))
        return df

    def cast_numeric_columns(self, df: Any, col_type_map: dict[str, str]) -> Any:
        """Cast columns to specified numeric types. col_type_map: {col_name: 'double'|'int'|'long'}"""
        from pyspark.sql import functions as F

        for col_name, dtype in col_type_map.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        return df

    # ── Deduplication ─────────────────────────────────────────────────────────

    def drop_duplicates(self, df: Any, subset: list[str] | None = None) -> Any:
        """Remove duplicate rows, optionally based on a subset of columns."""
        if subset:
            return df.dropDuplicates(subset)
        return df.dropDuplicates()

    # ── Aggregations ──────────────────────────────────────────────────────────

    def aggregate(
        self,
        df: Any,
        group_cols: list[str],
        agg_exprs: list[Any],
    ) -> Any:
        """Group by group_cols and apply aggregation expressions."""
        return df.groupBy(*group_cols).agg(*agg_exprs)

    def count_by_group(self, df: Any, group_cols: list[str]) -> Any:
        """Count records per group."""
        from pyspark.sql import functions as F

        return df.groupBy(*group_cols).agg(F.count("*").alias("record_count"))

    # ── Joins ─────────────────────────────────────────────────────────────────

    def enrich_with_lookup(
        self,
        df: Any,
        lookup_df: Any,
        join_col: str,
        join_type: str = "left",
    ) -> Any:
        """Join df with a lookup DataFrame on join_col."""
        return df.join(lookup_df, on=join_col, how=join_type)

    # ── SCD Type 2 ────────────────────────────────────────────────────────────

    def apply_scd2(
        self,
        current_df: Any,
        incoming_df: Any,
        business_key: str,
        effective_date_col: str = "effective_date",
        expiry_date_col: str = "expiry_date",
        is_current_col: str = "is_current",
        logical_date: str = "2026-01-01",
    ) -> Any:
        """
        Apply SCD Type 2 logic:
          1. Expire records in current_df that have changes in incoming_df
          2. Insert new records from incoming_df
        Returns the merged DataFrame.
        """
        from pyspark.sql import functions as F

        high_date = "9999-12-31"

        # Mark expired records
        changed = current_df.join(
            incoming_df.select(business_key),
            on=business_key,
            how="inner",
        ).withColumn(is_current_col, F.lit(False)).withColumn(
            expiry_date_col, F.lit(logical_date)
        )

        unchanged = current_df.join(
            incoming_df.select(business_key),
            on=business_key,
            how="left_anti",
        )

        # New records
        new_records = incoming_df.withColumn(
            effective_date_col, F.lit(logical_date)
        ).withColumn(expiry_date_col, F.lit(high_date)).withColumn(
            is_current_col, F.lit(True)
        )

        return unchanged.unionByName(changed).unionByName(new_records)

    # ── Null Handling ─────────────────────────────────────────────────────────

    def fill_nulls(self, df: Any, fill_map: dict[str, Any]) -> Any:
        """Fill null values per column using a mapping dict."""
        return df.fillna(fill_map)

    def drop_null_rows(self, df: Any, subset: list[str] | None = None) -> Any:
        """Drop rows with nulls in any (or specified) columns."""
        return df.dropna(subset=subset)

    # ── Audit Columns ─────────────────────────────────────────────────────────

    def add_audit_columns(self, df: Any, logical_date: str) -> Any:
        """Append standard audit columns to every batch output."""
        from pyspark.sql import functions as F

        return (
            df.withColumn("batch_date", F.lit(logical_date))
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("pipeline_version", F.lit("1.0.0"))
        )

