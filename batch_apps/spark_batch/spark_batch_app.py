"""
Spark Batch Application
=======================
Concrete Spark batch ETL:
  - Extract: S3 / HDFS / Delta Lake / local Parquet
  - Transform: Aggregations, joins, SCD-2, type casting
  - Load: Delta Lake / Parquet with partition management
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Any

from batch_apps.spark_batch.base_spark_batch import BaseSparkBatchApp
from batch_apps.spark_batch.transformations.batch_transforms import BatchTransforms

logger = logging.getLogger(__name__)


class SparkBatchApp(BaseSparkBatchApp):
    """
    Daily Spark batch ETL pipeline.

    Config keys expected::

        spark:
          master: local[*]
          app_name: de-dv-spark-batch
          shuffle_partitions: 200
          executor_memory: 2g
          driver_memory: 1g
        source:
          format: parquet          # parquet | delta | csv | json | jdbc
          path: s3://bucket/raw/   # for file-based sources
          jdbc_url: ...            # for JDBC sources
          table: raw_events
          partition_col: date
          lookback_days: 1
        sink:
          format: delta
          path: s3://bucket/processed/
          table: processed_events
          partition_by: [date, event_type]
          write_mode: append       # append | overwrite | merge
    """

    def __init__(self, config: dict[str, Any], logical_date: str | None = None) -> None:
        app_name = config.get("spark", {}).get("app_name", "de-dv-spark-batch")
        super().__init__(app_name, config)
        self._spark_cfg = config.get("spark", {})
        self._source_cfg = config.get("source", {})
        self._sink_cfg = config.get("sink", {})
        self.logical_date = logical_date or datetime.utcnow().strftime("%Y-%m-%d")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def build_session(self) -> Any:
        """Create a SparkSession configured for batch workloads."""
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise ImportError("pyspark is not installed. Run: pip install pyspark") from exc

        master = self._spark_cfg.get("master", os.getenv("SPARK_MASTER_URL", "local[*]"))
        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .master(master)
            .config(
                "spark.sql.shuffle.partitions",
                self._spark_cfg.get("shuffle_partitions", "200"),
            )
            .config(
                "spark.executor.memory",
                self._spark_cfg.get("executor_memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g")),
            )
            .config(
                "spark.driver.memory",
                self._spark_cfg.get("driver_memory", os.getenv("SPARK_DRIVER_MEMORY", "1g")),
            )
        )

        sink_format = self._sink_cfg.get("format", "parquet")
        if sink_format == "delta":
            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self._spark_cfg.get("log_level", "WARN"))
        logger.info("SparkSession created: master=%s appName=%s", master, self.app_name)
        return spark

    def extract(self) -> Any:
        """Read source data filtered by logical_date."""
        fmt = self._source_cfg.get("format", "parquet")
        logger.info("Extracting data: format=%s date=%s", fmt, self.logical_date)

        if fmt == "jdbc":
            return self._extract_jdbc()
        elif fmt in ("parquet", "delta", "csv", "json"):
            return self._extract_file(fmt)
        else:
            raise ValueError(f"Unsupported source format: {fmt}")

    def _extract_file(self, fmt: str) -> Any:
        """Read file-based source (Parquet / Delta / CSV / JSON)."""
        from pyspark.sql import functions as F

        path = self._source_cfg.get("path", f"/tmp/spark/input/{fmt}")
        partition_col = self._source_cfg.get("partition_col", "date")
        lookback = self._source_cfg.get("lookback_days", 1)

        df = self.spark.read.format(fmt).load(path)

        # Apply date filter if partition column exists
        if partition_col in df.columns:
            df = df.filter(F.col(partition_col) == self.logical_date)

        logger.info("Extracted %s records from path=%s", df.count(), path)
        return df

    def _extract_jdbc(self) -> Any:
        """Read from a JDBC source (PostgreSQL, MySQL, etc.)."""
        jdbc_url = self._source_cfg.get("jdbc_url", os.getenv("DATABASE_URL", ""))
        table = self._source_cfg.get("table", "raw_events")
        partition_col = self._source_cfg.get("partition_col", "date")

        query = f"(SELECT * FROM {table} WHERE {partition_col} = '{self.logical_date}') AS t"
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("numPartitions", self._source_cfg.get("num_partitions", 4))
            .load()
        )

    def transform(self, df: Any) -> Any:
        """Apply business transformations."""
        transforms = BatchTransforms()
        df = transforms.cast_timestamp_columns(df, self._source_cfg.get("ts_columns", []))
        df = transforms.add_audit_columns(df, self.logical_date)
        df = transforms.drop_duplicates(df, self._source_cfg.get("dedup_keys", []))
        return df

    def load(self, df: Any) -> None:
        """Write DataFrame to the configured sink."""
        fmt = self._sink_cfg.get("format", "parquet")
        path = self._sink_cfg.get("path", f"/tmp/spark/output/{fmt}")
        write_mode = self._sink_cfg.get("write_mode", "append")
        partition_by = self._sink_cfg.get("partition_by", [])

        logger.info("Loading data: format=%s path=%s mode=%s", fmt, path, write_mode)
        writer = df.write.format(fmt).mode(write_mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)
        logger.info("Data loaded successfully to path=%s", path)


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import yaml

    parser = argparse.ArgumentParser(description="Spark Batch ETL App")
    parser.add_argument("--config", default="pipelines/configs/batch_pipeline_config.yaml")
    parser.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    app = SparkBatchApp(cfg, logical_date=args.date)
    try:
        app.run(logical_date=args.date)
    finally:
        app.stop()

