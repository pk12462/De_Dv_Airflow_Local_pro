"""
Pandas Batch Application
========================
Concrete Pandas ETL:
  - Extract: CSV / Parquet / PostgreSQL / S3
  - Transform: Vectorized operations, type coercion, deduplication
  - Load: Parquet / CSV / PostgreSQL with chunked I/O
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Any

from batch_apps.pandas_batch.base_pandas_batch import BasePandasBatchApp

logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]


class PandasBatchApp(BasePandasBatchApp):
    """
    Lightweight Pandas batch ETL pipeline.

    Config keys expected::

        source:
          format: csv           # csv | parquet | postgres | s3_parquet
          path: /data/input/    # for file-based sources
          date_col: date
          chunk_size: 10000
        sink:
          format: parquet       # parquet | csv | postgres
          path: /data/output/
          write_mode: replace   # replace | append (for DB sinks)
        transform:
          dedup_cols: [id]
          fillna: {col: value}
          drop_cols: [col1, col2]
    """

    def __init__(self, config: dict[str, Any], logical_date: str | None = None) -> None:
        app_name = config.get("app_name", "de-dv-pandas-batch")
        super().__init__(app_name, config)
        self._source_cfg = config.get("source", {})
        self._sink_cfg = config.get("sink", {})
        self._transform_cfg = config.get("transform", {})
        self.logical_date = logical_date or datetime.utcnow().strftime("%Y-%m-%d")

    # ── Extract ────────────────────────────────────────────────────────────────

    def extract(self) -> "pd.DataFrame":
        fmt = self._source_cfg.get("format", "parquet")
        logger.info("Extracting data: format=%s date=%s", fmt, self.logical_date)

        if fmt == "csv":
            return self._extract_csv()
        elif fmt == "parquet":
            return self._extract_parquet()
        elif fmt == "postgres":
            return self._extract_postgres()
        elif fmt == "s3_parquet":
            return self._extract_s3_parquet()
        else:
            raise ValueError(f"Unsupported source format: {fmt}")

    def _extract_csv(self) -> "pd.DataFrame":
        path = self._source_cfg.get("path", "/tmp/input.csv")
        chunk_size = self._source_cfg.get("chunk_size", 10_000)
        date_col = self._source_cfg.get("date_col", "date")

        chunks = []
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            if date_col in chunk.columns:
                chunk = chunk[chunk[date_col] == self.logical_date]
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        logger.info("Extracted %d rows from CSV path=%s", len(df), path)
        return df

    def _extract_parquet(self) -> "pd.DataFrame":
        path = self._source_cfg.get("path", "/tmp/input.parquet")
        df = pd.read_parquet(path)
        date_col = self._source_cfg.get("date_col", "date")
        if date_col in df.columns:
            df = df[df[date_col] == self.logical_date]
        logger.info("Extracted %d rows from Parquet path=%s", len(df), path)
        return df

    def _extract_postgres(self) -> "pd.DataFrame":
        from sqlalchemy import create_engine

        db_url = os.getenv("DATABASE_URL", self._source_cfg.get("db_url", ""))
        table = self._source_cfg.get("table", "raw_events")
        date_col = self._source_cfg.get("date_col", "date")

        engine = create_engine(db_url)
        query = f"SELECT * FROM {table} WHERE {date_col} = '{self.logical_date}'"
        df = pd.read_sql(query, con=engine)
        logger.info("Extracted %d rows from PostgreSQL table=%s", len(df), table)
        return df

    def _extract_s3_parquet(self) -> "pd.DataFrame":
        import s3fs

        bucket = os.getenv("AWS_S3_BUCKET", self._source_cfg.get("bucket", ""))
        prefix = self._source_cfg.get("prefix", "data")
        path = f"s3://{bucket}/{prefix}/{self.logical_date}/"
        df = pd.read_parquet(path)
        logger.info("Extracted %d rows from S3 path=%s", len(df), path)
        return df

    # ── Transform ─────────────────────────────────────────────────────────────

    def transform(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Apply vectorized transformations from config."""
        # Deduplication
        dedup_cols = self._transform_cfg.get("dedup_cols")
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols)

        # Drop columns
        drop_cols = [c for c in self._transform_cfg.get("drop_cols", []) if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        # Fill nulls
        fillna_map = self._transform_cfg.get("fillna", {})
        if fillna_map:
            df = df.fillna(fillna_map)

        # Audit columns
        df["batch_date"] = self.logical_date
        df["processed_at"] = pd.Timestamp.utcnow()
        df["pipeline_version"] = "1.0.0"

        logger.info("Transformed DataFrame: %d rows, %d cols", len(df), len(df.columns))
        return df

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self, df: "pd.DataFrame") -> None:
        fmt = self._sink_cfg.get("format", "parquet")
        path = self._sink_cfg.get("path", f"/tmp/output.{fmt}")

        logger.info("Loading %d rows: format=%s path=%s", len(df), fmt, path)

        if fmt == "parquet":
            df.to_parquet(path, index=False)
        elif fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "postgres":
            self._load_postgres(df)
        else:
            raise ValueError(f"Unsupported sink format: {fmt}")

        logger.info("Data loaded to %s", path)

    def _load_postgres(self, df: "pd.DataFrame") -> None:
        from sqlalchemy import create_engine

        db_url = os.getenv("DATABASE_URL", self._sink_cfg.get("db_url", ""))
        table = self._sink_cfg.get("table", "processed_events")
        write_mode = self._sink_cfg.get("write_mode", "append")
        chunk_size = self._sink_cfg.get("chunk_size", 1000)

        engine = create_engine(db_url)
        df.to_sql(table, con=engine, if_exists=write_mode, index=False, chunksize=chunk_size)
        logger.info("Loaded %d rows to PostgreSQL table=%s", len(df), table)


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import yaml

    parser = argparse.ArgumentParser(description="Pandas Batch ETL App")
    parser.add_argument("--config", default="pipelines/configs/batch_pipeline_config.yaml")
    parser.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    app = PandasBatchApp(cfg, logical_date=args.date)
    app.run(logical_date=args.date)

