"""
SQL Batch Application
=====================
Concrete SQL batch runner using SQLAlchemy.
Supports parameterized Jinja-templated queries with ordered script execution.
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Any

from batch_apps.sql_batch.base_sql_batch import BaseSqlBatchApp

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Connection, Engine
except ImportError:
    create_engine = None  # type: ignore[assignment]
    text = None           # type: ignore[assignment]


class SqlBatchApp(BaseSqlBatchApp):
    """
    SQL batch runner (Extract → Transform → Load) via SQLAlchemy.

    Config keys expected::

        database:
          url: postgresql+psycopg2://user:pass@host:5432/db
        scripts:
          extract: batch_apps/sql_batch/queries/extract_raw.sql
          transform: batch_apps/sql_batch/queries/transform_stage.sql
          load: batch_apps/sql_batch/queries/load_target.sql
        params:
          schema: public
          target_table: processed_events
    """

    def __init__(self, config: dict[str, Any], logical_date: str | None = None) -> None:
        app_name = config.get("app_name", "de-dv-sql-batch")
        super().__init__(app_name, config)
        self._db_cfg = config.get("database", {})
        self._scripts_cfg = config.get("scripts", {})
        self._params = config.get("params", {})
        self.logical_date = logical_date or datetime.utcnow().strftime("%Y-%m-%d")
        self._engine: Any = None

    # ── Connection ────────────────────────────────────────────────────────────

    def get_connection(self) -> Any:
        """Create SQLAlchemy engine and return a connection."""
        if create_engine is None:
            raise ImportError("SQLAlchemy is not installed. Run: pip install sqlalchemy")

        db_url = self._db_cfg.get("url", os.getenv("DATABASE_URL", ""))
        pool_size = self._db_cfg.get("pool_size", 5)
        self._engine: Engine = create_engine(db_url, pool_size=pool_size, pool_pre_ping=True)
        connection: Connection = self._engine.connect()
        logger.info("Connected to database: %s", db_url.split("@")[-1])
        return connection

    def execute_query(self, sql: str, params: dict | None = None) -> Any:
        """Execute SQL using Jinja2 templating for parameter injection."""
        merged_params = {
            "logical_date": self.logical_date,
            "batch_date": self.logical_date,
            **self._params,
            **(params or {}),
        }

        # Jinja2 template rendering
        rendered_sql = self._render_template(sql, merged_params)

        logger.debug("Executing SQL (first 200 chars): %s", rendered_sql[:200])
        result = self._connection.execute(text(rendered_sql))
        self._connection.commit()
        return result

    def _render_template(self, sql: str, params: dict) -> str:
        """Render a Jinja2-templated SQL string with params."""
        try:
            from jinja2 import Template
            return Template(sql).render(**params)
        except ImportError:
            # Fallback: simple string format
            for k, v in params.items():
                sql = sql.replace("{{ " + k + " }}", str(v))
                sql = sql.replace("{{" + k + "}}", str(v))
            return sql

    def close(self) -> None:
        """Close connection and dispose engine."""
        if self._connection:
            self._connection.close()
        if self._engine:
            self._engine.dispose()
        logger.info("Database connection closed")

    # ── Convenience run ───────────────────────────────────────────────────────

    def run_etl(self) -> None:
        """Run the standard Extract → Transform → Load script sequence."""
        scripts = []
        for step in ("extract", "transform", "load"):
            script_path = self._scripts_cfg.get(step)
            if script_path:
                scripts.append(script_path)
            else:
                logger.warning("No script configured for step: %s", step)

        self.run(scripts=scripts, params={"logical_date": self.logical_date})


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import yaml

    parser = argparse.ArgumentParser(description="SQL Batch ETL App")
    parser.add_argument("--config", default="pipelines/configs/batch_pipeline_config.yaml")
    parser.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    app = SqlBatchApp(cfg, logical_date=args.date)
    app.run_etl()

