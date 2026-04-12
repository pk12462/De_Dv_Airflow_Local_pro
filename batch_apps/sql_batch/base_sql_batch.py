"""
Base SQL Batch App
==================
Abstract base class for SQL-based batch ETL jobs.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseSqlBatchApp(abc.ABC):
    """
    Abstract base for SQL batch applications.

    Lifecycle::

        get_connection()  →  execute_query()  →  close()
    """

    def __init__(self, app_name: str, config: dict[str, Any]) -> None:
        self.app_name = app_name
        self.config = config
        self._connection: Any = None
        logger.info("Initializing SQL Batch app: %s", app_name)

    @abc.abstractmethod
    def get_connection(self) -> Any:
        """Establish and return a database connection."""

    @abc.abstractmethod
    def execute_query(self, sql: str, params: dict | None = None) -> Any:
        """Execute a SQL statement with optional parameters."""

    @abc.abstractmethod
    def close(self) -> None:
        """Close the database connection."""

    def run_script(self, script_path: str, params: dict | None = None) -> None:
        """Read a .sql file and execute all statements within it."""
        with open(script_path) as f:
            sql = f.read()
        self.execute_query(sql, params)
        logger.info("Script executed: %s", script_path)

    def run(self, scripts: list[str], params: dict | None = None) -> None:
        """Run an ordered list of SQL scripts as a single batch job."""
        logger.info("Starting SQL batch job: %s", self.app_name)
        self._connection = self.get_connection()
        try:
            for script_path in scripts:
                logger.info("Executing script: %s", script_path)
                self.run_script(script_path, params)
            logger.info("SQL batch job completed: %s", self.app_name)
        finally:
            self.close()

