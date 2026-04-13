from __future__ import annotations

import re
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from operators.pipeline_config_utils import load_pipeline_config

_ALLOWED_NAME = re.compile(r"^[A-Za-z0-9_]+$")


class TargetLoadCheckOperator(BaseOperator):
    template_fields = ("config_path",)

    def __init__(self, config_path: str, min_rows: int = 1, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.min_rows = min_rows

    def execute(self, context: dict[str, Any]) -> dict[str, int]:
        cfg = load_pipeline_config(self.config_path)
        results: dict[str, int] = {}

        for target in cfg.get("targets", []):
            target_type = str(target.get("type", "")).lower()
            if target_type == "postgres":
                table = self._require_safe_name(str(target.get("table", "")), "postgres.table")
                count = self._check_postgres(cfg, table)
                results[f"postgres:{table}"] = count
            elif target_type == "cassandra":
                keyspace = self._require_safe_name(str(target.get("keyspace", "learning")), "cassandra.keyspace")
                table = self._require_safe_name(str(target.get("table", "")), "cassandra.table")
                count = self._check_cassandra(cfg, keyspace, table)
                results[f"cassandra:{keyspace}.{table}"] = count

        self.log.info("Target row counts: %s", results)
        return results

    def _check_postgres(self, cfg: dict[str, Any], table: str) -> int:
        try:
            import psycopg2
        except ImportError as exc:
            raise AirflowException("psycopg2 is required for postgres target validation") from exc

        conn = dict(cfg.get("connections", {}).get("postgres", {}))
        host = self._normalize_host(str(conn.get("host", "localhost")))
        sql = f"SELECT COUNT(1) FROM {table}"  # table name validated with regex

        with psycopg2.connect(
            host=host,
            port=conn.get("port", 5432),
            dbname=conn.get("database"),
            user=conn.get("user"),
            password=conn.get("password"),
        ) as db:
            with db.cursor() as cur:
                cur.execute(sql)
                count = int(cur.fetchone()[0])

        if count < self.min_rows:
            raise AirflowException(f"Postgres target {table} has {count} rows, expected >= {self.min_rows}")
        return count

    def _check_cassandra(self, cfg: dict[str, Any], keyspace: str, table: str) -> int:
        try:
            from cassandra.cluster import Cluster
        except ImportError as exc:
            raise AirflowException("cassandra-driver is required for cassandra target validation") from exc

        conn = dict(cfg.get("connections", {}).get("cassandra", {}))
        hosts = [self._normalize_host(str(h)) for h in conn.get("hosts", ["127.0.0.1"])]

        cluster = Cluster(contact_points=hosts, port=conn.get("port", 9042))
        try:
            session = cluster.connect(keyspace)
            sql = f"SELECT COUNT(1) FROM {table}"  # keyspace/table validated with regex
            row = session.execute(sql).one()
            count = int(row[0]) if row else 0
        finally:
            cluster.shutdown()

        if count < self.min_rows:
            raise AirflowException(
                f"Cassandra target {keyspace}.{table} has {count} rows, expected >= {self.min_rows}"
            )
        return count

    @staticmethod
    def _normalize_host(host: str) -> str:
        if host in {"localhost", "127.0.0.1"}:
            return "host.docker.internal"
        return host

    @staticmethod
    def _require_safe_name(value: str, field_name: str) -> str:
        if not value or not _ALLOWED_NAME.match(value):
            raise AirflowException(f"Invalid identifier for {field_name}: {value}")
        return value

