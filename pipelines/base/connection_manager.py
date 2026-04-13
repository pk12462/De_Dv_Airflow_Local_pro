from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pipelines.base.pipeline_utils import PipelineUtils
from pipelines.egress.egress_handler import EgressHandler


class ConnectionManager:
    """Resolves app connection references and enforces egress rules."""

    def __init__(
        self,
        app_config_path: str | Path | None = None,
        egress_rules_path: str | Path | None = None,
    ) -> None:
        self._app_config_path = Path(app_config_path) if app_config_path else self._default_app_config_path()
        payload = PipelineUtils.load_yaml(self._app_config_path)
        self._connections = payload.get("connections", {})
        self._egress = EgressHandler(rules_path=egress_rules_path)

    def resolve_all(self, pipeline_connections: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
        pipeline_connections = pipeline_connections or {}
        resolved: dict[str, dict[str, Any]] = {}
        for logical_name, cfg in pipeline_connections.items():
            if isinstance(cfg, dict):
                resolved[logical_name] = self.resolve_one(cfg)
            else:
                resolved[logical_name] = self.resolve_one({"connectionRef": str(cfg)})
        return resolved

    def resolve_one(self, conn_cfg: dict[str, Any]) -> dict[str, Any]:
        ref = str(conn_cfg.get("connectionRef", "")).strip()
        base = self._connections.get(ref, {}) if ref else {}
        merged = PipelineUtils.deep_merge(base, {k: v for k, v in conn_cfg.items() if k != "connectionRef"})

        # Normalize common auth fields for database writers.
        credentials = merged.pop("credentials", {}) if isinstance(merged.get("credentials"), dict) else {}
        if "user" not in merged:
            merged["user"] = credentials.get("username") or credentials.get("user")
        if "password" not in merged:
            merged["password"] = credentials.get("password")

        if "port" in merged and isinstance(merged["port"], str) and merged["port"].isdigit():
            merged["port"] = int(merged["port"])

        conn_type = str(merged.get("type", "")).lower()

        # Local-friendly defaults for learning setup when env vars are not exported.
        if conn_type == "postgres" or ref == "postgres":
            if not merged.get("host"):
                merged["host"] = "localhost"
            if not merged.get("port"):
                merged["port"] = 5432
            if not merged.get("database"):
                merged["database"] = "learning_db"
        if conn_type == "cassandra" or ref == "cassandra":
            hosts = merged.get("hosts")
            if not hosts:
                merged["hosts"] = [merged.get("host") or "127.0.0.1"]
            if not merged.get("port"):
                merged["port"] = 9042
            if not merged.get("keyspace"):
                merged["keyspace"] = "learning"

        return merged

    def assert_egress(self, pipeline_name: str, host: str, port: int) -> None:
        self._egress.check(pipeline_name=pipeline_name, host=host, port=port)

    @staticmethod
    def _default_app_config_path() -> Path:
        env_path = os.getenv("APP_CONNECTION_CONFIG_PATH")
        if env_path:
            return Path(env_path)

        airflow_path = Path("/opt/airflow/pipelines/configs/app_connection_config.yaml")
        if airflow_path.exists():
            return airflow_path

        return Path(__file__).resolve().parents[1] / "configs" / "app_connection_config.yaml"



