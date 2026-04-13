from __future__ import annotations

import json
from pathlib import Path

from pipelines.base.connection_manager import ConnectionManager


ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_configs_use_connection_refs_only() -> None:
    config_dir = ROOT / "etl-project" / "appconfig" / "dev"
    for path in sorted(config_dir.glob("pipeline_*_pg_cassandra.json")):
        with path.open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        connections = payload.get("connections", {})
        assert connections.get("postgres", {}).get("connectionRef") == "postgres", f"missing postgres ref in {path.name}"
        assert connections.get("cassandra", {}).get("connectionRef") == "cassandra", f"missing cassandra ref in {path.name}"
        assert "password" not in json.dumps(connections).lower(), f"hardcoded secret found in {path.name}"


def test_connection_manager_resolves_local_defaults() -> None:
    mgr = ConnectionManager()
    resolved = mgr.resolve_all({
        "postgres": {"connectionRef": "postgres"},
        "cassandra": {"connectionRef": "cassandra"},
    })

    assert resolved["postgres"]["host"]
    assert resolved["postgres"]["port"]
    assert resolved["cassandra"]["hosts"]
    assert resolved["cassandra"]["port"]

