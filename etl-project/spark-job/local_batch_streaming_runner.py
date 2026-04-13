from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipelines.base.connection_manager import ConnectionManager


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | local-pipeline | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _apply_transforms(records: list[dict[str, Any]], transforms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = records
    for t in transforms:
        t_type = str(t.get("type", "")).lower()
        if t_type == "filter_not_null":
            col = t.get("column")
            out = [r for r in out if r.get(col) not in (None, "")]
        elif t_type == "add_current_timestamp":
            col = t.get("column", "processed_at")
            now = datetime.now(timezone.utc).isoformat()
            out = [{**r, col: now} for r in out]
    return out


def _read_csv_file(path: Path, delimiter: str = ",") -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def _read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _expand_carid(path_value: str) -> str:
    carid = os.getenv("CARID", "8999")
    return path_value.replace("${CARID}", carid).replace("{CARID}", carid)


def _map_adls_to_local(path_value: str) -> list[Path]:
    normalized = _expand_carid(path_value).replace("\\", "/")
    if not normalized.startswith("/mnt/"):
        return []

    parts = normalized.split("/")
    # /mnt/<mount>/inbound/<domain>/<file>
    relative_parts = parts[4:] if len(parts) > 4 and parts[3] == "inbound" else parts[3:]
    if not relative_parts:
        return []

    project_root = Path(__file__).resolve().parents[1]
    local_root_candidates = [
        Path("/opt/airflow") / "etl-project" / "local_source",
        project_root / "local_source",
    ]

    candidates: list[Path] = []
    for root in local_root_candidates:
        candidates.append(root.joinpath(*relative_parts))
        candidates.append(root / relative_parts[-1])
    return candidates


def _resolve_source_path(path_value: str) -> Path:
    expanded = _expand_carid(path_value)
    source_path = Path(expanded)

    candidates: list[Path] = [source_path]
    candidates.extend(_map_adls_to_local(expanded))

    if not source_path.is_absolute():
        candidates.extend(
            [
                Path.cwd() / source_path,
                Path("/opt/airflow") / source_path,
                Path(__file__).resolve().parents[2] / source_path,
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def _normalize_pipeline_config(cfg: dict[str, Any]) -> dict[str, Any]:
    if "source" in cfg and "targets" in cfg:
        return cfg

    gi = cfg.get("generalInfo", {})
    mode = str(cfg.get("mode") or gi.get("processType", "batch")).lower()
    mode = "streaming" if mode.startswith("stream") else "batch"

    normalized: dict[str, Any] = {
        "pipelineName": cfg.get("pipelineName") or gi.get("appName", "pipeline"),
        "mode": mode,
        "source": {},
        "transforms": [],
        "targets": [],
        "connections": cfg.get("connections", {}),
        "streaming": cfg.get("streaming", {}),
    }

    transformations = cfg.get("mainFlow", [{}])[0].get("transformationList", [])
    transformations = sorted(transformations, key=lambda t: int(t.get("generalInfo", {}).get("order", 0)))

    for t in transformations:
        t_info = t.get("generalInfo", {})
        t_props = t.get("props", {})
        t_type = str(t_info.get("transformationType", "")).lower()

        if t_type == "source" and not normalized["source"]:
            src_sys = str(t_props.get("srcSysName", "local_csv")).lower()
            src_props = t_props.get("srcSysProp", {})
            source_type = src_sys if src_sys.startswith("local_") else "local_csv"
            source_path = (
                src_props.get("filePath")
                or src_props.get("path")
                or src_props.get("localFallbackPath")
                or src_props.get("inputPath")
            )
            if not source_path:
                raise ValueError("Source filePath/path is required in source transformation")
            normalized["source"] = {
                "type": source_type,
                "path": source_path,
                "delimiter": src_props.get("delimiter", "|"),
            }
            continue

        if t_type == "expression":
            runtime_transform = t_props.get("runtimeTransform")
            if isinstance(runtime_transform, dict):
                normalized["transforms"].append(runtime_transform)
            elif "processed_at" in str(t_props.get("trnsfmRules", "")).lower():
                normalized["transforms"].append({"type": "add_current_timestamp", "column": "processed_at"})
            continue

        if t_type == "filter":
            column = t_props.get("column")
            if not column and "condition" in t_props:
                condition = str(t_props.get("condition", ""))
                column = condition.split(" ", 1)[0] if " " in condition else condition
            if column:
                normalized["transforms"].append({"type": "filter_not_null", "column": column})
            continue

        if t_type == "target":
            tgt_sys = str(t_props.get("tgtSysName", "")).lower()
            tgt_props = t_props.get("tgtSysProp", {})
            if "postgres" in tgt_sys:
                normalized["targets"].append(
                    {
                        "type": "postgres",
                        "table": tgt_props.get("table") or tgt_props.get("tableName") or "events",
                    }
                )
            elif "cassandra" in tgt_sys:
                normalized["targets"].append(
                    {
                        "type": "cassandra",
                        "keyspace": tgt_props.get("keyspace", "learning"),
                        "table": tgt_props.get("table") or tgt_props.get("tableName") or "events",
                    }
                )

    if not normalized["source"]:
        raise ValueError("Pipeline config did not contain a SOURCE transformation")
    return normalized


def _read_source_records(source_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    source_type = str(source_cfg.get("type", "local_csv")).lower()
    source_path = _resolve_source_path(str(source_cfg["path"]))

    if source_type == "local_csv":
        return _read_csv_file(source_path, delimiter=",")
    if source_type == "local_delimited_file":
        return _read_csv_file(source_path, delimiter=source_cfg.get("delimiter", "|"))
    if source_type == "local_jsonl_file":
        return _read_jsonl_file(source_path)
    if source_type == "local_jsonl_dir":
        return _read_jsonl_dir_once(source_path)

    raise ValueError(f"Unsupported source type: {source_type}")


def _write_postgres(records: list[dict[str, Any]], conn: dict[str, Any], table: str, dry_run: bool) -> None:
    if dry_run:
        logging.info("DRY-RUN postgres target table=%s rows=%d", table, len(records))
        return

    import psycopg2

    if not records:
        return
    columns = list(records[0].keys())
    placeholders = ",".join(["%s"] * len(columns))
    col_sql = ",".join(columns)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

    with psycopg2.connect(
        host=conn.get("host"),
        port=conn.get("port", 5432),
        dbname=conn.get("database"),
        user=conn.get("user"),
        password=conn.get("password"),
    ) as db:
        with db.cursor() as cur:
            for rec in records:
                cur.execute(sql, [rec.get(c) for c in columns])
        db.commit()


def _write_cassandra(records: list[dict[str, Any]], conn: dict[str, Any], keyspace: str, table: str, dry_run: bool) -> None:
    if dry_run:
        logging.info("DRY-RUN cassandra target %s.%s rows=%d", keyspace, table, len(records))
        return

    from cassandra.cluster import Cluster

    if not records:
        return

    cluster = Cluster(contact_points=conn.get("hosts", ["127.0.0.1"]), port=conn.get("port", 9042))
    session = cluster.connect(keyspace)
    columns = list(records[0].keys())
    placeholders = ",".join(["%s"] * len(columns))
    col_sql = ",".join(columns)
    cql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

    try:
        prepared = session.prepare(cql)
        for rec in records:
            session.execute(prepared, [rec.get(c) for c in columns])
    finally:
        cluster.shutdown()


def run_batch(config: dict[str, Any], dry_run: bool) -> int:
    rows = _read_source_records(config["source"])

    rows = _apply_transforms(rows, config.get("transforms", []))
    conn_manager = ConnectionManager()
    connections = conn_manager.resolve_all(config.get("connections", {}))
    pipeline_name = str(config.get("pipelineName", "learning_pipeline"))

    for t in config.get("targets", []):
        t_type = str(t.get("type", "")).lower()
        if t_type == "postgres":
            pg_conn = connections.get("postgres", {})
            conn_manager.assert_egress(
                pipeline_name,
                host=str(pg_conn.get("host", "localhost")),
                port=int(pg_conn.get("port", 5432)),
            )
            _write_postgres(rows, connections.get("postgres", {}), t.get("table", "events"), dry_run)
        elif t_type == "cassandra":
            cs_conn = connections.get("cassandra", {})
            hosts = cs_conn.get("hosts") or [cs_conn.get("host", "127.0.0.1")]
            conn_manager.assert_egress(
                pipeline_name,
                host=str(hosts[0]),
                port=int(cs_conn.get("port", 9042)),
            )
            _write_cassandra(
                rows,
                connections.get("cassandra", {}),
                t.get("keyspace", "learning"),
                t.get("table", "events"),
                dry_run,
            )

    logging.info("Batch pipeline finished rows=%d", len(rows))
    return 0


def _read_jsonl_dir_once(source_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for p in sorted(source_dir.glob("*.jsonl")):
        with p.open("r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def run_streaming(config: dict[str, Any], dry_run: bool) -> int:
    source_cfg = config["source"]
    poll = int(config.get("streaming", {}).get("poll_interval_sec", 2))
    max_batches = int(config.get("streaming", {}).get("max_batches", 2))
    batch_size = int(config.get("streaming", {}).get("batch_size", 25))

    conn_manager = ConnectionManager()
    connections = conn_manager.resolve_all(config.get("connections", {}))
    pipeline_name = str(config.get("pipelineName", "learning_pipeline"))
    all_records = _read_source_records(source_cfg)

    for batch_no in range(1, max_batches + 1):
        start = (batch_no - 1) * batch_size
        end = start + batch_size
        rows = all_records[start:end] if all_records else []
        if not rows:
            rows = all_records
        rows = _apply_transforms(rows, config.get("transforms", []))

        for t in config.get("targets", []):
            t_type = str(t.get("type", "")).lower()
            if t_type == "postgres":
                pg_conn = connections.get("postgres", {})
                conn_manager.assert_egress(
                    pipeline_name,
                    host=str(pg_conn.get("host", "localhost")),
                    port=int(pg_conn.get("port", 5432)),
                )
                _write_postgres(rows, connections.get("postgres", {}), t.get("table", "events_stream"), dry_run)
            elif t_type == "cassandra":
                cs_conn = connections.get("cassandra", {})
                hosts = cs_conn.get("hosts") or [cs_conn.get("host", "127.0.0.1")]
                conn_manager.assert_egress(
                    pipeline_name,
                    host=str(hosts[0]),
                    port=int(cs_conn.get("port", 9042)),
                )
                _write_cassandra(
                    rows,
                    connections.get("cassandra", {}),
                    t.get("keyspace", "learning"),
                    t.get("table", "events_stream"),
                    dry_run,
                )

        logging.info("Streaming micro-batch=%d rows=%d", batch_no, len(rows))
        if batch_no < max_batches:
            time.sleep(poll)

    logging.info("Streaming pipeline finished batches=%d", max_batches)
    return 0


def main() -> int:
    _setup_logger()
    p = argparse.ArgumentParser(description="Local source batch/streaming runner to Postgres and Cassandra")
    p.add_argument("--config", required=True, help="Path to pipeline config json")
    p.add_argument("--dry-run", action="store_true", help="Do not write to targets")
    args = p.parse_args()

    cfg = _normalize_pipeline_config(_load_json(Path(args.config)))
    mode = str(cfg.get("mode", "batch")).lower()

    if mode == "batch":
        return run_batch(cfg, dry_run=args.dry_run)
    if mode == "streaming":
        return run_streaming(cfg, dry_run=args.dry_run)
    raise ValueError(f"Unsupported mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())

