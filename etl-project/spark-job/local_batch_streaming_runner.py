from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any


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
            now = datetime.utcnow().isoformat()
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


def _resolve_source_path(path_value: str) -> Path:
    source_path = Path(path_value)
    if source_path.is_absolute():
        return source_path

    candidates = [source_path, Path.cwd() / source_path, Path("/opt/airflow") / source_path]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    # Fallback for clearer container behavior when running from temp dirs.
    return Path("/opt/airflow") / source_path


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
    connections = config.get("connections", {})

    for t in config.get("targets", []):
        t_type = str(t.get("type", "")).lower()
        if t_type == "postgres":
            _write_postgres(rows, connections.get("postgres", {}), t.get("table", "events"), dry_run)
        elif t_type == "cassandra":
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

    connections = config.get("connections", {})
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
                _write_postgres(rows, connections.get("postgres", {}), t.get("table", "events_stream"), dry_run)
            elif t_type == "cassandra":
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

    cfg = _load_json(Path(args.config))
    mode = str(cfg.get("mode", "batch")).lower()

    if mode == "batch":
        return run_batch(cfg, dry_run=args.dry_run)
    if mode == "streaming":
        return run_streaming(cfg, dry_run=args.dry_run)
    raise ValueError(f"Unsupported mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())

