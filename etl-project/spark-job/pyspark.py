"""
DataFlow ETL Runner
====================
A config-driven batch ETL engine built for learning Data Engineering.

How it works
------------
1. Load three JSON config files from  appconfig/<env>/
       global.json      →  app name, Spark settings, logging
       connections.json →  DB / API / storage connection details
       pipeline.json    →  ordered list of transformation steps

2. Execute each step in order:

       SOURCE      – read data (JDBC, file, or sample)
       EXPRESSION  – rename / derive columns
       FILTER      – drop rows that fail a condition
       LOOKUP      – enrich by joining a reference dataset
       JOIN        – join two named in-memory DataFrames
       CACHE       – cache a DataFrame for reuse
       PARTITIONER – repartition or coalesce for performance
       TARGET      – write to REST API, file, or database

3. Log a checkpoint after every step (with optional PII masking).

Run it
------
    python etl-project/spark-job/pyspark.py --env dev --dry-run
    python etl-project/spark-job/pyspark.py --env uat --run-date 2026-04-12
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any


# ── Config loader ─────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict[str, Any]:
    # utf-8-sig handles files written with a UTF-8 BOM (common on Windows)
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


# ── Logger ────────────────────────────────────────────────────────────────────

def configure_logger(level: str, app_name: str = "dataflow-etl") -> None:
    fmt = f"%(asctime)s | {app_name} | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


# ── Spark session (optional — falls back to in-memory dicts) ─────────────────

def build_spark(global_cfg: dict[str, Any]) -> Any | None:
    """
    Create a SparkSession from global.json settings.
    Returns None if PySpark is not installed — the engine
    then uses plain Python dicts (great for learning locally).
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logging.warning("PySpark not installed — running in dict-mode (no Spark needed).")
        return None

    sc   = global_cfg.get("sparkConfig", {})
    name = global_cfg.get("generalInfo", {}).get("applicationName", "dataflow-etl")
    return (
        SparkSession.builder
        .appName(name)
        .master(sc.get("master", os.getenv("SPARK_MASTER_URL", "local[*]")))
        .config("spark.executor.memory", sc.get("executorMemory", "2g"))
        .config("spark.driver.memory",   sc.get("driverMemory",   "1g"))
        .getOrCreate()
    )


# ── Sample source data (used in dry-run and local learning) ──────────────────

def sample_source_records() -> list[dict[str, Any]]:
    """Returns three dummy rows so you can run the pipeline without any database."""
    return [
        {"user_id": "U001", "event_type": "purchase", "amount": 120.50,
         "event_date": "2026-04-11", "status": "COMPLETED"},
        {"user_id": "U002", "event_type": "refund",   "amount": 35.00,
         "event_date": "2026-04-11", "status": "COMPLETED"},
        {"user_id": None,   "event_type": "login",    "amount": 0.0,
         "event_date": "2026-04-11", "status": "INCOMPLETE"},
    ]


# ── Transformation helpers (dict-mode, no Spark required) ────────────────────

def apply_expression_records(records: list[dict], rules: list[str]) -> list[dict]:
    """
    Apply expression rules to plain Python dicts.
    Rule format: "source_col as target_col"
    Special value: "current_timestamp()" → UTC ISO timestamp
    """
    out = []
    for rec in records:
        row: dict[str, Any] = {}
        for rule in rules:
            parts = [p.strip() for p in rule.split(" as ")]
            if len(parts) == 2:
                src, dst = parts
                row[dst] = datetime.utcnow().isoformat() if src == "current_timestamp()" else rec.get(src)
            else:
                row[rule] = rec.get(rule)
        out.append(row)
    return out


def apply_filter_records(records: list[dict], condition: str) -> list[dict]:
    """
    Apply a simple filter condition to plain Python dicts.
    Supports: '<col> IS NOT NULL'
    """
    cond_upper = condition.strip().upper()
    if "IS NOT NULL" in cond_upper:
        col = condition.strip().split()[0]
        return [r for r in records if r.get(col) is not None]
    return records


# ── Spark-mode transformation helpers ─────────────────────────────────────────

def apply_expression_spark(df: Any, rules: list[str]) -> Any:
    from pyspark.sql.functions import expr
    return df.select(*[expr(r) for r in rules])


# ── Checkpoint / audit logger ─────────────────────────────────────────────────

class CheckpointLogger:
    """
    Records an audit entry after every pipeline step.

    Features
    --------
    * Captures record count per step
    * Optionally captures a payload sample (capturePayload=true in global.json)
    * Hashes columns marked hashingRequired=true before logging
      (so PII like user_id is never written to logs in plain text)
    """

    def __init__(self, app_name: str, logger_cfg: dict) -> None:
        self.app_name       = app_name
        self.capture        = logger_cfg.get("checkPointLog", {}).get("capturePayload", False)
        self.hash_cols      = {t["keyColName"] for t in logger_cfg.get("logTags", [])
                               if t.get("hashingRequired")}
        self._events: list[dict] = []

    def log_step(self, step: str, t_type: str,
                 count: int | None = None, sample: list | None = None) -> None:
        import hashlib
        entry: dict[str, Any] = {
            "ts": datetime.utcnow().isoformat(),
            "step": step, "type": t_type, "count": count,
        }
        if self.capture and sample:
            masked = []
            for rec in sample[:2]:
                r = dict(rec)
                for col in self.hash_cols:
                    if col in r and r[col]:
                        r[col] = hashlib.sha256(str(r[col]).encode()).hexdigest()[:16]
                masked.append(r)
            entry["payload_sample"] = masked
        self._events.append(entry)
        logging.info("[CHECKPOINT] %-32s %-14s count=%s", step, t_type, count)

    def summary(self) -> dict:
        return {"app": self.app_name, "total_steps": len(self._events), "events": self._events}


# ── Target writers ────────────────────────────────────────────────────────────

def write_to_rest_api(data: Any, props: dict, conn_cfg: dict,
                      dry_run: bool, ckpt: CheckpointLogger) -> None:
    """POST each record as JSON to a REST API endpoint."""
    api  = conn_cfg.get("api", {})
    url  = api.get("url", props.get("url", "http://localhost:8000/ingest"))
    method  = str(props.get("method", "POST")).upper()
    headers = {**{"Content-Type": "application/json"}, **props.get("headers", {})}

    rows    = data if isinstance(data, list) else [r.asDict() for r in data.collect()]
    count   = len(rows)
    ckpt.log_step("TARGET_API", "TARGET", count, rows[:2])

    if dry_run:
        logging.info("DRY-RUN | %s %s | %d rows | sample: %s", method, url, count, rows[:2])
        return

    import urllib.request, json as _json
    for row in rows:
        body = _json.dumps(row, default=str).encode()
        req  = urllib.request.Request(url, data=body, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"API error {resp.status} for row {row}")


def write_to_file(data: Any, props: dict, conn_cfg: dict,
                  dry_run: bool, ckpt: CheckpointLogger) -> None:
    """Write records to a local Parquet / JSON file (or ADLS / S3 path)."""
    storage = conn_cfg.get("storage", {})
    path    = props.get("path", storage.get("outputPath", "./output/data"))
    fmt     = props.get("format", "parquet").lower()
    rows    = data if isinstance(data, list) else [r.asDict() for r in data.collect()]
    ckpt.log_step("TARGET_FILE", "TARGET", len(rows))

    if dry_run:
        logging.info("DRY-RUN | FILE path=%s fmt=%s | %d rows", path, fmt, len(rows))
        return

    import pathlib, json as _json
    out = pathlib.Path(path)
    out.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        (out / "output.json").write_text(_json.dumps(rows, default=str), encoding="utf-8")
    elif not isinstance(data, list):
        data.write.mode("append").parquet(str(out))
    else:
        (out / "output.json").write_text(_json.dumps(rows, default=str), encoding="utf-8")


def write_to_database(data: Any, props: dict, conn_cfg: dict,
                      dry_run: bool, ckpt: CheckpointLogger) -> None:
    """Write records to a relational database via JDBC / SQLAlchemy."""
    db_key  = props.get("connectionName", "database")
    db_url  = conn_cfg.get(db_key, {}).get("url", os.getenv("DATABASE_URL", ""))
    table   = props.get("table", "events")
    rows    = data if isinstance(data, list) else [r.asDict() for r in data.collect()]
    ckpt.log_step("TARGET_DB", "TARGET", len(rows))

    if dry_run:
        logging.info("DRY-RUN | DB url=%s table=%s | %d rows", db_url, table, len(rows))
        return

    if isinstance(data, list):
        from sqlalchemy import create_engine
        import pandas as pd
        engine = create_engine(db_url)
        pd.DataFrame(rows).to_sql(table, engine, if_exists="append", index=False)
    else:
        (data.write.format("jdbc")
         .option("url", db_url).option("dbtable", table).mode("append").save())


# ── Main pipeline executor ────────────────────────────────────────────────────

def execute_pipeline(base_dir: Path, env: str,
                     dry_run: bool, run_date: str) -> int:
    env_dir    = base_dir / "appconfig" / env
    global_cfg = load_json(env_dir / "global.json")
    conn_cfg   = load_json(env_dir / "connections.json")
    pipeline   = load_json(env_dir / "pipeline.json")

    gi       = global_cfg.get("generalInfo", {})
    app_name = gi.get("applicationName", "dataflow-etl")
    configure_logger(gi.get("logLevel", "INFO"), app_name)
    logging.info("=" * 60)
    logging.info("Pipeline START  env=%-6s  dry_run=%s  date=%s", env, dry_run, run_date)
    logging.info("=" * 60)

    ckpt  = CheckpointLogger(app_name, global_cfg.get("loggerDetails", {}))
    spark = build_spark(global_cfg)
    named: dict[str, Any] = {}   # named DataFrames for JOIN / CACHE reuse
    start = time.time()

    try:
        steps = sorted(
            pipeline["mainFlow"][0]["transformationList"],
            key=lambda t: t["generalInfo"].get("order", 999),
        )
        current: Any = None

        for step in steps:
            info   = step["generalInfo"]
            t_type = info["transformationType"].upper()
            t_name = info["transformationName"]
            props  = step.get("props", {})
            logging.info("  STEP %-3s | %-14s | %s", info.get("order", "?"), t_type, t_name)

            # ── SOURCE ────────────────────────────────────────────────────────
            if t_type == "SOURCE":
                rows    = sample_source_records()
                current = rows if spark is None else spark.createDataFrame(rows)
                ckpt.log_step(t_name, t_type, len(rows), rows)

            # ── EXPRESSION ───────────────────────────────────────────────────
            elif t_type == "EXPRESSION":
                rules = props.get("rules", [])
                current = (apply_expression_records(current, rules)
                           if isinstance(current, list)
                           else apply_expression_spark(current, rules))
                ckpt.log_step(t_name, t_type)

            # ── FILTER ────────────────────────────────────────────────────────
            elif t_type == "FILTER":
                cond    = props.get("condition", "1=1")
                current = (apply_filter_records(current, cond)
                           if isinstance(current, list)
                           else current.filter(cond))
                count   = len(current) if isinstance(current, list) else current.count()
                ckpt.log_step(t_name, t_type, count)

            # ── LOOKUP ────────────────────────────────────────────────────────
            elif t_type == "LOOKUP":
                key     = props.get("joinKey", "user_id")
                j_type  = props.get("joinType", "LEFT").lower()
                sel     = props.get("selectCols", [])
                # Sample reference table (replace with real JDBC read in production)
                ref = [{"user_id": "U001", "tier": "GOLD",   "region": "APAC"},
                       {"user_id": "U002", "tier": "SILVER", "region": "EMEA"}]
                if isinstance(current, list):
                    ref_map = {str(r[key]): r for r in ref}
                    result  = []
                    for rec in current:
                        merged = {**rec, **(ref_map.get(str(rec.get(key)), {}))}
                        result.append({k: merged.get(k) for k in sel} if sel else merged)
                    current = result
                else:
                    ref_df  = spark.createDataFrame(ref)
                    current = current.join(ref_df, on=key, how=j_type)
                    if sel:
                        current = current.select(*sel)
                ckpt.log_step(t_name, t_type)

            # ── JOIN ──────────────────────────────────────────────────────────
            elif t_type == "JOIN":
                right   = named.get(props.get("rightDataFrame", ""), [])
                key     = props.get("joinKey", "id")
                j_type  = props.get("joinType", "inner").lower()
                if isinstance(current, list):
                    r_map   = {str(r.get(key)): r for r in right}
                    current = [{**l, **(r_map.get(str(l.get(key)), {}))} for l in current]
                else:
                    current = current.join(right, on=key, how=j_type)
                ckpt.log_step(t_name, t_type)

            # ── CACHE ─────────────────────────────────────────────────────────
            elif t_type == "CACHE":
                cache_name = props.get("name", t_name)
                if not isinstance(current, list):
                    current = current.cache()
                named[cache_name] = current
                ckpt.log_step(t_name, t_type)

            # ── PARTITIONER ───────────────────────────────────────────────────
            elif t_type == "PARTITIONER":
                if not isinstance(current, list):
                    ptype   = props.get("partitionType", "REPARTITION").upper()
                    num     = int(props.get("numPartitions", 4))
                    current = (current.coalesce(num) if ptype == "COALESCE"
                               else current.repartition(num))
                ckpt.log_step(t_name, t_type)

            # ── TARGET ────────────────────────────────────────────────────────
            elif t_type == "TARGET":
                target = props.get("targetSystem", "API").upper()
                if target == "API":
                    write_to_rest_api(current, props, conn_cfg, dry_run, ckpt)
                elif target in ("FILE", "PARQUET", "JSON", "DELTA"):
                    write_to_file(current, props, conn_cfg, dry_run, ckpt)
                elif target in ("DB", "JDBC", "POSTGRES"):
                    write_to_database(current, props, conn_cfg, dry_run, ckpt)
                else:
                    raise ValueError(f"Unknown targetSystem: {target}")

            else:
                raise ValueError(f"Unknown transformation type: {t_type}")

        elapsed = round(time.time() - start, 2)
        summary = ckpt.summary()
        logging.info("=" * 60)
        logging.info("Pipeline COMPLETE | steps=%d | elapsed=%.2fs",
                     summary["total_steps"], elapsed)
        logging.info("=" * 60)
        return 0

    finally:
        if spark is not None:
            spark.stop()


# ── CLI entry point ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DataFlow ETL Runner — config-driven batch pipeline for learning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--env",      default="dev",
                   choices=["dev", "it", "uat", "prod"],
                   help="Environment (maps to appconfig/<env>/)")
    p.add_argument("--base-dir", default="etl-project",
                   help="Root folder of the ETL project (default: etl-project)")
    p.add_argument("--dry-run",  action="store_true",
                   help="Print what would happen — no writes, no external calls")
    p.add_argument("--run-date", default=None,
                   help="Logical run date YYYY-MM-DD (default: today)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    base = Path(args.base_dir)
    if not base.exists():
        logging.error("Base dir not found: %s", base)
        return 2
    run_date = args.run_date or datetime.utcnow().strftime("%Y-%m-%d")
    return execute_pipeline(base, env=args.env, dry_run=args.dry_run, run_date=run_date)


if __name__ == "__main__":
    raise SystemExit(main())

