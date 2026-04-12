"""
Schema Validator
================
Validates pipeline JSON configs and data records.

Use this to:
  1. Catch config mistakes before running the pipeline
  2. Validate individual data records against a JSON Schema
  3. Run a full check across all environments at once

Quick usage
-----------
    from schema_validator import validate_all_envs
    from pathlib import Path
    validate_all_envs(Path("etl-project"))
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

VALID_TRANSFORMATION_TYPES = {
    "SOURCE", "EXPRESSION", "FILTER", "LOOKUP",
    "JOIN", "CACHE", "PARTITIONER", "TARGET",
}
VALID_TARGET_SYSTEMS  = {"API", "FILE", "PARQUET", "JSON", "DELTA", "DB", "JDBC", "POSTGRES"}
VALID_PROCESS_TYPES   = {"BATCH", "STREAMING"}
VALID_ENVIRONMENTS    = {"dev", "it", "uat", "prod"}


class SchemaValidator:
    """
    Validates ETL configs and data records.

    Parameters
    ----------
    schema_path : path to schema/schema.json (optional).
                  If supplied, validate_record() checks field types and required fields.
    """

    def __init__(self, schema_path: str | Path | None = None) -> None:
        self._schema: dict[str, Any] = {}
        if schema_path:
            p = Path(schema_path)
            if p.exists():
                with p.open("r", encoding="utf-8") as f:
                    self._schema = json.load(f)
                logger.info("Schema loaded from %s", p)

    # ── Config validators ─────────────────────────────────────────────────────

    def validate_global_config(self, cfg: dict[str, Any]) -> None:
        """Check that global.json has the required keys and valid values."""
        gi = cfg.get("generalInfo", {})
        self._require(gi, ["applicationName", "processType", "logLevel"], "generalInfo")
        if gi.get("processType") not in VALID_PROCESS_TYPES:
            raise ValueError(
                f"processType must be one of {VALID_PROCESS_TYPES}, got: {gi.get('processType')}"
            )
        logger.info("global.json  ✓")

    def validate_pipeline_config(self, cfg: dict[str, Any]) -> None:
        """Check pipeline.json structure, step ordering, and transformation types."""
        gi = cfg.get("generalInfo", {})
        self._require(gi, ["appName", "processType"], "generalInfo")

        t_list = cfg.get("mainFlow", [{}])[0].get("transformationList", [])
        if not t_list:
            raise ValueError("transformationList must not be empty")

        src = tgt = 0
        for t in t_list:
            ti = t.get("generalInfo", {})
            self._require(ti, ["transformationName", "transformationType", "order"],
                          "transformation.generalInfo")
            kind = ti["transformationType"].upper()
            if kind not in VALID_TRANSFORMATION_TYPES:
                raise ValueError(f"Unknown transformationType: {kind!r}")
            if kind == "SOURCE": src += 1
            if kind == "TARGET":
                tgt += 1
                sys = t.get("props", {}).get("targetSystem", "").upper()
                if sys and sys not in VALID_TARGET_SYSTEMS:
                    raise ValueError(f"Unknown targetSystem: {sys!r}")

        if src == 0:
            raise ValueError("Pipeline must have at least one SOURCE step")
        if tgt == 0:
            raise ValueError("Pipeline must have at least one TARGET step")
        logger.info("pipeline.json  ✓  (%d steps)", len(t_list))

    def validate_connections_config(self, cfg: dict[str, Any]) -> None:
        if not cfg:
            raise ValueError("connections.json is empty")
        logger.info("connections.json  ✓  (%d connections)", len(cfg))

    # ── Record validators ─────────────────────────────────────────────────────

    def validate_record(self, record: dict[str, Any]) -> bool:
        """
        Validate one data record against the JSON Schema.
        Returns True if valid, raises ValueError if not.
        """
        if not self._schema:
            return True
        required = self._schema.get("required", [])
        missing  = [f for f in required if f not in record]
        if missing:
            raise ValueError(f"Record missing required fields: {missing}")
        for field, spec in self._schema.get("properties", {}).items():
            if field in record and record[field] is not None:
                expected = spec.get("type", [])
                if isinstance(expected, str):
                    expected = [expected]
                expected = [t for t in expected if t != "null"]
                if not self._type_ok(record[field], expected):
                    raise ValueError(
                        f"Field '{field}' expected {expected}, got {type(record[field]).__name__}"
                    )
        return True

    def validate_batch(self, records: list[dict]) -> tuple[int, int]:
        """Validate a list of records. Returns (valid_count, invalid_count)."""
        ok = bad = 0
        for rec in records:
            try:
                self.validate_record(rec)
                ok += 1
            except ValueError as e:
                logger.warning("Invalid record: %s", e)
                bad += 1
        return ok, bad

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _require(obj: dict, keys: list[str], section: str) -> None:
        missing = [k for k in keys if k not in obj]
        if missing:
            raise ValueError(f"{section} missing required keys: {missing}")

    @staticmethod
    def _type_ok(value: Any, expected: list[str]) -> bool:
        MAP = {"string": str, "integer": int, "number": (int, float),
               "boolean": bool, "object": dict, "array": list}
        return any(isinstance(value, MAP[t]) for t in expected if t in MAP) or not expected


def validate_all_envs(base_dir: Path, environments: list[str] | None = None) -> None:
    """
    Run config validation for every environment folder.

    Parameters
    ----------
    base_dir     : root of the ETL project (contains appconfig/, schema/, etc.)
    environments : list of env names to check; defaults to all valid environments
    """
    envs      = environments or list(VALID_ENVIRONMENTS)
    validator = SchemaValidator(base_dir / "schema" / "schema.json")
    errors: list[str] = []

    for env in envs:
        env_dir = base_dir / "appconfig" / env
        if not env_dir.exists():
            logger.debug("Skipping env '%s' (folder not found)", env)
            continue
        try:
            with (env_dir / "global.json").open("r", encoding="utf-8-sig")      as f: g = json.load(f)
            with (env_dir / "pipeline.json").open("r", encoding="utf-8-sig")    as f: p = json.load(f)
            with (env_dir / "connections.json").open("r", encoding="utf-8-sig") as f: c = json.load(f)
            validator.validate_global_config(g)
            validator.validate_pipeline_config(p)
            validator.validate_connections_config(c)
            logger.info("env=%-6s  ✓", env)
        except Exception as exc:
            errors.append(f"env={env}: {exc}")
            logger.error("env=%-6s  ✗  %s", env, exc)

    if errors:
        raise ValueError(
            f"Validation failed for {len(errors)} environment(s):\n" + "\n".join(errors)
        )
    logger.info("All environments validated successfully")
