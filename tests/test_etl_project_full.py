"""
ETL Project Config Validation Test
====================================
Tests for all environments (dev, it, uat), the pipeline engine dry-run,
and the schema validator.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT     = Path(__file__).resolve().parents[1]
ETL_BASE = ROOT / "etl-project"
RUNNER   = ETL_BASE / "spark-job" / "pyspark.py"


# ── File presence ─────────────────────────────────────────────────────────────

def test_all_required_files_exist():
    required = [
        ETL_BASE / "appconfig" / "dev" / "global.json",
        ETL_BASE / "appconfig" / "dev" / "connections.json",
        ETL_BASE / "appconfig" / "dev" / "pipeline.json",
        ETL_BASE / "appconfig" / "it"  / "global.json",
        ETL_BASE / "appconfig" / "it"  / "connections.json",
        ETL_BASE / "appconfig" / "it"  / "pipeline.json",
        ETL_BASE / "appconfig" / "uat" / "global.json",
        ETL_BASE / "appconfig" / "uat" / "connections.json",
        ETL_BASE / "appconfig" / "uat" / "pipeline.json",
        ETL_BASE / "queries"   / "source_query.sql",
        ETL_BASE / "queries"   / "lookup_risk_ref.sql",
        ETL_BASE / "schema"    / "schema.json",
        ETL_BASE / "deployment"/ "dev.yaml",
        ETL_BASE / "deployment"/ "it.yaml",
        ETL_BASE / "deployment"/ "uat.yaml",
        ETL_BASE / "deployment"/ "prod.yaml",
        RUNNER,
    ]
    for p in required:
        assert p.exists(), f"Missing: {p.relative_to(ROOT)}"


# ── JSON parse ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("env", ["dev", "it", "uat"])
@pytest.mark.parametrize("config", ["global.json", "connections.json", "pipeline.json"])
def test_json_configs_parse(env, config):
    path = ETL_BASE / "appconfig" / env / config
    if not path.exists():
        pytest.skip(f"Not present: {path}")
    with path.open(encoding="utf-8-sig") as f:
        data = json.load(f)
    assert data, f"{path} parsed as empty"


# ── Schema validator ──────────────────────────────────────────────────────────

def test_schema_validator_all_envs():
    sys.path.insert(0, str(ETL_BASE / "spark-job"))
    from schema_validator import validate_all_envs
    validate_all_envs(ETL_BASE, environments=["dev", "it", "uat"])


def test_schema_validator_record_valid():
    sys.path.insert(0, str(ETL_BASE / "spark-job"))
    from schema_validator import SchemaValidator
    v = SchemaValidator(ETL_BASE / "schema" / "schema.json")
    assert v.validate_record({"user_id": "U001", "event_type": "purchase", "event_date": "2026-04-12"})


def test_schema_validator_record_missing_required():
    sys.path.insert(0, str(ETL_BASE / "spark-job"))
    from schema_validator import SchemaValidator
    v = SchemaValidator(ETL_BASE / "schema" / "schema.json")
    with pytest.raises(ValueError, match="missing required"):
        v.validate_record({"amount": 99.9})


# ── Dry-run execution ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("env", ["dev", "it"])
def test_pipeline_dry_run(env):
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--env", env, "--dry-run", "--base-dir", str(ETL_BASE)],
        capture_output=True, text=True, cwd=str(ROOT),
    )
    assert result.returncode == 0, (
        f"Pipeline dry-run FAILED for env={env}\nSTDOUT:{result.stdout}\nSTDERR:{result.stderr}"
    )
    assert "Pipeline COMPLETE" in result.stderr or "Pipeline COMPLETE" in result.stdout


# ── Pipeline config structure ─────────────────────────────────────────────────

def test_pipeline_has_source_and_target():
    with (ETL_BASE / "appconfig" / "dev" / "pipeline.json").open() as f:
        cfg = json.load(f)
    t_list = cfg["mainFlow"][0]["transformationList"]
    types = [t["generalInfo"]["transformationType"] for t in t_list]
    assert "SOURCE" in types
    assert "TARGET" in types


def test_pipeline_orders_are_unique():
    with (ETL_BASE / "appconfig" / "dev" / "pipeline.json").open() as f:
        cfg = json.load(f)
    orders = [t["generalInfo"]["order"] for t in cfg["mainFlow"][0]["transformationList"]]
    assert len(orders) == len(set(orders)), "Duplicate order values found"


def test_pipeline_uat_has_lookup():
    with (ETL_BASE / "appconfig" / "uat" / "pipeline.json").open() as f:
        cfg = json.load(f)
    t_list = cfg["mainFlow"][0]["transformationList"]
    types = [t["generalInfo"]["transformationType"] for t in t_list]
    assert "LOOKUP" in types, "UAT pipeline should include a LOOKUP enrichment step"

