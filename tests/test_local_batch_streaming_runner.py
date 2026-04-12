from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "etl-project" / "spark-job" / "local_batch_streaming_runner.py"

BATCH_CONFIGS = [
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_batch_local_pg_cassandra.json",
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_bank_cards_batch_csv_pg_cassandra.json",
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_bank_cards_pipe_batch_pg_cassandra.json",
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_city_trips_batch_pg_cassandra.json",
]

STREAMING_CONFIGS = [
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_streaming_local_pg_cassandra.json",
    ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_bank_cards_streaming_json_pg_cassandra.json",
]


@pytest.mark.parametrize("cfg", BATCH_CONFIGS)
def test_batch_runner_dry_run(cfg: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--config", str(cfg), "--dry-run"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "Batch pipeline finished" in (result.stdout + result.stderr)


@pytest.mark.parametrize("cfg", STREAMING_CONFIGS)
def test_streaming_runner_dry_run(cfg: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--config", str(cfg), "--dry-run"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "Streaming pipeline finished" in (result.stdout + result.stderr)
