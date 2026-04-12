from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "etl-project" / "spark-job" / "local_batch_streaming_runner.py"
BATCH_CFG = ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_batch_local_pg_cassandra.json"
STREAM_CFG = ROOT / "etl-project" / "appconfig" / "dev" / "pipeline_streaming_local_pg_cassandra.json"


def test_batch_runner_dry_run() -> None:
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--config", str(BATCH_CFG), "--dry-run"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "Batch pipeline finished" in (result.stdout + result.stderr)


def test_streaming_runner_dry_run() -> None:
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--config", str(STREAM_CFG), "--dry-run"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "Streaming pipeline finished" in (result.stdout + result.stderr)

