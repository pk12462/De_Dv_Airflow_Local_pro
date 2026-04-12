from pathlib import Path
import json
import subprocess
import sys


def test_etl_project_files_and_dry_run():
    root = Path(__file__).resolve().parents[1]
    base = root / "etl-project"

    required = [
        base / "appconfig" / "dev" / "global.json",
        base / "appconfig" / "dev" / "connections.json",
        base / "appconfig" / "dev" / "pipeline.json",
        base / "deployment" / "dev.yaml",
        base / "deployment" / "egress.yaml",
        base / "queries" / "source_query.sql",
        base / "spark-job" / "pyspark.py",
    ]
    for p in required:
        assert p.exists(), f"Missing: {p}"

    with (base / "appconfig" / "dev" / "pipeline.json").open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    assert "mainFlow" in cfg

    cmd = [
        sys.executable,
        str(base / "spark-job" / "pyspark.py"),
        "--env",
        "dev",
        "--dry-run",
    ]
    result = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr + "\n" + result.stdout

