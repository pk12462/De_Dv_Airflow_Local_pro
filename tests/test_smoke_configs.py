from pathlib import Path

import yaml


def test_required_configs_exist_and_parse():
    root = Path(__file__).resolve().parents[1]
    required = [
        root / "pipelines" / "configs" / "app_connection_config.yaml",
        root / "pipelines" / "configs" / "streaming_pipeline_config.yaml",
        root / "pipelines" / "configs" / "batch_pipeline_config.yaml",
        root / "pipelines" / "egress" / "egress_rules.yaml",
        root / "pipelines" / "shield" / "shield_values.yaml",
        root / "airflow" / "docker-compose.yaml",
    ]

    for path in required:
        assert path.exists(), f"Missing file: {path}"
        with path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        assert loaded is not None, f"YAML did not parse: {path}"

