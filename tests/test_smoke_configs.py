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
        root / "scripts" / "windows" / "start-airflow.ps1",
        root / "scripts" / "windows" / "wait-airflow-ready.ps1",
        root / "scripts" / "windows" / "unpause-pipeline-dags.ps1",
        root / "scripts" / "windows" / "install-airflow-autostart.ps1",
        root / "scripts" / "windows" / "stop-airflow.ps1",
        root / "airflow" / "docker-compose.cloudflare.yaml",
        root / "airflow" / ".env.cloudflare.example",
        root / "airflow" / "CLOUDFLARE_TUNNEL_SETUP.md",
        root / "scripts" / "windows" / "start-airflow-cloudflare.ps1",
        root / "scripts" / "windows" / "stop-airflow-cloudflare.ps1",
        root / "scripts" / "windows" / "preflight-etl.ps1",
        root / "scripts" / "cloudflare-build.sh",
        root / "scripts" / "windows" / "cloudflare-build.ps1",
    ]

    for path in required:
        assert path.exists(), f"Missing file: {path}"
        if path.suffix in {".yaml", ".yml"}:
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
            assert loaded is not None, f"YAML did not parse: {path}"


def test_airflow_compose_always_on_settings():
    root = Path(__file__).resolve().parents[1]
    compose_path = root / "airflow" / "docker-compose.yaml"
    with compose_path.open("r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    assert services.get("postgres", {}).get("restart") == "unless-stopped"
    assert services.get("airflow-webserver", {}).get("restart") == "unless-stopped"
    assert services.get("airflow-scheduler", {}).get("restart") == "unless-stopped"
    assert "healthcheck" in services.get("airflow-scheduler", {})


