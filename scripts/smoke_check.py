"""Small local smoke check for config and module imports."""
from pathlib import Path
import importlib


MODULES = [
    "streaming_apps",
    "batch_apps",
    "pipelines.base.pipeline_utils",
    "pipelines.egress.egress_handler",
    "pipelines.shield.shield_validator",
]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    required_paths = [
        root / "pipelines" / "configs" / "app_connection_config.yaml",
        root / "airflow" / "docker-compose.yaml",
        root / "docker" / "base" / "Dockerfile.airflow",
    ]

    for p in required_paths:
        if not p.exists():
            print(f"MISSING: {p}")
            return 1

    for mod in MODULES:
        importlib.import_module(mod)

    print("Smoke check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

