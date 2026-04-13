from __future__ import annotations


def get_docker_commands() -> list[str]:
    """Docker/Compose commands used in this project workflow."""
    return [
        'Set-Location "C:\\Users\\PAVAN\\Local_pro\\De_Dv_Airflow_Local_pro\\airflow"',
        "docker compose ps",
        "docker compose up -d airflow-init",
        "docker compose up -d airflow-webserver airflow-scheduler",
        "docker compose logs --no-color airflow-webserver",
        "docker compose logs --no-color airflow-scheduler",
    ]

