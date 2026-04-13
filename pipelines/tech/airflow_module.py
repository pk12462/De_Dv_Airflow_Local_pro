from __future__ import annotations


def get_airflow_commands() -> list[str]:
    """Airflow-specific commands used for health and DAG operations."""
    return [
        'Set-Location "C:\\Users\\PAVAN\\Local_pro\\De_Dv_Airflow_Local_pro\\airflow"',
        "docker compose ps",
        "docker compose logs --no-color airflow-webserver",
        "docker compose logs --no-color airflow-scheduler",
        "# Airflow UI: http://localhost:8080",
    ]

