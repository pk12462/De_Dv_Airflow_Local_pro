from __future__ import annotations


def get_path_setup_commands() -> list[str]:
    """Workspace/path setup commands used during implementation and tests."""
    return [
        'Set-Location "C:\\Users\\PAVAN\\Local_pro\\De_Dv_Airflow_Local_pro"',
        'Set-Location "C:\\Users\\PAVAN\\Local_pro\\De_Dv_Airflow_Local_pro\\airflow"',
        "$env:CARID=\"8999\"",
        "$env:POSTGRES_HOST=\"localhost\"",
        "$env:CASSANDRA_HOST=\"127.0.0.1\"",
    ]

