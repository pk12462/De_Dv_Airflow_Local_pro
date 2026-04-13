from __future__ import annotations


def get_postgres_notes() -> dict[str, str]:
    """PostgreSQL runtime env keys resolved by ConnectionManager."""
    return {
        "POSTGRES_HOST": "Database host",
        "POSTGRES_PORT": "Database port",
        "POSTGRES_DB": "Database name",
        "POSTGRES_USER": "Database user",
        "POSTGRES_PASSWORD": "Database password",
    }

