from __future__ import annotations


def get_cassandra_notes() -> dict[str, str]:
    """Cassandra runtime env keys resolved by ConnectionManager."""
    return {
        "CASSANDRA_HOST": "Primary contact point",
        "CASSANDRA_PORT": "CQL native transport port",
        "CASSANDRA_KEYSPACE": "Target keyspace",
        "CASSANDRA_USERNAME": "Optional username",
        "CASSANDRA_PASSWORD": "Optional password",
    }

