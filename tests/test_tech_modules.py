from pipelines.tech import (
    get_airflow_commands,
    get_cassandra_notes,
    get_docker_commands,
    get_path_setup_commands,
    get_postgres_notes,
    get_runner_commands,
)


def test_tech_modules_return_values() -> None:
    assert get_path_setup_commands()
    assert get_docker_commands()
    assert get_airflow_commands()
    assert get_runner_commands()
    assert "POSTGRES_HOST" in get_postgres_notes()
    assert "CASSANDRA_HOST" in get_cassandra_notes()

