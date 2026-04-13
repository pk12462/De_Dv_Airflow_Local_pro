from __future__ import annotations

from pipelines.tech import (
    get_airflow_commands,
    get_cassandra_notes,
    get_docker_commands,
    get_path_setup_commands,
    get_postgres_notes,
    get_runner_commands,
)


def _print_section(title: str, items: list[str]) -> None:
    print(f"\n## {title}")
    for cmd in items:
        print(cmd)


def main() -> None:
    _print_section("Path Setup", get_path_setup_commands())
    _print_section("Docker", get_docker_commands())
    _print_section("Airflow", get_airflow_commands())
    _print_section("Runner Validation", get_runner_commands())

    print("\n## Postgres Env Keys")
    for key, desc in get_postgres_notes().items():
        print(f"{key}: {desc}")

    print("\n## Cassandra Env Keys")
    for key, desc in get_cassandra_notes().items():
        print(f"{key}: {desc}")


if __name__ == "__main__":
    main()

