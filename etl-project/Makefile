# ─────────────────────────────────────────────────────────────────────────────
# Makefile — ETL Learning Project
# Usage: make <target> [ENV=dev|it|uat|prod]
# ─────────────────────────────────────────────────────────────────────────────
ENV ?= dev
BASE_DIR = etl-project
RUNNER = $(BASE_DIR)/spark-job/pyspark.py
PYTHON = python

.PHONY: help validate dry-run run local-batch local-streaming bank-cards-batch bank-cards-pipe-batch bank-cards-stream city-trips-batch lint test airflow-up airflow-down clean

help:
	@echo "ETL Learning Makefile"
	@echo ""
	@echo "  make validate         Validate all JSON configs for ENV=$(ENV)"
	@echo "  make dry-run          Dry-run pipeline for ENV=$(ENV)"
	@echo "  make run              Full pipeline run for ENV=$(ENV)"
	@echo "  make lint             Lint all Python files"
	@echo "  make test             Run all tests"
	@echo "  make airflow-up       Start Airflow via docker-compose"
	@echo "  make airflow-down     Stop Airflow"
	@echo "  make clean            Remove __pycache__ and temp files"
	@echo "  make local-batch      Run local CSV -> PostgreSQL + Cassandra (dry-run)"
	@echo "  make local-streaming  Run local JSONL stream -> PostgreSQL + Cassandra (dry-run)"
	@echo "  make bank-cards-batch Run bank_data.csv -> PostgreSQL + Cassandra (dry-run)"
	@echo "  make bank-cards-pipe-batch Run bank_data.txt (pipe) -> PostgreSQL + Cassandra (dry-run)"
	@echo "  make bank-cards-stream Run bank_data.json stream -> PostgreSQL + Cassandra (dry-run)"
	@echo "  make city-trips-batch Run trip_data.csv -> PostgreSQL + Cassandra (dry-run)"

validate:
	$(PYTHON) -c "\
import sys; sys.path.insert(0,'$(BASE_DIR)/spark-job'); \
from schema_validator import validate_all_envs; from pathlib import Path; \
validate_all_envs(Path('$(BASE_DIR)'), environments=['$(ENV)'])"

dry-run:
	$(PYTHON) $(RUNNER) --env $(ENV) --base-dir $(BASE_DIR) --dry-run

run:
	$(PYTHON) $(RUNNER) --env $(ENV) --base-dir $(BASE_DIR)

lint:
	python -m py_compile etl-project/spark-job/pyspark.py
	python -m py_compile etl-project/spark-job/schema_validator.py
	@echo "Lint passed"

test:
	$(PYTHON) -m pytest tests/test_etl_project_full.py tests/test_etl_project_setup.py -v -o addopts=

airflow-up:
	cd airflow && docker-compose up -d

airflow-down:
	cd airflow && docker-compose down

clean:
	find . -type d -name __pycache__ | xargs rm -rf 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -rf _pdf_pages 2>/dev/null; true

local-batch:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_batch_local_pg_cassandra.json --dry-run

local-streaming:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_streaming_local_pg_cassandra.json --dry-run

bank-cards-batch:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_bank_cards_batch_csv_pg_cassandra.json --dry-run

bank-cards-pipe-batch:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_bank_cards_pipe_batch_pg_cassandra.json --dry-run

bank-cards-stream:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_bank_cards_streaming_json_pg_cassandra.json --dry-run

city-trips-batch:
	$(PYTHON) $(BASE_DIR)/spark-job/local_batch_streaming_runner.py --config $(BASE_DIR)/appconfig/dev/pipeline_city_trips_batch_pg_cassandra.json --dry-run
