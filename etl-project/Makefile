# ─────────────────────────────────────────────────────────────────────────────
# Makefile — UDAIP ETL Project (cust-risk-triggers)
# Usage: make <target> [ENV=dev|it|uat|prod]
# ─────────────────────────────────────────────────────────────────────────────
ENV ?= dev
BASE_DIR = etl-project
RUNNER = $(BASE_DIR)/spark-job/pyspark.py
PYTHON = python

.PHONY: help validate dry-run run lint test airflow-up airflow-down clean

help:
	@echo "UDAIP ETL Makefile"
	@echo ""
	@echo "  make validate         Validate all JSON configs for ENV=$(ENV)"
	@echo "  make dry-run          Dry-run pipeline for ENV=$(ENV)"
	@echo "  make run              Full pipeline run for ENV=$(ENV)"
	@echo "  make lint             Lint all Python files"
	@echo "  make test             Run all tests"
	@echo "  make airflow-up       Start Airflow via docker-compose"
	@echo "  make airflow-down     Stop Airflow"
	@echo "  make clean            Remove __pycache__ and temp files"

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

