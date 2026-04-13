# PROJECT BLUEPRINT

## 1) Objective

This project is a learning-focused, end-to-end data platform blueprint for:
- Batch and streaming pipeline development
- Airflow-based orchestration and scheduling
- Dockerized local runtime
- Config-driven processing using JSON transformation flow definitions
- Secure connection management and egress enforcement

Primary business sample domain used here: banking-style datasets (cards, accounts, trips, events).

---

## 2) High-Level Architecture

### Core Layers
- Source Layer (local files mapped as ADLS-style `/mnt/...` paths)
- Pipeline Layer (config + reusable pipeline modules)
- App Layer (batch apps + streaming apps)
- Orchestration Layer (Airflow DAGs + custom operators)
- Runtime Layer (Docker services)
- Security Layer (central connection refs + egress rules + shield values)
- Target Layer (PostgreSQL + Cassandra)

### Data Path
1. Pipeline JSON config is loaded
2. Config is normalized (`mainFlow.transformationList` -> runner runtime model)
3. Source path is resolved (including `${CARID}` and `/mnt/...` mapping)
4. Transform steps are applied (filter + timestamp + others)
5. Connection details are resolved using `connectionRef`
6. Egress policy is checked for each outbound target call
7. Data is written to Postgres and Cassandra
8. Airflow performs schedule + holiday + file watch + run + load validation

---

## 3) Key Project Structure

- `airflow/`
  - `docker-compose.yaml` (Airflow 2.11 local stack)
  - `dags/` (batch + streaming + DAG factory)
  - `plugins/operators/` (custom operators)
- `etl-project/`
  - `appconfig/dev/pipeline_*_pg_cassandra.json` (pipeline definitions)
  - `spark-job/local_batch_streaming_runner.py` (core local runner)
  - `local_source/` (input datasets)
- `pipelines/`
  - `configs/app_connection_config.yaml` (central connection catalog)
  - `egress/egress_rules.yaml` + `egress_handler.py`
  - `shield/shield_values.yaml`
  - `tech/` (technology command modules)
- `batch_apps/` and `streaming_apps/` (separate app modules)
- `tests/` (runner/config/security/module tests)

---

## 4) Config Model (Standard)

Pipeline files use JSON with this canonical shape:
- `generalInfo` (appName, processType, carId, deploymentType)
- `connections` (per-target `connectionRef` only; no hardcoded secrets)
- `mainFlow[0].transformationList`
  - Source step
  - Transformation steps (expression/filter/partitioner)
  - Target steps (Postgres/Cassandra)

Secrets are not stored in pipeline JSON.

---

## 5) Security and Connectivity Blueprint

### Connection Management
- Centralized at `pipelines/base/connection_manager.py`
- Uses refs from `pipelines/configs/app_connection_config.yaml`
- Supports per-pipeline refs (e.g., `postgres_bank_cards_live_stream`)
- Supports env-based secret injection (`${POSTGRES_PASSWORD}`, etc.)

### Egress Governance
- Policy file: `pipelines/egress/egress_rules.yaml`
- Enforced by: `pipelines/egress/egress_handler.py`
- Runtime blocks unauthorized outbound connections
- Per-pipeline target egress rules defined for Postgres/Cassandra

### Shield Controls
- Baseline guardrail config kept in `pipelines/shield/shield_values.yaml`

---

## 6) Airflow Orchestration Blueprint

Primary operator chain pattern:
1. `HolidayCalendarOperator`
2. `FileWatcherOperator`
3. `PipelineRunnerOperator`
4. `TargetLoadCheckOperator`

DAG generation and scheduling are managed by `airflow/dags/pipeline_apps_dag_factory.py`.

Airflow runtime stack uses:
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`
- `postgres` (Airflow metadata DB)

---

## 7) Technology Modules Added

Separate module per technology created in `pipelines/tech/`:
- `adls_module.py` (path/env setup commands)
- `docker_module.py`
- `airflow_module.py`
- `postgres_module.py`
- `cassandra_module.py`
- `spark_runner_module.py`
- `__init__.py` exports all

Helper script:
- `scripts/print_used_commands.py` prints command groups and env keys

---

## 8) Commands Used So Far

## 8.1 Executed Commands

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
python -m pytest tests/test_local_batch_streaming_runner.py -q
python -m pytest -o addopts="" tests/test_local_batch_streaming_runner.py -q
python -c "import json,glob; files=glob.glob('etl-project/appconfig/dev/pipeline_*_pg_cassandra.json'); [json.load(open(f,encoding='utf-8-sig')) for f in files]; print('validated',len(files),'pipeline json files')"
python -m pytest -o addopts="" tests/test_local_batch_streaming_runner.py tests/test_connection_manager.py -q
python -m pytest -o addopts="" tests/test_connection_manager.py tests/test_local_batch_streaming_runner.py -q
python -m pytest -o addopts="" tests/test_tech_modules.py tests/test_connection_manager.py -q
```

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
docker compose ps
```

## 8.2 Shared Setup Commands (for running stack)

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
docker compose logs --no-color airflow-webserver
docker compose logs --no-color airflow-scheduler
```

## 8.3 Path and Env Setup Commands

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
$env:CARID="8999"
$env:POSTGRES_HOST="localhost"
$env:POSTGRES_PORT="5432"
$env:POSTGRES_DB="learning_db"
$env:POSTGRES_USER="postgres"
$env:POSTGRES_PASSWORD="<set_your_secret>"
$env:CASSANDRA_HOST="127.0.0.1"
$env:CASSANDRA_PORT="9042"
$env:CASSANDRA_KEYSPACE="learning"
$env:CASSANDRA_USERNAME=""
$env:CASSANDRA_PASSWORD=""
```

---

## 9) Validation and Quality Gates

Implemented validations include:
- `tests/test_local_batch_streaming_runner.py`
- `tests/test_connection_manager.py`
- `tests/test_tech_modules.py`

These validate:
- Config parsing and runner dry-runs
- No hardcoded secrets in pipeline connection blocks
- Technology command modules return expected values

---

## 10) Local Access

When Airflow services are healthy, UI endpoint is:
- `http://localhost:8080`

(Availability depends on Docker Desktop engine status and running containers.)

---

## 11) Always-On Runtime Plan

- `airflow/docker-compose.yaml` uses restart policies (`unless-stopped`) for `postgres`, `airflow-webserver`, and `airflow-scheduler`.
- `airflow-init` runs once before runtime services and gates startup.
- Scheduler healthcheck is enabled so unhealthy scheduler instances are visible quickly.
- Windows startup scripts are provided in `scripts/windows/`:
  - `start-airflow.ps1`
  - `wait-airflow-ready.ps1`
  - `unpause-pipeline-dags.ps1`
  - `install-airflow-autostart.ps1`
  - `stop-airflow.ps1`

Run sequence for scheduled triggers:

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
powershell -ExecutionPolicy Bypass -File scripts\windows\start-airflow.ps1
powershell -ExecutionPolicy Bypass -File scripts\windows\wait-airflow-ready.ps1
powershell -ExecutionPolicy Bypass -File scripts\windows\unpause-pipeline-dags.ps1
```

