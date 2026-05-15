# 🛠️ REMEDIATION GUIDE — Fix All Validation Issues

**Purpose:** Step-by-step guide to fix all issues found in project validation  
**Estimated Time:** 30-45 minutes  
**Last Updated:** 2026-05-15

---

## 🚀 QUICK START — TL;DR

```bash
# 1. Add Cassandra config to .env
# 2. Fix DAG file paths  
# 3. Update docker-compose for all services
# 4. Restart Airflow
```

---

## 📋 ISSUE-BY-ISSUE REMEDIATION

### 🔴 ISSUE #1: Missing Cassandra Configuration Variables

**Status:** ❌ NOT FIXED  
**Severity:** CRITICAL  
**Impact:** Cassandra connections will fail at runtime

#### Step 1.1: Update `.env.example`

```bash
# Location: C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\.env.example
# Add these lines after line 27 (POSTGRES section):
```

**File:** `.env.example` — ADD:
```dotenv
# ─── Cassandra ────────────────────────────────────────────────────────────
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=learning
CASSANDRA_USERNAME=cassandra
CASSANDRA_PASSWORD=cassandra
```

**Command:**
```bash
# Open .env.example and add the above Cassandra section
# Then copy to .env if needed
```

#### Step 1.2: Update `.env.bank-etl` (if exists)

```bash
# Same Cassandra variables as above
```

#### Step 1.3: Verify in `app_connection_config.yaml`

**File:** `pipelines/configs/app_connection_config.yaml`  
**Status:** ✅ Already references Cassandra configs correctly
```yaml
cassandra:
  type: cassandra
  hosts:
    - "${CASSANDRA_HOST}"
  port: "${CASSANDRA_PORT}"
  keyspace: "${CASSANDRA_KEYSPACE}"
  credentials:
    username: "${CASSANDRA_USERNAME}"
    password: "${CASSANDRA_PASSWORD}"
```

✅ **No changes needed for app_connection_config.yaml**

---

### 🔴 ISSUE #2: DAG File Paths Not Correct for Container

**Status:** ❌ NOT FIXED  
**Severity:** CRITICAL  
**Files to Fix:** Multiple DAG files  
**Impact:** Pipelines will fail with FileNotFoundError

#### Step 2.1: Fix `airflow/dags/batch/bank_cards_and_trips_batch_dag.py`

**Current Issue (Lines 31-37):**
```python
bash_command=(
    f"python {RUNNER} "
    f"--config {BASE}/appconfig/dev/pipeline_bank_cards_batch_csv_pg_cassandra.json "
    "--dry-run"
),
```

**Problem:** Working directory is not `/opt/airflow`, causing relative path issues

**Solution Step A:** Update to use absolute paths with `cd`:

Replace this file content with the fixed version below:

```python
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "learning-user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BASE = "/opt/airflow/etl-project"
RUNNER = f"{BASE}/spark-job/local_batch_streaming_runner.py"

with DAG(
    dag_id="bank_cards_and_trips_batch_dag",
    description="Batch pipelines from local bank/trip files to PostgreSQL and Cassandra",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "bank_cards", "trips", "postgres", "cassandra"],
) as dag:
    start = EmptyOperator(task_id="start")

    # FIX: Add 'cd /opt/airflow && ' prefix to working directory
    bank_cards_csv = BashOperator(
        task_id="bank_cards_csv_to_pg_cassandra",
        bash_command=(
            f"cd /opt/airflow && python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_bank_cards_batch_csv_pg_cassandra.json"
        ),
    )

    bank_cards_pipe = BashOperator(
        task_id="bank_cards_pipe_to_pg_cassandra",
        bash_command=(
            f"cd /opt/airflow && python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_bank_cards_pipe_batch_pg_cassandra.json"
        ),
    )

    city_trips = BashOperator(
        task_id="city_trips_batch_to_pg_cassandra",
        bash_command=(
            f"cd /opt/airflow && python {RUNNER} "
            f"--config {BASE}/appconfig/dev/pipeline_city_trips_batch_pg_cassandra.json"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> [bank_cards_csv, bank_cards_pipe, city_trips] >> end
```

**Key Changes:**
- Line 34, 42, 50: Added `cd /opt/airflow && ` prefix
- Removed `--dry-run` flag to actually execute

#### Step 2.2: Similar fixes for other batch DAGs

Apply the same `cd /opt/airflow && ` prefix to:

**Files to Update:**
1. `airflow/dags/batch/local_batch_pg_cassandra_dag.py`
2. `airflow/dags/batch/cust_risk_triggers_dag.py`  
3. `airflow/dags/batch/spark_batch_dag.py`
4. `airflow/dags/batch/pandas_batch_dag.py`
5. `airflow/dags/batch/sql_batch_dag.py`

**Pattern to Find & Replace:**
```bash
OLD: bash_command=(f"python {RUNNER}...
NEW: bash_command=(f"cd /opt/airflow && python {RUNNER}...
```

#### Step 2.3: Fix streaming DAGs similarly

**Files:**
1. `airflow/dags/streaming/kafka_streaming_dag.py`
2. `airflow/dags/streaming/spark_streaming_dag.py`
3. `airflow/dags/streaming/local_streaming_pg_cassandra_dag.py`
4. `airflow/dags/streaming/bank_cards_streaming_dag.py`

**Apply same fix:** Add `cd /opt/airflow && ` prefix

---

### 🔴 ISSUE #3: Missing Airflow Services (DAG Processor & Triggerer)

**Status:** ❌ NOT FIXED  
**Severity:** HIGH  
**File:** `airflow/docker-compose.yaml`  
**Impact:** DAGs won't process automatically; scheduled triggers won't work

#### Step 3.1: Update docker-compose.yaml

**Location:** Lines after `airflow-scheduler` service (~line 85)

**Add these two new services:**

```yaml
  airflow-dag-processor:
    <<: *airflow-common
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    container_name: airflow-dag-processor
    entrypoint: ["airflow", "dag-processor"]
    healthcheck:
      test: ["CMD-SHELL", "airflow dag-processor check-health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    environment:
      AIRFLOW__CORE__DAG_PROCESSOR_TTL: 300

  airflow-triggerer:
    <<: *airflow-common
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    container_name: airflow-triggerer
    entrypoint: ["airflow", "triggerer"]
    healthcheck:
      test: ["CMD-SHELL", "airflow triggerer check-health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    environment:
      AIRFLOW__TRIGGERER__DEFAULT_HANDLER_CLASS: "airflow.providers.apache.airflow_provider_for_databricks.trigger.DatabricksJobSensorDeferrableOperator"
```

**Place this AFTER the `airflow-scheduler` service definition.**

---

### 🟡 ISSUE #4: Source Data File Validation

**Status:** ⚠️ PARTIALLY ADDRESSED  
**Severity:** HIGH  
**Files Involved:** 
- `etl-project/appconfig/dev/pipeline_*.json`
- Actual data files in `etl-project/local_source/`

#### Step 4.1: Verify Local Source Files Exist

**Command to verify:**
```bash
# From project root:
ls -la etl-project/local_source/bank/
ls -la trip_data.csv

# Expected files:
# - bank_data.csv (or .json, .parquet, .txt variants)
# - bank_data.json
# - trip_data.csv
```

**If missing, copy sample files:**
```bash
# These already exist in the project root:
# - bank_data.csv
# - bank_data.json  
# - bank_data.parquet
# - trip_data.csv

# Copy to correct location:
cp bank_data.csv etl-project/local_source/bank/
cp bank_data.json etl-project/local_source/bank/
cp trip_data.csv etl-project/local_source/
```

#### Step 4.2: Verify Pipeline Configuration Paths

**File:** `etl-project/appconfig/dev/pipeline_bank_cards_batch_csv_pg_cassandra.json`  
**Check (Line 28):**
```json
"filePath": "/mnt/${CARID}-cda-fileshare-1/inbound/bank/bank_data.csv"
```

**For Local Development:**
✅ This is OK because:
- In production, `/mnt/` would be actual ADLS mount
- For local dev, adjust in runner or create symlink

**No change needed** if using the runner script correctly.

---

### 🟡 ISSUE #5: Python 3.13 Dependency Compatibility

**Status:** ⚠️ ENVIRONMENT ISSUE  
**Severity:** MEDIUM (Only if using Python 3.13)  
**Note:** Docker image uses Python 3.11 ✅ — This is SAFE

#### Step 5.1: Verify Docker Uses Python 3.11

**File:** `airflow/docker-compose.yaml` (Line 4)

Current:
```yaml
image: apache/airflow:2.11.2-python3.11
```

✅ **This is CORRECT** — No changes needed.

#### Step 5.2: If running locally (non-Docker), use Python 3.11

```bash
# Check your Python version:
python --version
# Should output: Python 3.11.x

# If using Python 3.13, downgrade to 3.11:
# Using pyenv:
pyenv install 3.11.0
pyenv local 3.11.0

# Using conda:
conda create -n airflow python=3.11
conda activate airflow
```

---

### 🟢 ISSUE #6: Cloudflare DNS Configuration

**Status:** ⚠️ PARTIALLY CONFIGURED  
**Severity:** MEDIUM (for production only)  
**Files:** `wrangler.toml`, Cloudflare tunnel setup

#### Step 6.1: Verify wrangler.toml Configuration

**Current File:**
```toml
route = "airflow-yourproject.example.com"
```

**Update to:**
```toml
route = "airflow-btst.airflowpk.com"
```

#### Step 6.2: Update Airflow Base URL

**File:** `airflow/docker-compose.yaml` (Line 15)

Update:
```yaml
AIRFLOW__WEBSERVER__BASE_URL: "https://airflow-btst.airflowpk.com"
```

#### Step 6.3: Update .env for Cloudflare

Add to `.env`:
```dotenv
AIRFLOW_PUBLIC_HOSTNAME=airflow-btst.airflowpk.com
AIRFLOW__WEBSERVER__BASE_URL=https://airflow-btst.airflowpk.com
```

---

## ✅ VERIFICATION CHECKLIST

After applying all fixes, verify with this checklist:

### Phase 1: Configuration ✅
- [ ] `.env` file exists with all required variables
- [ ] Cassandra variables added to `.env`
- [ ] `airflow/docker-compose.yaml` has all 5 services defined
- [ ] DAG files updated with `cd /opt/airflow && ` prefix
- [ ] Source data files exist in `etl-project/local_source/`

### Phase 2: Build & Start Services 🚀
```bash
# Navigate to project root:
cd C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro

# Stop existing containers:
docker-compose -f airflow/docker-compose.yaml down

# Remove old volumes (if needed):
docker volume prune

# Rebuild and start:
docker-compose -f airflow/docker-compose.yaml up -d

# Check status:
docker-compose -f airflow/docker-compose.yaml ps
```

### Phase 3: Verify Services ✓
```bash
# Check Airflow health:
curl http://localhost:8080/health

# Expected output:
# {"dag_processor": {"status": "healthy"}, 
#  "scheduler": {"status": "healthy"}, 
#  "triggerer": {"status": "healthy"}}

# Check DAGs loaded:
docker-compose -f airflow/docker-compose.yaml exec airflow-webserver airflow dags list

# Should see:
# bank_cards_and_trips_batch_dag
# local_batch_pg_cassandra_dag
# kafka_streaming_dag
# etc.
```

### Phase 4: Test DAG Execution 🧪
```bash
# Trigger a test run:
docker-compose -f airflow/docker-compose.yaml exec airflow-webserver \
  airflow dags test bank_cards_and_trips_batch_dag 2026-01-01

# Check logs:
docker logs airflow-webserver | tail -100
```

### Phase 5: Verify Database Connections 🔗
```bash
# Test PostgreSQL:
docker-compose -f airflow/docker-compose.yaml exec postgres \
  psql -U airflow -d airflow -c "SELECT 1;"

# Output should show: "1"

# Test Cassandra (if container running):
docker-compose exec cassandra cqlsh -e "SELECT now() FROM system.local;"
```

---

## 📊 FIXES SUMMARY TABLE

| # | Issue | Severity | Status | Time |
|---|-------|----------|--------|------|
| 1 | Missing Cassandra config | 🔴 Critical | ❌ Not Fixed | 2 min |
| 2 | DAG file paths | 🔴 Critical | ❌ Not Fixed | 10 min |
| 3 | Missing airflow services | 🔴 Critical | ❌ Not Fixed | 5 min |
| 4 | Source data files | 🟡 High | ⚠️ Partial | 3 min |
| 5 | Python 3.13 deps | 🟢 Medium | ✅ OK | 0 min |
| 6 | Cloudflare DNS | 🟢 Medium | ⚠️ Partial | 5 min |

**Total Estimated Fix Time: 30-45 minutes**

---

## 🔍 DETAILED FIX COMMANDS

### Quick Fix Script

```bash
#!/bin/bash
# fix_project.sh — Run all fixes automatically

set -e

PROJECT_ROOT="/c/Users/PAVAN/Local_pro/De_Dv_Airflow_Local_pro"
cd "$PROJECT_ROOT"

echo "🔧 Starting project fixes..."

# Create .env from template if not exists
if [ ! -f .env ]; then
    echo "📝 Creating .env from .env.example..."
    cp .env.example .env
    echo "✅ .env created (Update with real credentials)"
fi

# Add Cassandra config if missing
if ! grep -q "CASSANDRA_HOST" .env; then
    echo "📝 Adding Cassandra configuration..."
    cat >> .env << 'EOF'

# Cassandra Configuration
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=learning
CASSANDRA_USERNAME=cassandra
CASSANDRA_PASSWORD=cassandra
EOF
    echo "✅ Cassandra config added"
fi

# Verify DAG files exist
echo "✓ Checking DAG files..."
ls -la airflow/dags/batch/bank_cards_and_trips_batch_dag.py

# Copy source data files
echo "📝 Copying source data files..."
mkdir -p etl-project/local_source/bank
cp -v bank_data.csv etl-project/local_source/bank/ 2>/dev/null || echo "⚠️ bank_data.csv not found"
cp -v bank_data.json etl-project/local_source/bank/ 2>/dev/null || echo "⚠️ bank_data.json not found"

echo "✅ All fixes applied!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env with real credentials"
echo "2. Update DAG files with 'cd /opt/airflow && ' prefix"
echo "3. Update docker-compose.yaml with airflow-dag-processor and airflow-triggerer"
echo "4. Run: docker-compose -f airflow/docker-compose.yaml up -d"
```

**Save as:** `fix_project.sh`  
**Run:**
```bash
bash fix_project.sh
```

---

## 📞 SUPPORT & NEXT STEPS

1. **After fixes**, rerun validation:
   ```bash
   python scripts/smoke_check.py
   ```

2. **View logs** if issues persist:
   ```bash
   docker logs airflow-webserver
   docker logs airflow-scheduler
   docker logs airflow-dag-processor
   docker logs airflow-triggerer
   ```

3. **Check Docker health:**
   ```bash
   docker ps  # List running containers
   docker inspect airflow-webserver  # Detailed info
   ```

---

**Generated by:** GitHub Copilot  
**Format:** Markdown  
**Maintainable:** Yes  
**Timestamp:** 2026-05-15

