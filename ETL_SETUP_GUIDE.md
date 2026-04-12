# Bank Customer Risk Triggers ETL Platform - Complete Setup Guide

## 📋 Overview

This is a **production-grade Enterprise ETL Platform** for processing customer risk trigger data. It uses Apache Spark (PySpark & Scala), PostgreSQL, and REST APIs with comprehensive monitoring, data quality validation, and audit logging.

**Application Name:** `btot-8999-syn-api-cust-risk-tg`  
**Dataset:** `cust-risk-triggers`  
**Source:** Synapse SQL Server (with CSV fallback)  
**Targets:** PostgreSQL + REST API + Audit Files

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Sources                                 │
├─────────────┬────────────────────────┬──────────────────────────┤
│  Synapse    │   CSV Files            │   Parquet Files          │
│  SQL Server │   (Fallback)           │   (Optional)             │
└────────────┬────────────┬────────────┴──────────────────────────┘
             │            │
             └────────────┼──────────────────────────────────────┐
                          ▼                                       │
              ┌──────────────────────┐                            │
              │   EXTRACTION LAYER   │                            │
              │  (PySpark + Scala)   │                            │
              └──────────┬───────────┘                            │
                         │                                        │
         ┌───────────────┼───────────────┐                        │
         ▼               ▼               ▼                        │
    Expression      Deduplicator    Partitioner                  │
    Transform                                                     │
         │               │               │                        │
         └───────────────┼───────────────┘                        │
                         │                                        │
         ┌───────────────┼───────────────┬─────────────────────┐  │
         ▼               ▼               ▼                     ▼  │
    PostgreSQL      REST API         Audit Files          Metrics│
    (Primary)       (Real-time)       (Fixed-Width)     (Monitoring)
```

---

## 📁 Project Structure

```
De_Dv_Airflow_Local_pro/
├── batch_apps/
│   └── spark_batch/
│       ├── bank_cust_risk_triggers_etl.py      # Main PySpark app
│       ├── BankCustomerRiskTriggersETL.scala   # Scala alternative
│       ├── build.sbt                           # Scala build config
│       ├── postgres_schema.sql                 # PostgreSQL DDL
│       ├── postgres_init.sql                   # Sample data
│       ├── source.sql                          # Synapse query
│       └── transformations/
│           ├── batch_transforms.py             # Transformation library
│           └── __init__.py
├── pipelines/
│   └── configs/
│       ├── bank_cust_risk_triggers_config.yaml # Main config
│       ├── app_connection_config.yaml
│       └── batch_pipeline_config.yaml
├── docker-compose-bank-etl.yaml                # Docker services
├── .env.bank-etl                               # Environment vars
├── setup.sh                                    # Setup script
├── BANK_ETL_README.md                         # Detailed guide
└── requirements.txt                            # Python dependencies
```

---

## 🚀 Quick Start

### 1️⃣ **Prerequisites**

- Docker & Docker Compose
- Python 3.11+
- Java 8+ (for Scala)
- Git

### 2️⃣ **Clone & Setup**

```bash
# Navigate to project directory
cd De_Dv_Airflow_Local_pro

# Make setup script executable
chmod +x setup.sh

# Run setup (creates Docker containers, installs dependencies)
bash setup.sh

# Load environment variables
export $(cat .env.bank-etl | grep -v '^#' | xargs)
```

### 3️⃣ **Verify Services Are Running**

```bash
# Check Docker containers
docker-compose -f docker-compose-bank-etl.yaml ps

# Access services:
# PostgreSQL: localhost:5432
# pgAdmin: http://localhost:5050
# Spark Master: http://localhost:8080
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000
```

### 4️⃣ **Run the ETL Pipeline**

#### **Option A: PySpark (Python)**

```bash
# Basic run
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py

# Run with custom config
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \
  --config pipelines/configs/bank_cust_risk_triggers_config.yaml \
  --date 2026-04-12 \
  --mode normal

# Debug mode
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode debug

# Test mode (no actual loads)
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode test
```

#### **Option B: Scala**

```bash
# Build
cd batch_apps/spark_batch
sbt package

# Submit to Spark
spark-submit \
  --class com.bank.etl.cust_risk_triggers.BankCustomerRiskTriggersETL \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 2g \
  target/scala-2.12/bank-cust-risk-triggers-etl_2.12-1.0.0.jar \
  --config pipelines/configs/bank_cust_risk_triggers_config.yaml \
  --date 2026-04-12
```

---

## 🔧 Configuration

### Main Config File: `pipelines/configs/bank_cust_risk_triggers_config.yaml`

```yaml
generalInfo:
  appName: "btot-8999-syn-api-cust-risk-tg"
  dataSetName: "cust-risk-triggers"
  logLevel: "INFO"
  deploymentType: "OnPrem"

spark:
  master: "local[*]"
  executor_memory: "2g"
  driver_memory: "1g"

mainFlow:
  - transformationName: "Synapse_Source"
    transformationType: "Source"
    order: 0
  
  - transformationName: "expression1"
    transformationType: "expression"
    order: 1
  
  - transformationName: "partitioner1"
    transformationType: "partitioner"
    order: 2
  
  - transformationName: "postgres-target"
    transformationType: "Target"
    order: 3
  
  - transformationName: "RESTAPI-target"
    transformationType: "Target"
    order: 4
```

### Environment Variables (`.env.bank-etl`)

```bash
# Spark
SPARK_MASTER_URL=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g

# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=bank_db
PG_USER=postgres
PG_PASSWORD=postgres

# Synapse
SYNAPSE_SERVER=your-synapse-server.database.windows.net
SYNAPSE_DATABASE=your_database
SYNAPSE_USER=sa
SYNAPSE_PASSWORD=your_password

# API
API_URI=https://digital-servicing-api.us.bank-dns.com/digital/servicing/risk-triggers
API_AUTH_TYPE=MTLS
```

---

## 📊 Data Model

### Core Table: `cust_risk_triggers`

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| customerLpid | VARCHAR(50) | Customer ID (UNIQUE with date) |
| custcardTokens | VARCHAR(255) | Card token |
| riskScore | DECIMAL(10,2) | Risk score (0-100) |
| riskTriggerDate | TIMESTAMP | Risk trigger time |
| **riskIndicator** | VARCHAR(50) | **HIGH / MEDIUM / LOW / UNKNOWN** |
| transactionAmount | DECIMAL(15,2) | Amount |
| transactionDate | DATE | Transaction date |
| merchantCategory | VARCHAR(100) | Merchant category |
| processedAt | TIMESTAMP | Processing time |
| processDate | DATE | Processing date |
| pipelineVersion | VARCHAR(20) | ETL version |
| dataQuality | VARCHAR(20) | Quality status |

**Unique Constraint:** `(customerLpid, transactionDate)`

**Indexes:**
- `idx_cust_risk_triggers_customerLpid`
- `idx_cust_risk_triggers_transactionDate`
- `idx_cust_risk_triggers_riskIndicator`
- `idx_cust_risk_triggers_processDate`

### Views

```sql
-- High-risk customers (risk score > 50)
SELECT * FROM v_high_risk_customers;

-- Daily risk summary
SELECT * FROM v_daily_risk_summary;

-- Customer risk profile (materialized view)
SELECT * FROM mv_customer_risk_profile;
```

---

## 📝 Pipeline Execution Steps

### Phase 1: **EXTRACTION**
- Reads from Synapse SQL Server using `source.sql` query
- Fallback to CSV if Synapse unavailable
- Supports CSV, Parquet, and JSON formats

### Phase 2: **TRANSFORMATION**
1. **Expression Transformation**
   - Type casting (String, Decimal, Timestamp, Date)
   - Add derived columns
   - Risk indicator categorization

2. **Deduplication**
   - Remove duplicates on (customerLpid, transactionDate)
   - Track removal count in metrics

3. **Partitioning**
   - Repartition to 10 partitions for parallel processing
   - Improves write performance

### Phase 3: **DATA QUALITY VALIDATION**
- Count total records
- Check null values per column
- Validate data types
- Generate quality metrics

### Phase 4: **LOADING**
- Write to PostgreSQL (`cust_risk_triggers` table)
- Mode: APPEND (to preserve history)
- Batch size: 1000 records

### Phase 5: **API PUSH** (Optional)
- Push records to REST API endpoint
- Rate limit: 2000 records/second
- Retry: 3 attempts with exponential backoff
- Auth: MTLS

### Phase 6: **AUDIT OUTPUT**
- Write audit log to fixed-width file
- Contains: customerLpid, riskScore, riskIndicator, timestamps

---

## 📊 Sample Data

The pipeline includes 30 sample records in `bank_data_cust_risk_triggers.csv`:

```csv
customerLpid,custcardTokens,riskScore,riskTriggerDate,riskIndicator,transactionAmount,transactionDate,merchantCategory,createdAt
LPID001,TOKEN_4523xxxx,85.50,2026-04-11T10:30:00,HIGH,5000.00,2026-04-11,RETAIL,2026-04-11T10:31:00
LPID002,TOKEN_7891xxxx,45.25,2026-04-11T11:15:00,MEDIUM,1500.00,2026-04-11,GROCERY,2026-04-11T11:16:00
LPID003,TOKEN_5634xxxx,92.75,2026-04-11T09:45:00,HIGH,8500.00,2026-04-11,TRAVEL,2026-04-11T09:46:00
...
```

**Data Characteristics:**
- 30 records
- Risk scores: 15 to 95
- Risk indicators: HIGH (≥75), MEDIUM (50-74), LOW (25-49), UNKNOWN (<25)
- Transaction amounts: $250 to $20,000
- Various merchant categories

---

## 🧪 Testing

### Run in Test Mode
```bash
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode test
```
- No actual database writes
- No API calls
- Validates logic only

### Verify Data Loads

```bash
# Connect to PostgreSQL
docker exec postgres-bank psql -U postgres -d bank_db

# Check record count
SELECT COUNT(*) FROM cust_risk_triggers;

# View sample data
SELECT * FROM cust_risk_triggers LIMIT 10;

# Risk distribution
SELECT riskIndicator, COUNT(*) as count 
FROM cust_risk_triggers 
GROUP BY riskIndicator;

# High-risk customers
SELECT * FROM v_high_risk_customers LIMIT 10;

# Daily summary
SELECT * FROM v_daily_risk_summary LIMIT 5;
```

---

## 🔍 Monitoring & Debugging

### Logs

```bash
# PySpark logs
tail -f btot-8999-syn-api-cust-risk-tg-*.log

# Docker logs
docker logs postgres-bank
docker logs spark-master
docker-compose -f docker-compose-bank-etl.yaml logs -f
```

### Spark UI
- **Master:** http://localhost:8080
- **Worker 1:** http://localhost:8081
- **Worker 2:** http://localhost:8082

### Prometheus Metrics
- **URL:** http://localhost:9090
- **Query:** `spark_executor_memory_used_bytes`

### Grafana Dashboards
- **URL:** http://localhost:3000
- **Credentials:** admin / admin
- Add Prometheus data source at `http://prometheus:9090`

### Database Admin
- **pgAdmin:** http://localhost:5050
- **Credentials:** admin@example.com / admin
- **Server:** postgres-bank:5432

---

## 🐛 Troubleshooting

### Issue: "Synapse extraction failed"
**Solution:** Pipeline automatically falls back to CSV
```bash
# Verify CSV file exists
ls -la bank_data_cust_risk_triggers.csv

# Check environment variables
echo $SYNAPSE_SERVER
```

### Issue: "PostgreSQL connection refused"
**Solution:** Ensure PostgreSQL container is running
```bash
docker-compose -f docker-compose-bank-etl.yaml up -d postgres-bank
docker-compose -f docker-compose-bank-etl.yaml ps
```

### Issue: "Out of memory"
**Solution:** Increase memory allocation
```bash
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g
```

### Issue: "API request timeout"
**Solution:** Increase timeout in config
```yaml
tgtSysProp:
  retryAttempts: 5
  retryDelayMs: 2000
  timeoutSeconds: 30
```

---

## 📈 Performance Optimization

### Spark Tuning
```python
spark.config("spark.sql.shuffle.partitions", "10")  # Match cluster size
spark.config("spark.sql.adaptive.enabled", "true")
spark.config("spark.sql.broadcastTimeout", "300")
```

### PostgreSQL Tuning
```sql
-- Increase connection pool
ALTER SYSTEM SET max_connections = 200;

-- Increase work memory
ALTER SYSTEM SET work_mem = '256MB';

-- Increase shared buffers
ALTER SYSTEM SET shared_buffers = '256MB';

SELECT pg_reload_conf();
```

### CSV Export Strategy
```bash
# For large datasets, export in batches
COPY cust_risk_triggers TO '/path/to/export.csv' WITH CSV HEADER;
```

---

## 🔐 Security

### Credentials Management
- Use `.env.bank-etl` for environment variables (NOT committed to git)
- Use secrets management for production (Vault, AWS Secrets)
- MTLS certificates in `/opt/certs/`

### Database Security
```bash
# Create read-only user
psql -U postgres -c "CREATE ROLE readonly_user LOGIN PASSWORD 'password';"
psql -U postgres -d bank_db -c "GRANT SELECT ON cust_risk_triggers TO readonly_user;"
```

### Network Security
- Docker network isolation: `bank-etl-network`
- PostgreSQL accessible only from containers
- REST API uses MTLS authentication

---

## 📅 Scheduling (Airflow Integration)

### DAG Example

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'btot-8999-syn-api-cust-risk-tg',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
)

run_etl = BashOperator(
    task_id='run_etl',
    bash_command='''
    python /workspace/batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \
      --config /workspace/pipelines/configs/bank_cust_risk_triggers_config.yaml \
      --date {{ ds }}
    ''',
    dag=dag,
)
```

---

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Scala Spark API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/)

---

## 🤝 Contributing

1. Create a feature branch
2. Make changes
3. Test thoroughly
4. Submit pull request

---

## 📞 Support & Issues

- Check `BANK_ETL_README.md` for detailed documentation
- Review logs in `/var/log/spark/btot-8999-syn-api-cust-risk-tg/`
- Contact: de-team@example.com

---

## 📄 License

MIT License - See LICENSE file

---

**Version:** 1.0.0  
**Last Updated:** April 12, 2026  
**Status:** ✅ Production Ready

