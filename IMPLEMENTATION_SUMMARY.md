"""
Implementation Summary & Quick Reference
=========================================

Bank Customer Risk Triggers ETL Platform
Application: btot-8999-syn-api-cust-risk-tg
Dataset: cust-risk-triggers

This document provides a comprehensive summary of the complete ETL platform
implementation including architecture, components, configurations, and usage.
"""

# ============================================================================
# 1. PROJECT STRUCTURE
# ============================================================================

PROJECT_STRUCTURE = """
De_Dv_Airflow_Local_pro/
│
├── batch_apps/spark_batch/
│   ├── bank_cust_risk_triggers_etl.py           ✅ Main PySpark application
│   ├── BankCustomerRiskTriggersETL.scala         ✅ Scala alternative
│   ├── build.sbt                                 ✅ Scala build config
│   ├── postgres_schema.sql                       ✅ Database schema (12 tables/views)
│   ├── postgres_init.sql                         ✅ Sample data (30 records)
│   ├── source.sql                                ✅ Synapse extraction query
│   └── transformations/
│       ├── batch_transforms.py                   ✅ Transformation library
│       └── __init__.py
│
├── pipelines/configs/
│   ├── bank_cust_risk_triggers_config.yaml       ✅ Main YAML config (6 transformations)
│   ├── app_connection_config.yaml                ✅ Connection strings
│   └── batch_pipeline_config.yaml                ✅ Pipeline settings
│
├── airflow/dags/
│   └── btot_8999_syn_api_cust_risk_tg.py        ✅ Airflow DAG (9 tasks)
│
├── docker-compose-bank-etl.yaml                  ✅ Docker services (9 containers)
├── .env.bank-etl                                 ✅ Environment variables
├── setup.sh                                      ✅ Setup script
├── deploy.sh                                     ✅ Deployment script
├── BANK_ETL_README.md                            ✅ Detailed documentation
├── ETL_SETUP_GUIDE.md                            ✅ Quick start guide
└── requirements.txt                              ✅ Python dependencies
"""

# ============================================================================
# 2. KEY COMPONENTS
# ============================================================================

COMPONENTS = {
    'Data Sources': [
        'Synapse SQL Server (Primary)',
        'CSV Files (Fallback)',
        'Parquet Files (Optional)',
        'JSON Files (Optional)',
    ],
    
    'Transformations': [
        'Expression Transformation (Type casting, field mapping)',
        'Deduplication (Remove duplicates on customerLpid + transactionDate)',
        'Partitioning (Repartition to 10 partitions)',
        'Data Quality Validation (Null checks, metrics)',
    ],
    
    'Data Targets': [
        'PostgreSQL (Primary data warehouse)',
        'REST API (Real-time updates)',
        'Fixed-Width Audit Files (Compliance)',
    ],
    
    'Infrastructure': [
        'Docker Compose (9 services)',
        'PostgreSQL 15 (Database)',
        'Apache Spark 3.5.1 (Processing)',
        'Prometheus (Metrics)',
        'Grafana (Visualization)',
        'Redis (Caching)',
        'MinIO (Object Storage)',
    ],
}

# ============================================================================
# 3. CONFIGURATION TEMPLATE
# ============================================================================

CONFIGURATION_TEMPLATE = """
# Main Configuration File: pipelines/configs/bank_cust_risk_triggers_config.yaml

generalInfo:
  appName: "btot-8999-syn-api-cust-risk-tg"
  dataSetName: "cust-risk-triggers"
  deploymentType: "OnPrem"
  logLevel: "INFO"

mainFlow:
  # Step 0: Extract from Synapse/CSV
  - transformationName: "Synapse_Source"
    transformationType: "Source"
    order: 0
  
  # Step 1: Expression transformations
  - transformationName: "expression1"
    transformationType: "expression"
    order: 1
    props:
      trnsfmRules: |
        CAST(customerLpid as STRING) as customerLpid,
        CAST(riskScore as DECIMAL(10,2)) as riskScore,
        CAST(riskTriggerDate as TIMESTAMP) as riskTriggerDate
  
  # Step 2: Deduplication
  - transformationName: "deduplicator1"
    transformationType: "deduplicator"
    order: 2
    props:
      deduplicationKeys:
        - customerLpid
        - transactionDate
  
  # Step 3: Partitioning
  - transformationName: "partitioner1"
    transformationType: "partitioner"
    order: 3
    props:
      partitionType: "REPARTITION"
      numPartition: "10"
  
  # Step 4: Load to PostgreSQL
  - transformationName: "postgres-target"
    transformationType: "Target"
    order: 4
    props:
      tgtSysName: "PostgreSQL"
      tgtSysProp:
        table: "cust_risk_triggers"
        mode: "append"
  
  # Step 5: Push to REST API
  - transformationName: "RESTAPI-target"
    transformationType: "Target"
    order: 5
    props:
      tgtSysName: "RESTAPI"
      tgtSysProp:
        uri: "https://digital-servicing-api.us.bank-dns.com/digital/servicing/risk-triggers"
        method: "POST"
  
  # Step 6: Write audit file
  - transformationName: "output_audit_file"
    transformationType: "Target"
    order: 6
    props:
      tgtSysName: "fixedwidth-file"
      tgtSysProp:
        fileName: "cust_risk_triggers_audit.txt"
"""

# ============================================================================
# 4. DATABASE SCHEMA
# ============================================================================

DATABASE_SCHEMA = """
Main Tables (4):
├── cust_risk_triggers
│   ├── id (BIGSERIAL PRIMARY KEY)
│   ├── customerLpid (VARCHAR 50) - UNIQUE with transactionDate
│   ├── riskScore (DECIMAL 10,2)
│   ├── riskIndicator (VARCHAR 50) - HIGH/MEDIUM/LOW/UNKNOWN
│   ├── transactionAmount (DECIMAL 15,2)
│   ├── transactionDate (DATE)
│   └── 4 INDEXES for performance
│
├── cust_risk_triggers_audit
│   ├── Tracks changes to risk indicators
│   ├── Stores old/new values
│   └── 2 INDEXES
│
├── data_quality_log
│   ├── Tracks data quality metrics
│   ├── Records: total, valid, duplicate counts
│   └── 2 INDEXES
│
└── pipeline_execution_log
    ├── Pipeline execution history
    ├── Execution duration tracking
    └── 2 INDEXES

Views (3):
├── v_high_risk_customers (Risk > 50)
├── v_daily_risk_summary (Daily aggregations)
└── mv_customer_risk_profile (Materialized view)

Functions (1):
└── check_data_quality(date) - Data quality checks
"""

# ============================================================================
# 5. RUNNING THE PIPELINE
# ============================================================================

QUICK_START = """
STEP 1: Setup Environment
$ chmod +x setup.sh
$ bash setup.sh

STEP 2: Verify Services
$ docker-compose -f docker-compose-bank-etl.yaml ps
Expected: 9 containers running (postgres, pgadmin, spark-master, 2 workers, etc.)

STEP 3: Run ETL Pipeline (PySpark)
$ python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \\
    --config pipelines/configs/bank_cust_risk_triggers_config.yaml \\
    --date 2026-04-12 \\
    --mode normal

OR Run Scala Version:
$ cd batch_apps/spark_batch && sbt package && cd ../../
$ spark-submit \\
    --class com.bank.etl.cust_risk_triggers.BankCustomerRiskTriggersETL \\
    --master spark://spark-master:7077 \\
    target/scala-2.12/bank-cust-risk-triggers-etl_2.12-1.0.0.jar

STEP 4: Verify Data Load
$ docker exec postgres-bank psql -U postgres -d bank_db
> SELECT COUNT(*) FROM cust_risk_triggers;
Expected: 30 records

STEP 5: View Reports
- PostgreSQL: http://localhost:5050
- Spark UI: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
"""

# ============================================================================
# 6. DOCKER SERVICES
# ============================================================================

DOCKER_SERVICES = {
    'postgres-bank': {
        'image': 'postgres:15-alpine',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'bank_db',
        'volume': 'postgres-data',
    },
    'pgadmin': {
        'image': 'dpage/pgadmin4:latest',
        'port': 5050,
        'user': 'admin@example.com',
        'password': 'admin',
    },
    'spark-master': {
        'image': 'bitnami/spark:3.5.1',
        'ports': '8080,7077,6066',
        'mode': 'Master',
    },
    'spark-worker-1': {
        'image': 'bitnami/spark:3.5.1',
        'port': 8081,
        'mode': 'Worker',
    },
    'spark-worker-2': {
        'image': 'bitnami/spark:3.5.1',
        'port': 8082,
        'mode': 'Worker',
    },
    'prometheus': {
        'image': 'prom/prometheus:latest',
        'port': 9090,
        'volume': 'prometheus-data',
    },
    'grafana': {
        'image': 'grafana/grafana:latest',
        'port': 3000,
        'user': 'admin',
        'password': 'admin',
    },
    'redis': {
        'image': 'redis:7-alpine',
        'port': 6379,
        'volume': 'redis-data',
    },
    'minio': {
        'image': 'minio/minio:latest',
        'ports': '9000,9001',
        'volume': 'minio-data',
    },
}

# ============================================================================
# 7. TRANSFORMATION PIPELINE FLOW
# ============================================================================

TRANSFORMATION_FLOW = """
Input Data (CSV/Synapse)
         │
         ▼
┌─────────────────────────────────────┐
│ Source Transformation               │
│ - Read from Synapse SQL Server      │
│ - Fallback to CSV if unavailable    │
│ - Normalize column names            │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│ Expression Transformation           │
│ - Type casting                      │
│ - Field mapping                     │
│ - Derive riskIndicator from score   │
│ - Add processedAt, processDate      │
│ - Add pipelineVersion, dataQuality  │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│ Deduplication                       │
│ - Remove duplicates                 │
│ - Key: (customerLpid, txnDate)      │
│ - Keep latest by processedAt        │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│ Partitioning                        │
│ - Repartition to 10 partitions      │
│ - Enables parallel processing       │
└────────────┬────────────────────────┘
             │
    ┌────────┴────────┬───────────┐
    ▼                 ▼           ▼
PostgreSQL       REST API    Audit File
(Persistence)   (Real-time)  (Compliance)
"""

# ============================================================================
# 8. ENVIRONMENT VARIABLES
# ============================================================================

ENV_VARIABLES = """
# .env.bank-etl

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
SYNAPSE_SERVER=your-server.database.windows.net
SYNAPSE_DATABASE=your_db
SYNAPSE_USER=sa
SYNAPSE_PASSWORD=your_password

# API
API_URI=https://digital-servicing-api.us.bank-dns.com/digital/servicing/risk-triggers
API_AUTH_TYPE=MTLS
API_RETRY_ATTEMPTS=3

# Monitoring
ENABLE_METRICS=true
ENABLE_AUDIT=true
LOG_LEVEL=INFO
"""

# ============================================================================
# 9. COMMON COMMANDS
# ============================================================================

COMMON_COMMANDS = """
# Setup & Initialization
chmod +x setup.sh && bash setup.sh
chmod +x deploy.sh && bash deploy.sh dev
bash deploy.sh rollback
bash deploy.sh cleanup

# Run Pipeline
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode test
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode debug

# Docker Management
docker-compose -f docker-compose-bank-etl.yaml up -d
docker-compose -f docker-compose-bank-etl.yaml down
docker-compose -f docker-compose-bank-etl.yaml ps
docker-compose -f docker-compose-bank-etl.yaml logs -f

# Database Queries
docker exec postgres-bank psql -U postgres -d bank_db
SELECT COUNT(*) FROM cust_risk_triggers;
SELECT * FROM v_high_risk_customers LIMIT 10;
SELECT * FROM v_daily_risk_summary ORDER BY processDate DESC;

# Monitor Services
# Spark Master UI: http://localhost:8080
# pgAdmin: http://localhost:5050
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000
"""

# ============================================================================
# 10. TROUBLESHOOTING
# ============================================================================

TROUBLESHOOTING = """
Problem: Synapse extraction failed
Solution: 
- Check SYNAPSE_SERVER, SYNAPSE_DATABASE, SYNAPSE_USER, SYNAPSE_PASSWORD
- Pipeline automatically falls back to CSV
- Verify bank_data_cust_risk_triggers.csv exists

Problem: PostgreSQL connection refused
Solution:
- Ensure postgres-bank container is running
- Check PG_HOST, PG_PORT, PG_USER, PG_PASSWORD in .env.bank-etl
- docker-compose -f docker-compose-bank-etl.yaml up -d postgres-bank

Problem: Out of memory
Solution:
- Increase SPARK_EXECUTOR_MEMORY and SPARK_DRIVER_MEMORY
- Reduce number of partitions if needed
- Check spark-worker logs for details

Problem: API timeout
Solution:
- Increase API_TIMEOUT_SECONDS in config
- Check network connectivity to API endpoint
- Review retry_attempts and retry_delay_ms settings

Problem: Duplicate key violations
Solution:
- Ensure deduplication is enabled (transformationType: deduplicator)
- Check deduplicationKeys in config
- Review data quality metrics
"""

# ============================================================================
# 11. PRODUCTION DEPLOYMENT CHECKLIST
# ============================================================================

PRODUCTION_CHECKLIST = """
Pre-Deployment:
☐ Update .env.bank-etl with production credentials
☐ Configure Synapse connection details
☐ Set up MTLS certificates in /opt/certs/
☐ Configure PostgreSQL backup strategy
☐ Set up monitoring and alerting
☐ Review all transformation rules
☐ Test data quality thresholds
☐ Configure Airflow DAG schedule

Deployment:
☐ Run setup.sh to initialize Docker containers
☐ Verify all 9 containers are running
☐ Initialize PostgreSQL schema
☐ Load sample data for testing
☐ Test ETL pipeline in test mode
☐ Verify data loads to PostgreSQL
☐ Test API integration
☐ Verify audit files are created

Post-Deployment:
☐ Monitor first pipeline run
☐ Review data quality metrics
☐ Verify all targets (PostgreSQL, API, Audit)
☐ Set up Grafana dashboards
☐ Enable Prometheus scraping
☐ Configure alerting rules
☐ Set up log aggregation
☐ Document any deviations
☐ Plan maintenance windows
"""

# ============================================================================
# 12. PERFORMANCE OPTIMIZATION
# ============================================================================

OPTIMIZATION_TIPS = """
Spark Configuration:
spark.sql.shuffle.partitions = number_of_cores * 2-3
spark.sql.adaptive.enabled = true
spark.sql.broadcastTimeout = 300

PostgreSQL Tuning:
shared_buffers = 25% of available RAM
effective_cache_size = 50-75% of available RAM
work_mem = RAM / (max_connections * number_of_workers)
maintenance_work_mem = RAM / number_of_workers

CSV Export:
COPY cust_risk_triggers TO '/path/file.csv' WITH CSV HEADER;

Compression:
Use Parquet format for large datasets
Enable Snappy or LZ4 compression
"""

# ============================================================================
# Summary
# ============================================================================

if __name__ == '__main__':
    print(__doc__)
    print("\n" + "="*80)
    print("COMPLETE IMPLEMENTATION SUMMARY")
    print("="*80)
    print(PROJECT_STRUCTURE)
    print("\n" + QUICK_START)
    print("\n" + TRANSFORMATION_FLOW)

