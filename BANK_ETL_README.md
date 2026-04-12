# Bank Customer Risk Triggers ETL Pipeline
## Application: btot-8999-syn-api-cust-risk-tg
## Dataset: cust-risk-triggers

---

## 📋 Overview

This is a comprehensive ETL (Extract, Transform, Load) pipeline that processes customer risk trigger data from multiple sources (Synapse SQL Server, CSV, Parquet) and loads it into PostgreSQL and REST API endpoints. The pipeline is designed for high-performance batch processing using Apache Spark.

**Key Features:**
- ✅ Multi-source data extraction (Synapse, CSV, Parquet, JSON)
- ✅ Advanced data transformations and type casting
- ✅ Automatic data quality validation
- ✅ Duplicate detection and handling
- ✅ PostgreSQL integration for persistence
- ✅ REST API integration for real-time updates
- ✅ Comprehensive audit logging
- ✅ Error handling and retry mechanisms
- ✅ Scalable Spark-based processing

---

## 📁 Project Structure

```
batch_apps/spark_batch/
├── bank_cust_risk_triggers_etl.py     # PySpark main application
├── BankCustomerRiskTriggersETL.scala   # Scala Spark application
├── build.sbt                           # Scala project configuration
├── postgres_schema.sql                 # PostgreSQL schema definition
├── source.sql                          # Synapse source query
├── transformations/                    # Transformation modules
│   ├── batch_transforms.py
│   └── __init__.py
└── __init__.py

pipelines/configs/
├── bank_cust_risk_triggers_config.yaml # Main pipeline configuration
├── batch_pipeline_config.yaml
└── app_connection_config.yaml

bank_data_cust_risk_triggers.csv        # Sample source data
docker-compose-bank-etl.yaml            # Docker services (PostgreSQL, Spark, etc.)
.env.bank-etl                           # Environment variables
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Apache Spark 3.5.1+
- PostgreSQL 15+ (or Docker)
- Java 8+ (for Scala)
- Scala 2.12+ (for Scala version)

### 1. Install Dependencies

```bash
# For PySpark version
pip install -r requirements.txt

# Install additional packages if needed
pip install pyspark==3.5.1 psycopg2-binary==2.9.9 requests==2.32.3 pyyaml==6.0.1
```

### 2. Set Up PostgreSQL (Docker)

```bash
# Start PostgreSQL container
docker-compose -f docker-compose-bank-etl.yaml up postgres-bank pgadmin

# The database schema will be automatically initialized
# PostgreSQL: localhost:5432
# pgAdmin: localhost:5050 (admin@example.com / admin)
```

### 3. Load Sample Data

```bash
# The sample data is already in: bank_data_cust_risk_triggers.csv
# It will be used as the data source
```

### 4. Run the Pipeline

#### Option A: PySpark (Python)
```bash
# Run with default configuration
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py

# Run with custom configuration and date
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \
  --config pipelines/configs/bank_cust_risk_triggers_config.yaml \
  --date 2026-04-12 \
  --mode normal

# Run in debug mode
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py --mode debug
```

#### Option B: Scala/Spark Submit
```bash
# Build the Scala project
cd batch_apps/spark_batch/
sbt package

# Submit to Spark
spark-submit \
  --class com.bank.etl.cust_risk_triggers.BankCustomerRiskTriggersETL \
  --master local[*] \
  --driver-memory 1g \
  --executor-memory 2g \
  target/scala-2.12/bank-cust-risk-triggers-etl_2.12-1.0.0.jar
```

---

## 📊 Pipeline Architecture

### Data Flow

```
Synapse (SQL Server)     CSV File        Parquet File
         │                   │                 │
         └───────────────────┴─────────────────┘
                      │
                  Extract
                      │
              ┌───────────────┐
              │ Data Validation
              │ (Quality Check)
              └───────────────┘
                      │
              ┌───────────────┐
              │ Transform
              │ - Type Casting
              │ - Field Mapping
              │ - Deduplication
              │ - Null Handling
              └───────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
      Load to               Push to API
      PostgreSQL            Endpoints
         │                         │
         └────────────┬────────────┘
                      │
              Audit & Logging

```

### Transformation Steps

1. **Extract**: Read data from configured source
2. **Normalize**: Map column names to expected schema
3. **Type Casting**: Convert columns to proper data types
   - `customerLpid` → STRING
   - `riskScore` → DECIMAL(10,2)
   - `riskTriggerDate` → TIMESTAMP
   - `riskIndicator` → STRING (HIGH, MEDIUM, LOW, UNKNOWN)
4. **Add Audit Columns**: 
   - `processedAt` (TIMESTAMP)
   - `processDate` (DATE)
   - `pipelineVersion` (STRING)
   - `dataQuality` (STRING)
5. **Deduplication**: Remove duplicates based on (customerLpid, transactionDate)
6. **Null Handling**: Fill null values with defaults
7. **Load**: Write to PostgreSQL and API endpoints

---

## 🗄️ Database Schema

### Main Tables

#### `cust_risk_triggers`
Stores customer risk trigger events.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| customerLpid | VARCHAR(50) | Customer identifier (UNIQUE with transactionDate) |
| custcardTokens | VARCHAR(255) | Card token |
| riskScore | NUMERIC(10,2) | Risk score (0-100) |
| riskTriggerDate | TIMESTAMP | When risk was triggered |
| riskIndicator | VARCHAR(50) | Risk level (HIGH, MEDIUM, LOW, UNKNOWN) |
| transactionAmount | NUMERIC(15,2) | Transaction amount |
| transactionDate | DATE | Transaction date |
| merchantCategory | VARCHAR(100) | Merchant category |
| createdAt | TIMESTAMP | Record creation time |
| processedAt | TIMESTAMP | Processing time |
| processDate | DATE | Processing date |
| pipelineVersion | VARCHAR(20) | Pipeline version |
| dataQuality | VARCHAR(20) | Quality status (VALIDATED, FAILED, PARTIAL) |

**Indexes:**
- `idx_cust_risk_triggers_customerLpid`
- `idx_cust_risk_triggers_transactionDate`
- `idx_cust_risk_triggers_riskIndicator`
- `idx_cust_risk_triggers_processDate`

#### `cust_risk_triggers_audit`
Tracks changes to risk indicators.

#### `data_quality_log`
Records data quality metrics for each run.

#### `pipeline_execution_log`
Tracks pipeline execution history.

### Views

- **`v_high_risk_customers`**: Customers with high risk scores
- **`v_daily_risk_summary`**: Daily risk statistics

---

## 🔧 Configuration

### Configuration File: `bank_cust_risk_triggers_config.yaml`

```yaml
generalInfo:
  appName: "btot-8999-syn-api-cust-risk-tg"
  dataSetName: "cust-risk-triggers"
  logLevel: "INFO"

spark:
  master: "local[*]"
  shuffle_partitions: "10"
  executor_memory: "2g"
  driver_memory: "1g"

source:
  srcSysName: "Synapse"
  srcSysProp:
    server: "${SYNAPSE_SERVER}"
    database: "${SYNAPSE_DATABASE}"
    sqlQueryFilePath: "/opt/nfs/.../source.sql"
    fallbackFormat: "csv"
    fallbackPath: "/opt/nfs/.../cust_risk_triggers/"

mainFlow:
  transformations:
    - Expression transformation
    - Partitioner (repartition)
    - PostgreSQL target
    - REST API target
    - Audit file output
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
SYNAPSE_SERVER=synapse-server.database.windows.net
SYNAPSE_DATABASE=edw_prod
SYNAPSE_USER=sa
SYNAPSE_PASSWORD=your_password

# API
API_URI=https://digital-servicing-api.../risk-triggers
```

---

## 📝 Sample Data

The pipeline includes sample data in `bank_data_cust_risk_triggers.csv`:

```csv
customerLpid,custcardTokens,riskScore,riskTriggerDate,riskIndicator,transactionAmount,transactionDate,merchantCategory,createdAt
LPID001,TOKEN_4523xxxx,85.50,2026-04-11T10:30:00,HIGH,5000.00,2026-04-11,RETAIL,2026-04-11T10:31:00
LPID002,TOKEN_7891xxxx,45.25,2026-04-11T11:15:00,MEDIUM,1500.00,2026-04-11,GROCERY,2026-04-11T11:16:00
...
```

**Data Characteristics:**
- 30 sample records
- Risk scores ranging from 15 to 95
- Risk indicators: HIGH (≥75), MEDIUM (50-74), LOW (25-49), UNKNOWN (<25)
- Transaction amounts: $300 to $20,000
- Various merchant categories

---

## 🔌 API Integration

The pipeline pushes risk trigger data to REST API endpoints:

### Request Configuration
```yaml
request:
  uri: "https://digital-servicing-api.us.bank-dns.com/digital/servicing/risk-triggers"
  method: "POST"
  headers:
    - Content-Type: application/json
    - Correlation-ID: risk-signal-trigger
    - Application-ID: risk-trigger
authType: "MTLS"
```

### Payload Example
```json
{
  "customerLpid": "LPID001",
  "custcardTokens": "TOKEN_4523xxxx",
  "riskScore": 85.50,
  "riskTriggerDate": "2026-04-11T10:30:00",
  "riskIndicator": "HIGH",
  "transactionAmount": 5000.00,
  "transactionDate": "2026-04-11",
  "merchantCategory": "RETAIL",
  "processedAt": "2026-04-11T10:31:00"
}
```

### Error Handling
- Rate Limit: 2000 records/second
- Retry Attempts: 3 (with exponential backoff)
- Timeout: 10 seconds per request

---

## 📊 Monitoring & Logging

### Log Levels
- **INFO**: Standard operational messages
- **WARN**: Non-critical issues
- **ERROR**: Critical failures
- **DEBUG**: Detailed diagnostic information

### Log Output
```
2026-04-12 10:15:30 - BankCustomerRiskTriggersETL - INFO - Starting ETL Pipeline
2026-04-12 10:15:31 - BankCustomerRiskTriggersETL - INFO - SparkSession created
2026-04-12 10:15:32 - BankCustomerRiskTriggersETL - INFO - Extracted 30 records
2026-04-12 10:15:33 - BankCustomerRiskTriggersETL - INFO - Transformation completed
2026-04-12 10:15:34 - BankCustomerRiskTriggersETL - INFO - Loading to PostgreSQL
2026-04-12 10:15:36 - BankCustomerRiskTriggersETL - INFO - ETL Pipeline completed
```

### Data Quality Metrics
The pipeline tracks:
- Total records processed
- Duplicate records found
- Null value occurrences
- Data quality score
- Processing duration

---

## 🧪 Testing

### Run Sample Pipeline
```bash
# Run with sample CSV data
python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \
  --config pipelines/configs/bank_cust_risk_triggers_config.yaml \
  --date 2026-04-12 \
  --mode test
```

### Verify Data in PostgreSQL
```sql
-- Connect to PostgreSQL
psql -h localhost -U postgres -d bank_db

-- Check loaded data
SELECT COUNT(*) FROM cust_risk_triggers;
SELECT * FROM cust_risk_triggers ORDER BY processedAt DESC LIMIT 10;

-- View data quality metrics
SELECT * FROM v_daily_risk_summary ORDER BY processDate DESC LIMIT 5;

-- Identify high-risk customers
SELECT * FROM v_high_risk_customers LIMIT 10;
```

### Query Examples
```sql
-- Customers with risk score > 75
SELECT customerLpid, riskScore, riskIndicator 
FROM cust_risk_triggers 
WHERE riskScore > 75 
ORDER BY riskScore DESC;

-- Daily transaction summary
SELECT 
  transactionDate, 
  COUNT(*) as transaction_count,
  AVG(transactionAmount) as avg_amount,
  SUM(transactionAmount) as total_amount
FROM cust_risk_triggers 
GROUP BY transactionDate 
ORDER BY transactionDate DESC;

-- Risk distribution
SELECT 
  riskIndicator, 
  COUNT(*) as count, 
  AVG(riskScore) as avg_score
FROM cust_risk_triggers 
GROUP BY riskIndicator;
```

---

## 🛠️ Troubleshooting

### Issue: "Synapse extraction failed"
**Solution**: The pipeline automatically falls back to CSV format
```bash
# Ensure CSV file is in the configured path
ls -la bank_data_cust_risk_triggers.csv
```

### Issue: "PostgreSQL connection refused"
**Solution**: Start PostgreSQL container
```bash
docker-compose -f docker-compose-bank-etl.yaml up postgres-bank
```

### Issue: "Out of memory error"
**Solution**: Increase Spark memory
```bash
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g
```

### Issue: "API push timeout"
**Solution**: Check network connectivity and increase timeout
```yaml
# In config file
tgtSysProp:
  retryAttempts: 5
  retryDelayMs: 2000
```

---

## 📈 Performance Optimization

### Spark Configuration Tips
```python
# In bank_cust_risk_triggers_etl.py
spark.config("spark.sql.shuffle.partitions", "10")      # Match cluster size
spark.config("spark.sql.adaptive.enabled", "true")       # Adaptive query execution
spark.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.config("spark.default.parallelism", "10")
spark.config("spark.sql.broadcastTimeout", "300")        # For large joins
```

### Best Practices
1. **Partitioning**: Distribute data across partitions for parallel processing
2. **Caching**: Cache intermediate DataFrames if reused
3. **Optimization**: Use column selection to reduce data transfer
4. **Indexing**: Create indexes on frequently queried columns in PostgreSQL

---

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Scala Spark API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/)

---

## 📞 Support

For issues or questions:
1. Check logs in: `/var/log/spark/btot-8999-syn-api-cust-risk-tg`
2. Review PostgreSQL logs: `docker logs postgres-bank`
3. Check Spark Master UI: `http://localhost:8080`

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🔐 Security Notes

- ⚠️ Never commit `.env` files with credentials
- ⚠️ Use MTLS certificates for API communication
- ⚠️ Encrypt sensitive data in transit
- ⚠️ Restrict database user permissions
- ⚠️ Enable audit logging for compliance

---

**Last Updated**: April 12, 2026
**Version**: 1.0.0
**Status**: Production Ready

