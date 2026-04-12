# De_Dv_Airflow_Local_pro

A comprehensive Data Engineering Platform with Streaming Apps, Batch Apps, Pipelines, Docker/Helm, and Airflow 2.11 orchestration.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       De_Dv_Airflow_Local_pro                           │
│                                                                          │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────────────────────┐  │
│  │ streaming_apps│   │  batch_apps  │   │         pipelines           │  │
│  │  ┌─────────┐ │   │ ┌──────────┐ │   │ ┌─────────────────────────┐ │  │
│  │  │  Kafka  │ │   │ │  Spark   │ │   │ │  app_connection_configs  │ │  │
│  │  │Consumer │ │   │ │  Batch   │ │   │ ├─────────────────────────┤ │  │
│  │  │Producer │ │   │ ├──────────┤ │   │ │      base_code          │ │  │
│  │  ├─────────┤ │   │ │  Pandas  │ │   │ ├─────────────────────────┤ │  │
│  │  │  Spark  │ │   │ │  Batch   │ │   │ │      egress_rules       │ │  │
│  │  │Streaming│ │   │ ├──────────┤ │   │ ├─────────────────────────┤ │  │
│  │  └─────────┘ │   │ │   SQL    │ │   │ │     shield_values       │ │  │
│  └──────────────┘   │ │  Batch   │ │   │ └─────────────────────────┘ │  │
│                     │ └──────────┘ │   └─────────────────────────────┘  │
│                     └──────────────┘                                     │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │                          airflow/                                │    │
│  │   dags/streaming/  dags/batch/   plugins/operators/             │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │                          docker/                                 │    │
│  │   base/Dockerfiles   helm/charts   secrets/   k8s_services/     │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
De_Dv_Airflow_Local_pro/
├── streaming_apps/                     # Streaming application module
│   ├── kafka/
│   │   ├── consumer/                   # Kafka consumer implementations
│   │   ├── producer/                   # Kafka producer implementations
│   │   └── schemas/                    # Avro/JSON schema definitions
│   └── spark_streaming/               # Spark Structured Streaming apps
├── batch_apps/                         # Batch application module
│   ├── spark_batch/                   # Spark batch ETL jobs
│   ├── pandas_batch/                  # Pandas-based batch jobs
│   └── sql_batch/                     # SQL batch scripts (Extract/Transform/Load)
├── pipelines/                          # Pipeline orchestration configs
│   ├── configs/                        # App connection & pipeline configs (YAML)
│   ├── base/                           # Base pipeline classes & utilities
│   ├── egress/                         # Egress rules & enforcement
│   └── shield/                         # Data masking & PII shield values
├── airflow/                            # Airflow 2.11 setup
│   ├── dags/                           # DAG definitions
│   │   ├── streaming/                  # Streaming pipeline DAGs
│   │   └── batch/                      # Batch pipeline DAGs
│   ├── plugins/operators/              # Custom Airflow operators
│   ├── config/                         # Airflow configuration
│   └── docker-compose.yaml            # Airflow 2.11 stack
├── docker/                             # Docker & Kubernetes manifests
│   ├── base/                           # Dockerfiles
│   ├── helm/                           # Helm chart
│   ├── secrets/                        # K8s secret templates
│   └── k8s_services/                  # K8s service manifests
├── requirements.txt
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Helm 3.x
- kubectl (for K8s deployments)

### 1. Clone and Setup Environment

```bash
git clone <repo-url>
cd De_Dv_Airflow_Local_pro

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your actual values
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Airflow 2.11 Stack

```bash
cd airflow
# Initialize Airflow metadata DB
docker-compose up airflow-init
# Start all services
docker-compose up -d
# Access Airflow UI at http://localhost:8080 (admin/admin)
```

### 4. Deploy with Helm (Kubernetes)

```bash
cd docker/helm
helm dependency update
helm install de-dv-platform . -f values.yaml --namespace de-dv --create-namespace
```

### 5. Run a Streaming App Locally

```bash
python -m streaming_apps.spark_streaming.spark_streaming_app \
  --config pipelines/configs/streaming_pipeline_config.yaml
```

### 6. Run a Batch App Locally

```bash
python -m batch_apps.spark_batch.spark_batch_app \
  --config pipelines/configs/batch_pipeline_config.yaml \
  --date 2026-04-12
```

---

## Airflow DAGs

| DAG ID | Schedule | Description |
|--------|----------|-------------|
| `kafka_streaming_dag` | `@continuous` | Kafka consumer/producer pipeline |
| `spark_streaming_dag` | `@hourly` | Spark Structured Streaming job |
| `spark_batch_dag` | `@daily` | Spark batch ETL pipeline |
| `pandas_batch_dag` | `@daily` | Pandas lightweight batch job |
| `sql_batch_dag` | `@daily` | SQL extract → transform → load |

---

## Environment Variables

See `.env.example` for the full list. Key variables:

| Variable | Description |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses |
| `SPARK_MASTER_URL` | Spark master URL |
| `DATABASE_URL` | PostgreSQL connection string |
| `AWS_S3_BUCKET` | S3 bucket for data storage |
| `AIRFLOW__CORE__FERNET_KEY` | Airflow encryption key |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Airflow metadata DB |

---

## Shield & Egress

- **Egress Rules** (`pipelines/egress/egress_rules.yaml`): Whitelisted destinations, rate limits, protocol restrictions
- **Shield Values** (`pipelines/shield/shield_values.yaml`): PII field definitions, masking strategies (hash/redact/tokenize)

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `pytest tests/`
4. Submit a pull request

