# ETL Project (Learning Framework)

Config-driven ETL scaffold for learning and experimentation:

`SOURCE -> TRANSFORMATION -> ENRICHMENT -> TARGET -> LOGGING`

## Structure

- `appconfig/dev/` runtime configs (`global.json`, `connections.json`, `pipeline.json`)
- `queries/` SQL source queries
- `schema/` schema specs
- `deployment/` environment and egress YAMLs
- `spark-job/pyspark.py` config-driven execution engine

## Quick Try (dry run)

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
python etl-project\spark-job\pyspark.py --env dev --dry-run
```

## Local Spark Run (if PySpark available)

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
python etl-project\spark-job\pyspark.py --env dev
```

## Local Batch + Streaming Pipelines

New learning pipelines use local files as source and write to PostgreSQL and Cassandra:

- Batch config: `etl-project/appconfig/dev/pipeline_batch_local_pg_cassandra.json`
- Streaming config: `etl-project/appconfig/dev/pipeline_streaming_local_pg_cassandra.json`
- Runner: `etl-project/spark-job/local_batch_streaming_runner.py`

Run batch pipeline (dry-run):

```powershell
python etl-project/spark-job/local_batch_streaming_runner.py --config etl-project/appconfig/dev/pipeline_batch_local_pg_cassandra.json --dry-run
```

Run streaming pipeline (dry-run):

```powershell
python etl-project/spark-job/local_batch_streaming_runner.py --config etl-project/appconfig/dev/pipeline_streaming_local_pg_cassandra.json --dry-run
```

Airflow DAGs:

- `airflow/dags/batch/local_batch_pg_cassandra_dag.py`
- `airflow/dags/streaming/local_streaming_pg_cassandra_dag.py`
