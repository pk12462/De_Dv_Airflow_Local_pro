# ETL Project (UDAIP-style)

Config-driven enterprise ETL scaffold aligned to the architecture:

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

