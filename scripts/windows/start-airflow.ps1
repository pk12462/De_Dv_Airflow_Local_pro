$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$airflowDir = Join-Path $root "airflow"

Set-Location $airflowDir

docker compose up -d airflow-init
docker compose up -d postgres airflow-webserver airflow-scheduler

docker compose ps

