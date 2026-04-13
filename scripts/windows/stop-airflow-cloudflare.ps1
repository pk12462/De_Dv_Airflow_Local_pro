$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$airflowDir = Join-Path $root "airflow"

Set-Location $airflowDir

docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml down

