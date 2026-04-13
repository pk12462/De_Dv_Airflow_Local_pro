$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$airflowDir = Join-Path $root "airflow"

Set-Location $airflowDir

$dagIds = docker compose exec -T airflow-scheduler airflow dags list --output plain | Select-String "_dag" | ForEach-Object { ($_ -split "\s+")[0] }

foreach ($dagId in $dagIds) {
    if ($dagId) {
        docker compose exec -T airflow-scheduler airflow dags unpause $dagId | Out-Null
        Write-Host "Unpaused DAG: $dagId"
    }
}

