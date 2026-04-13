$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$airflowDir = Join-Path $root "airflow"

Set-Location $airflowDir

if (-not (Test-Path ".env.cloudflare")) {
    throw "Missing airflow/.env.cloudflare. Copy airflow/.env.cloudflare.example and set CLOUDFLARE_TUNNEL_TOKEN"
}

docker compose --env-file .env -f docker-compose.yaml up -d airflow-init
docker compose --env-file .env --env-file .env.cloudflare -f docker-compose.yaml -f docker-compose.cloudflare.yaml up -d

docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml ps

