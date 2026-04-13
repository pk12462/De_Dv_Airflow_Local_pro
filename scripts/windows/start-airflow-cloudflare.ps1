$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$airflowDir = Join-Path $root "airflow"

Set-Location $airflowDir

if (-not (Test-Path ".env.cloudflare")) {
    throw "Missing airflow/.env.cloudflare. Copy airflow/.env.cloudflare.example and set CLOUDFLARE_TUNNEL_TOKEN"
}

$cfEnv = Get-Content ".env.cloudflare" | Where-Object {$_ -and $_ -notmatch '^\s*#'}
$tokenLine = $cfEnv | Where-Object {$_ -match '^CLOUDFLARE_TUNNEL_TOKEN='} | Select-Object -First 1
$hostLine = $cfEnv | Where-Object {$_ -match '^AIRFLOW_PUBLIC_HOSTNAME='} | Select-Object -First 1

if (-not $tokenLine -or $tokenLine -match 'replace-with-your-tunnel-token$') {
    throw "Set CLOUDFLARE_TUNNEL_TOKEN in airflow/.env.cloudflare"
}

if (-not $hostLine) {
    throw "Set AIRFLOW_PUBLIC_HOSTNAME in airflow/.env.cloudflare"
}

docker compose --env-file .env -f docker-compose.yaml up -d airflow-init
docker compose --env-file .env --env-file .env.cloudflare -f docker-compose.yaml -f docker-compose.cloudflare.yaml up -d
docker compose --env-file .env --env-file .env.cloudflare -f docker-compose.yaml -f docker-compose.cloudflare.yaml up -d airflow-triggerer airflow-dag-processor

docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml ps

Write-Host "Waiting for Airflow local health endpoint..."
powershell -ExecutionPolicy Bypass -File "$root\scripts\windows\wait-airflow-ready.ps1" -TimeoutSeconds 240

$publicHost = ($hostLine -split '=',2)[1]
Write-Host "Airflow public URL (via Cloudflare): https://$publicHost"

