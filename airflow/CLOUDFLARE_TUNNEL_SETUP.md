# Cloudflare Tunnel Setup (Airflow)

## Important

- Airflow, Postgres, and pipeline apps **do not run on Cloudflare Workers**.
- They continue to run in Docker on your machine/server.
- Cloudflare Tunnel provides secure HTTPS access to your Airflow UI.

## Prerequisites

- A Cloudflare account
- A domain in Cloudflare DNS
- Existing tunnel created in Cloudflare Zero Trust
- Tunnel public hostname mapped to `http://airflow-webserver:8080`

## 1) Prepare env file

Create `airflow/.env.cloudflare` from `airflow/.env.cloudflare.example` and set:

- `CLOUDFLARE_TUNNEL_TOKEN`
- `AIRFLOW_PUBLIC_HOSTNAME` (use a unique DNS name for this project)
- `AIRFLOW__WEBSERVER__BASE_URL` (must match `https://<AIRFLOW_PUBLIC_HOSTNAME>`)

Example unique hostname:

- `airflow-dedv-pavan-2026.example.com`

## 2) Start Airflow + Tunnel

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
docker compose --env-file .env -f docker-compose.yaml up -d airflow-init
docker compose --env-file .env --env-file .env.cloudflare -f docker-compose.yaml -f docker-compose.cloudflare.yaml up -d
```

## 3) Verify

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml ps
docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml logs --no-color cloudflared
```

## 4) Secure Access (recommended)

In Cloudflare Zero Trust:
- Add Access policy on the Airflow hostname
- Require email/IdP login + MFA
- Restrict to your org domain/users

## 5) ETL Reliability Before Scheduler Trigger

Run preflight checks before enabling always-on scheduler runs:

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
powershell -ExecutionPolicy Bypass -File scripts\windows\preflight-etl.ps1
```

## Stop stack

```powershell
Set-Location "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro\airflow"
docker compose -f docker-compose.yaml -f docker-compose.cloudflare.yaml down
```

