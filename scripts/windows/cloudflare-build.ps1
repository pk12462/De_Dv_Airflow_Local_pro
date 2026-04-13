$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
Set-Location $root

python -m pytest -o addopts="" tests/test_smoke_configs.py -q
Write-Host "Cloudflare pre-deploy build checks passed."

