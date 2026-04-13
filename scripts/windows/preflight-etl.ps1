$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"

Set-Location $root

# Quick confidence checks before enabling scheduler-driven runs.
python -m pytest -o addopts="" tests/test_connection_manager.py tests/test_local_batch_streaming_runner.py -q
python -m pytest -o addopts="" tests/test_smoke_configs.py tests/test_tech_modules.py -q

Write-Host "ETL preflight checks passed. Scheduler-based runs can be enabled."

