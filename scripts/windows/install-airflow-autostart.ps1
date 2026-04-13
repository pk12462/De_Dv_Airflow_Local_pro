$ErrorActionPreference = "Stop"

$root = "C:\Users\PAVAN\Local_pro\De_Dv_Airflow_Local_pro"
$startScript = Join-Path $root "scripts\windows\start-airflow.ps1"
$waitScript = Join-Path $root "scripts\windows\wait-airflow-ready.ps1"
$unpauseScript = Join-Path $root "scripts\windows\unpause-pipeline-dags.ps1"

$cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$startScript`"; powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$waitScript`"; powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$unpauseScript`""

schtasks /Create /TN "AirflowAutoStart" /TR $cmd /SC ONLOGON /RL HIGHEST /F

Write-Host "Created Task Scheduler entry: AirflowAutoStart"

