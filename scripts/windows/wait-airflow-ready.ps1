param(
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)

while ((Get-Date) -lt $deadline) {
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get -TimeoutSec 10
        if ($health.metadatabase.status -eq "healthy" -and $health.scheduler.status -eq "healthy") {
            Write-Host "Airflow is healthy"
            exit 0
        }
    }
    catch {
        # keep polling until timeout
    }

    Start-Sleep -Seconds 5
}

Write-Error "Airflow did not become healthy within $TimeoutSeconds seconds."
exit 1

