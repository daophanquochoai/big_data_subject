$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot

function Invoke-Docker {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker failed with exit code $LASTEXITCODE" }
}

$envPath = Join-Path $projectRoot '.env.training'
if (-not (Test-Path -LiteralPath $envPath)) {
    $randomBytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $generator.GetBytes($randomBytes)
    $generator.Dispose()
    $workerToken = [Convert]::ToBase64String($randomBytes)
    [IO.File]::WriteAllText($envPath, "TRAINING_WORKER_TOKEN=$workerToken`nCMAPSS_DATA_DIR=./.local/cmapss`n", (New-Object System.Text.UTF8Encoding($false)))
}

Invoke-Docker -DockerArgs @('build', '-t', 'pdm-v2', 'spark/docker')
# The same Python environment is available on every machine with Docker.
Invoke-Docker -DockerArgs @('run', '--rm', '-v', "${projectRoot}:/workspace", 'pdm-v2', 'python', '/workspace/training/prepare_runtime_data.py', '/workspace')
Invoke-Docker -DockerArgs @('build', '-t', 'pdm-training:local', 'training')
Invoke-Docker -DockerArgs @('compose', '--env-file', '.env.training', 'up', '-d', '--build', 'postgres', 'kafka', 'spark-master', 'spark-worker-1', 'spark-worker-2', 'airflow', 'sensor', 'training-control')
Invoke-Docker -DockerArgs @('compose', '--env-file', '.env.training', 'exec', '-T', 'airflow', 'python', '/training/configure_airflow.py')
Write-Host 'Training UI: http://localhost:8001/'
Write-Host 'Airflow:     http://localhost:8088'
Write-Host 'Spark:       http://localhost:8080'
