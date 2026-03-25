param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = "us-central1",
    [string]$Schedule = "0 */6 * * *",
    [string]$TimeZone = "America/Los_Angeles",
    [string]$EnvVarsFile = "cloudrun.env.yaml"
)

$ErrorActionPreference = "Stop"

if (-not $ProjectId) {
    throw "Debes indicar -ProjectId o definir GCP_PROJECT_ID."
}

$JobName = "appointment-pipeline"
$ServiceAccount = "$JobName@$ProjectId.iam.gserviceaccount.com"
$SchedulerJobName = "$JobName-schedule"
$SchedulerUri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/$JobName`:run"

Write-Host "=== Deploying Appointment Pipeline to Cloud Run Jobs ==="
Write-Host "Project: $ProjectId"
Write-Host "Region: $Region"
Write-Host "Job: $JobName"

Write-Host "1. Building Docker image..."
gcloud builds submit --tag "gcr.io/$ProjectId/$JobName" .

Write-Host "2. Deploying Cloud Run Job..."
$deployArgs = @(
    "run", "jobs", "deploy", $JobName,
    "--image", "gcr.io/$ProjectId/$JobName",
    "--region", $Region,
    "--service-account", $ServiceAccount,
    "--memory", "2Gi",
    "--cpu", "2",
    "--task-timeout", "3600",
    "--max-retries", "1",
    "--tasks", "1"
)

if (Test-Path $EnvVarsFile) {
    Write-Host "Using env vars file: $EnvVarsFile"
    $deployArgs += @("--env-vars-file", $EnvVarsFile)
}
else {
    Write-Host "Env vars file not found; using only GCP_PROJECT_ID"
    $deployArgs += @("--set-env-vars", "GCP_PROJECT_ID=$ProjectId")
}

& gcloud @deployArgs

Write-Host "3. Creating or updating Cloud Scheduler..."
$schedulerExists = $true
try {
    gcloud scheduler jobs describe $SchedulerJobName --location $Region | Out-Null
}
catch {
    $schedulerExists = $false
}

if ($schedulerExists) {
    gcloud scheduler jobs update http $SchedulerJobName `
        --schedule $Schedule `
        --uri $SchedulerUri `
        --location $Region `
        --time-zone $TimeZone `
        --http-method POST `
        --oauth-service-account-email $ServiceAccount `
        --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" | Out-Null
}
else {
    gcloud scheduler jobs create http $SchedulerJobName `
        --schedule $Schedule `
        --uri $SchedulerUri `
        --location $Region `
        --time-zone $TimeZone `
        --http-method POST `
        --oauth-service-account-email $ServiceAccount `
        --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" | Out-Null
}

Write-Host ""
Write-Host "=== Deployment complete! ==="
Write-Host "Cloud Run Job: $JobName"
Write-Host "Scheduler job: $SchedulerJobName"
Write-Host "Manual execution:"
Write-Host "gcloud run jobs execute $JobName --region $Region --project $ProjectId"
