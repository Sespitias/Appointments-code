param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = "us-central1"
)

$ErrorActionPreference = "Stop"

if (-not $ProjectId) {
    throw "Debes indicar -ProjectId o definir GCP_PROJECT_ID."
}

$JobName = "appointment-pipeline"
$ServiceAccount = "$JobName@$ProjectId.iam.gserviceaccount.com"

Write-Host "=== Setting up GCP resources for Appointment Pipeline ==="
Write-Host "Project: $ProjectId"
Write-Host "Region: $Region"

Write-Host "1. Enabling required APIs..."
gcloud services enable `
    run.googleapis.com `
    cloudbuild.googleapis.com `
    cloudscheduler.googleapis.com `
    bigquery.googleapis.com `
    sheets.googleapis.com `
    drive.googleapis.com `
    --project $ProjectId

Write-Host "2. Creating service account..."
try {
    gcloud iam service-accounts create $JobName `
        --display-name "Appointment Pipeline" `
        --project $ProjectId | Out-Null
}
catch {
    Write-Host "Service account may already exist"
}

Write-Host "3. Granting permissions..."
$roles = @(
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/run.invoker",
    "roles/storage.objectViewer"
)

foreach ($role in $roles) {
    gcloud projects add-iam-policy-binding $ProjectId `
        --member="serviceAccount:$ServiceAccount" `
        --role="$role" `
        --quiet | Out-Null
}

Write-Host "4. Optional: creating Cloud Build GitHub trigger..."
if ($env:REPO_OWNER) {
    try {
        gcloud builds triggers create github `
            --repo-name=appointment-pipeline `
            --repo-owner=$env:REPO_OWNER `
            --branch-pattern="main" `
            --build-config="cloudbuild.yaml" `
            --substitutions="_REGION=$Region" `
            --project $ProjectId | Out-Null
    }
    catch {
        Write-Host "Trigger creation skipped or already exists"
    }
}
else {
    Write-Host "REPO_OWNER no definido; omitiendo trigger de Cloud Build"
}

Write-Host ""
Write-Host "=== Setup complete! ==="
Write-Host "Next steps:"
Write-Host "1. Copia cloudrun.env.example.yaml a cloudrun.env.yaml y complétalo"
Write-Host "2. Ejecuta .\deploy.ps1 -ProjectId $ProjectId -Region $Region"
Write-Host "3. Ejecuta manualmente: gcloud run jobs execute $JobName --region $Region --project $ProjectId"
