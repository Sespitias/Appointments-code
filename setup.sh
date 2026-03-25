#!/bin/bash

set -e

PROJECT_ID=${1:-$GCP_PROJECT_ID}
REGION=${2:-us-central1}
JOB_NAME="appointment-pipeline"
SERVICE_ACCOUNT="$JOB_NAME@$PROJECT_ID.iam.gserviceaccount.com"

echo "=== Setting up GCP resources for Appointment Pipeline ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"

echo "1. Enabling required APIs..."
gcloud services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    cloudscheduler.googleapis.com \
    bigquery.googleapis.com \
    sheets.googleapis.com \
    drive.googleapis.com \
    --project $PROJECT_ID

echo "2. Creating service account..."
gcloud iam service-accounts create $JOB_NAME \
    --display-name "Appointment Pipeline" \
    --project $PROJECT_ID \
    2>/dev/null || echo "Service account may already exist"

echo "3. Granting permissions..."
for ROLE in \
    roles/bigquery.dataEditor \
    roles/bigquery.jobUser \
    roles/run.invoker \
    roles/storage.objectViewer
do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="$ROLE" \
      --quiet >/dev/null
done

echo "4. Creating trigger for Cloud Build..."
gcloud builds triggers create github \
    --repo-name=appointment-pipeline \
    --repo-owner=${REPO_OWNER} \
    --branch-pattern="main" \
    --build-config="cloudbuild.yaml" \
    --substitutions="_REGION=$REGION" \
    --project $PROJECT_ID \
    2>/dev/null || echo "Trigger creation skipped (requires GitHub repo setup)"

echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "1. Create the required secrets and environment variables"
echo "2. Run: ./deploy.sh $PROJECT_ID $REGION"
echo "3. Trigger manually: gcloud run jobs execute $JOB_NAME --region $REGION"
