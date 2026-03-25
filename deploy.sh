#!/bin/bash

set -e

PROJECT_ID=${1:-$GCP_PROJECT_ID}
REGION=${2:-us-central1}
JOB_NAME="appointment-pipeline"
SERVICE_ACCOUNT="$JOB_NAME@$PROJECT_ID.iam.gserviceaccount.com"
SCHEDULER_JOB_NAME="${JOB_NAME}-schedule"
SCHEDULE=${SCHEDULE:-"0 */6 * * *"}
TIME_ZONE=${TIME_ZONE:-"America/Los_Angeles"}
ENV_VARS_FILE=${CLOUD_RUN_ENV_FILE:-cloudrun.env.yaml}

echo "=== Deploying Appointment Pipeline to Cloud Run Jobs ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Job: $JOB_NAME"

echo "1. Building Docker image..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$JOB_NAME .

echo "2. Deploying to Cloud Run Job..."
DEPLOY_ARGS=(
  "$JOB_NAME"
  --image "gcr.io/$PROJECT_ID/$JOB_NAME"
  --region "$REGION"
  --service-account "$SERVICE_ACCOUNT"
  --memory 2Gi
  --cpu 2
  --timeout 3600
  --max-retries 1
  --tasks 1
)

if [ -f "$ENV_VARS_FILE" ]; then
  echo "Using env vars file: $ENV_VARS_FILE"
  DEPLOY_ARGS+=(--env-vars-file "$ENV_VARS_FILE")
else
  echo "Env vars file not found, using only GCP_PROJECT_ID"
  DEPLOY_ARGS+=(--set-env-vars "GCP_PROJECT_ID=$PROJECT_ID")
fi

gcloud run jobs deploy "${DEPLOY_ARGS[@]}"

SCHEDULER_URI="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/$JOB_NAME:run"

echo "3. Creating or updating Cloud Scheduler..."
gcloud scheduler jobs describe $SCHEDULER_JOB_NAME --location $REGION >/dev/null 2>&1 && EXISTS=1 || EXISTS=0

if [ "$EXISTS" -eq 1 ]; then
  gcloud scheduler jobs update http $SCHEDULER_JOB_NAME \
    --schedule "$SCHEDULE" \
    --uri "$SCHEDULER_URI" \
    --location $REGION \
    --time-zone "$TIME_ZONE" \
    --http-method POST \
    --oauth-service-account-email $SERVICE_ACCOUNT \
    --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
else
  gcloud scheduler jobs create http $SCHEDULER_JOB_NAME \
    --schedule "$SCHEDULE" \
    --uri "$SCHEDULER_URI" \
    --location $REGION \
    --time-zone "$TIME_ZONE" \
    --http-method POST \
    --oauth-service-account-email $SERVICE_ACCOUNT \
    --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
fi

echo "=== Deployment complete! ==="
echo "Cloud Run Job: $JOB_NAME"
echo "Scheduler job: $SCHEDULER_JOB_NAME"
echo "Manual execution:"
echo "gcloud run jobs execute $JOB_NAME --region $REGION --project $PROJECT_ID"
