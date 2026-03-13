#!/bin/bash

set -e

PROJECT_ID=${1:-$GCP_PROJECT_ID}
REGION=${2:-us-central1}
SERVICE_NAME="appointment-pipeline"

echo "=== Deploying Appointment Pipeline to Cloud Run ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"

echo "1. Building Docker image..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME .

echo "2. Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
    --region $REGION \
    --platform managed \
    --service-account $SERVICE_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --memory 2Gi \
    --cpu 2 \
    --timeout 3600 \
    --allow-unauthenticated \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID

echo "3. Setting up Cloud Scheduler..."
gcloud scheduler jobs create http $SERVICE_NAME-cron \
    --schedule "0 6 * * *" \
    --uri "https://$REGION-run.googleapis.com/apis/v1/projects/$PROJECT_ID/services/$SERVICE_NAME:run" \
    --location $REGION \
    --time-zone "America/New_York" \
    --oidc-service-account-email $SERVICE_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    2>/dev/null || echo "Scheduler job may already exist"

echo "=== Deployment complete! ==="
echo "Service URL: https://$SERVICE_NAME-$PROJECT_ID.$REGION.run.app"
