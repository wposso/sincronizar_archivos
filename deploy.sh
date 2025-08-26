#!/bin/bash

# Variables
PROJECT_ID="your-project-id"
SERVICE_NAME="drive-to-gcs-sync"
REGION="us-central1"

echo "🚀 Desplegando Drive to GCS Sync..."

# Construir imagen Docker
echo "📦 Construyendo imagen Docker..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME

# Desplegar en Cloud Run
echo "☁️ Desplegando en Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars="BUCKET_NAME=talenthub_central,ROOT_FOLDER_ID=1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD" \
  --memory 512Mi \
  --cpu 1 \
  --max-instances 3 \
  --timeout 300s

echo "✅ Despliegue completado!"
echo "🌐 URL del servicio:"
gcloud run services describe $SERVICE_NAME --region $REGION --format="value(status.url)"