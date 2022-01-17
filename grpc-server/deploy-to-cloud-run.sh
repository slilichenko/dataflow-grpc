set -u
set -e

gcloud run deploy zip-resolver --max-instances 2 --use-http2 --image gcr.io/dataflow-jdbc-sync/zip-resolver --region us-central1 --allow-unauthenticated
