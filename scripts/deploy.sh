#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# SPP Workers — Build & GKE Deployment Script (mirroring spp-factory-foundation)
# Target:
#   GCP Project : microdrive-dev
#   GKE Cluster : md-dev (us-east1)
#   Node Pool   : spp-pool (workload=spp, dedicated=spp:NoSchedule)
#   Namespace   : spp-platform
#   Registry    : us-east1-docker.pkg.dev/microdrive-dev/spp
#   Runner KSA  : spp-tf-runner -> spp-deploy@microdrive-dev.iam.gserviceaccount.com
# ---------------------------------------------------------------------------

PROJECT_ID="${GCP_PROJECT_ID:-microdrive-dev}"
REGION="${GCP_REGION:-us-east1}"
CLUSTER="${GKE_CLUSTER_NAME:-md-dev}"
NAMESPACE="${SPP_K8S_NAMESPACE:-spp-platform}"
REGISTRY="${IMAGE_REGISTRY:-us-east1-docker.pkg.dev/${PROJECT_ID}/spp}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || echo v1.0.0)}"

echo "=================================================================="
echo "SPP Workers Deployment"
echo "Project     : ${PROJECT_ID}"
echo "Cluster     : ${CLUSTER} (${REGION})"
echo "Namespace   : ${NAMESPACE}"
echo "Registry    : ${REGISTRY}/spp-workers:${IMAGE_TAG}"
echo "Runner SA   : spp-tf-runner -> spp-deploy@microdrive-dev.iam.gserviceaccount.com"
echo "=================================================================="

echo "[1/4] Submitting Cloud Build to build & push spp-workers..."
if [[ "${SKIP_BUILD:-false}" != "true" ]]; then
  gcloud builds submit \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --config=cloudbuild.yaml \
    --substitutions="_REGISTRY=${REGISTRY},_TAG=${IMAGE_TAG}" \
    .
else
  echo "SKIP_BUILD=true: using existing image ${REGISTRY}/spp-workers:${IMAGE_TAG}"
fi

echo "[2/4] Fetching GKE cluster credentials for ${CLUSTER}..."
gcloud container clusters get-credentials "${CLUSTER}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --dns-endpoint

echo "[3/4] Syncing AlloyDB password from Secret Manager ('spp-alloydb-postgres-password') into Kubernetes Secret 'spp-alloydb-credentials' in '${NAMESPACE}'..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
ALLOYDB_PW="$(gcloud secrets versions access latest --secret=spp-alloydb-postgres-password --project="${PROJECT_ID}" | tr -d '\r\n')"
kubectl create secret generic spp-alloydb-credentials \
  --namespace="${NAMESPACE}" \
  --from-literal=postgres-password="${ALLOYDB_PW}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[4/4] Running Helm upgrade --install for spp-workers in namespace ${NAMESPACE}..."
helm upgrade --install spp-workers ./deploy/helm/spp-workers \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --set images.registry="${REGISTRY}" \
  --set images.tag="${IMAGE_TAG}" \
  --wait \
  --timeout 10m

echo "=================================================================="
echo "Deployment complete. Checking pods and CronJobs in ${NAMESPACE}:"
kubectl get pods,sa,cronjobs,jobs -n "${NAMESPACE}"
echo "=================================================================="
