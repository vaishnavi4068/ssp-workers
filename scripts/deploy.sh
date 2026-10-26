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
#   Images      : spp-workers:<tag>, spp-tf-runner:<runner-tag>
#   Runner KSA  : spp-tf-runner -> spp-deploy@microdrive-dev.iam.gserviceaccount.com
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT_ID="${GCP_PROJECT_ID:-microdrive-dev}"
REGION="${GCP_REGION:-us-east1}"
CLUSTER="${GKE_CLUSTER_NAME:-md-dev}"
NAMESPACE="${SPP_K8S_NAMESPACE:-spp-platform}"
REGISTRY="${IMAGE_REGISTRY:-us-east1-docker.pkg.dev/${PROJECT_ID}/spp}"
IMAGE_TAG="${IMAGE_TAG:-$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo v1.0.0)}"
RUNNER_IMAGE_TAG="${RUNNER_IMAGE_TAG:-${IMAGE_TAG}}"
VALUES_FILE="${VALUES_FILE:-${REPO_ROOT}/deploy/values-md-dev.yaml}"
CHART_DIR="${CHART_DIR:-${REPO_ROOT}/deploy/helm/spp-workers}"

if [[ ! -f "${VALUES_FILE}" ]]; then
  echo "ERROR: Helm values file not found at ${VALUES_FILE}" >&2
  exit 1
fi

HELM_SET_ARGS=(
  --set "images.registry=${REGISTRY}"
  --set "images.tag=${IMAGE_TAG}"
  --set "images.runnerTag=${RUNNER_IMAGE_TAG}"
)

if [[ "${DRY_RUN:-false}" == "true" || "${1:-}" == "--dry-run" || "${1:-}" == "--template" ]]; then
  echo "[dry-run] Validating Helm chart and rendering templates with ${VALUES_FILE}..."
  helm lint "${CHART_DIR}" -f "${VALUES_FILE}" "${HELM_SET_ARGS[@]}"
  helm template spp-workers "${CHART_DIR}" \
    --namespace "${NAMESPACE}" \
    -f "${VALUES_FILE}" \
    "${HELM_SET_ARGS[@]}"
  exit 0
fi

echo "=================================================================="
echo "SPP Workers & Terraform Runner Deployment"
echo "Project     : ${PROJECT_ID}"
echo "Cluster     : ${CLUSTER} (${REGION})"
echo "Namespace   : ${NAMESPACE}"
echo "Values File : ${VALUES_FILE}"
echo "Worker Img  : ${REGISTRY}/spp-workers:${IMAGE_TAG}"
echo "Runner Img  : ${REGISTRY}/spp-tf-runner:${RUNNER_IMAGE_TAG}"
echo "Runner SA   : spp-tf-runner -> spp-deploy@microdrive-dev.iam.gserviceaccount.com"
echo "=================================================================="

echo "[0/4] Validating Helm template locally with ${VALUES_FILE}..."
helm template spp-workers "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  -f "${VALUES_FILE}" \
  "${HELM_SET_ARGS[@]}" >/dev/null

echo "[1/4] Submitting Cloud Build to build & push spp-workers and spp-tf-runner..."
if [[ "${SKIP_BUILD:-false}" != "true" ]]; then
  gcloud builds submit \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --config="${REPO_ROOT}/cloudbuild.yaml" \
    --substitutions="_REGISTRY=${REGISTRY},_TAG=${IMAGE_TAG}" \
    "${REPO_ROOT}"
else
  echo "SKIP_BUILD=true: using existing images spp-workers:${IMAGE_TAG} and spp-tf-runner:${RUNNER_IMAGE_TAG}"
fi

echo "[2/4] Fetching GKE cluster credentials for ${CLUSTER}..."
gcloud container clusters get-credentials "${CLUSTER}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --dns-endpoint

echo "[3/4] Syncing AlloyDB password from Secret Manager ('spp-alloydb-postgres-password') into Kubernetes Secret 'spp-alloydb-credentials' in '${NAMESPACE}'..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
(
  set +x
  ALLOYDB_PW="$(gcloud secrets versions access latest --secret=spp-alloydb-postgres-password --project="${PROJECT_ID}" | tr -d '\r\n')"
  if [[ -z "${ALLOYDB_PW}" ]]; then
    echo "ERROR: Retrieved empty password from Secret Manager ('spp-alloydb-postgres-password')." >&2
    exit 1
  fi
  kubectl create secret generic spp-alloydb-credentials \
    --namespace="${NAMESPACE}" \
    --from-literal=postgres-password="${ALLOYDB_PW}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  unset ALLOYDB_PW
)
echo "Synced Kubernetes Secret 'spp-alloydb-credentials' in namespace '${NAMESPACE}'."

echo "[4/4] Running Helm upgrade --install for spp-workers in namespace ${NAMESPACE}..."
helm upgrade --install spp-workers "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --reset-then-reuse-values \
  -f "${VALUES_FILE}" \
  "${HELM_SET_ARGS[@]}" \
  --wait \
  --timeout 10m

echo "=================================================================="
echo "Deployment complete. Checking pods and CronJobs in ${NAMESPACE}:"
kubectl get pods,sa,cronjobs,jobs -n "${NAMESPACE}"
echo "=================================================================="
