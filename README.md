# `ssp-workers` — SPP Temporal Workers, GCS Module (`modules/gcs-bucket`), and Drift Job

Implements backlog items **WI-002** (Temporal workers & K8s Job runner), **WI-004** (GCS bucket Terraform module, placed at `modules/gcs-bucket` for now and to be split into `ssp-module-gcs-bucket` later), and **WI-005** (independent alert-only drift detection job) from `github.com/vaishnavi4068/ssp-cp` `docs/backlog/` against specifications in `github.com/vaishnavi4068/ssp-doc` (`ADR-SVC-001`, `ADR-AUT-001..003`, `ADR-GLB-004/006/007/008`, `specs/examples/gcs_bucket.gates.json`, `specs/data-model/spp_state_model.sql`).

---

## Architecture & Scope Summary

1. **Temporal Workers (`workers/`, WI-002):**
   - **`GcsBucketProvisionWorkflow`**, **`RequestWorkflow`** (`req-<request_id>`), **`ApprovalGateWorkflow`** (`apr-<request_id>`), and **`StateUnitReconcilerWorkflow`** (`su-gcp-<project_id>`).
   - Renders create-only `Expected` revisions (`manifest.json` + `terraform.tfvars.json`, `tfvars_collection = "gcs_buckets"`, `tf_address = 'module.gcs_bucket["{name}"]'`, seconds conversions `retention_days * 86400` and `soft_delete_retention_days * 86400`) to `gs://microdrive-dev-spp-tfstate/projects/<project>/expected/rev-NNNNNN/` with `ifGenerationMatch=0` (`INV-5`).
   - Launches deterministic Kubernetes Jobs `tf-plan-<20hex>` and `tf-apply-<20hex>` in namespace `spp-platform` (`k8s_job_name(operation_id, phase)` matching SQL `idp.k8s_job_name`), reattaching idempotently on `409 AlreadyExists` (`INV-7`).
   - Evaluates Terraform `plan.json` against the Rego policy bundle via `conftest` (`INV-4`) and requires an explicit human approval signal before `apply`.
   - Records `Observed` state in `idp.observation` and upserts `< 2000`-byte non-sensitive projections in `idp.resource_observed` only when `projection_sha256` changes.
   - **Rollback:** Restores and re-applies the last approved specification as a new `Expected` revision (`rev-(N+1)`).
   - **Decommission:** Refuses non-empty buckets (`bucket_not_empty`) and never force-deletes (`force_destroy = false`, `GCS-DEL-001`).

2. **Terraform GCS Bucket Module (`modules/gcs-bucket/`, WI-004):**
   - Enforces `location = "us-east1"` only (`ADR-SVC-001 GR-1`), CMEK encryption (`kms_key_name` / `default_kms_key_name` in `locations/us-east1`, default `projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek` with 90-day rotation), `uniform_bucket_level_access = true`, `public_access_prevention = "enforced"`, `versioning`, optional `lifecycle_rules` defaulting to `[]`, optional `retention_policy` (`is_locked = false` by default, `GCS-RET-003`), `force_destroy = false` (`GCS-DEL-001`), and the **8 mandatory labels** (`sppappid` matching `^apm[0-9]{5}$` [lowercase `apm` + 5 digits, from `APM` + 5 digits], `costcenter`, `environmenttype` in `nonprod|prod`, `primarylob`, `datasensitivity`, `owner`, `provisionedby`, `idp_resource_id`; forbidding `environmentsubtype`).
   - GCS backend configured for `gs://microdrive-dev-spp-tfstate`.

3. **Independent Drift Detection Job (`workers/drift.py`, `drift-<project_id>`, WI-005):**
   - Runs `tf-refresh-plan-<20hex>` / reads live bucket state via GCP Storage API, writes snapshot to `gs://microdrive-dev-spp-tfstate/projects/<project_id>/observed/scan-<window>/plan.json` without advancing `state_serial`, compares `Expected` vs. `Observed` by JSON pointer minus `drift_ignore_paths` (`['/force_destroy']`), and records `idp.drift_finding` rows in **strictly alert-only mode** (`status = 'OPEN'`, zero auto-remediation).

4. **Deployment (`Dockerfile`, `cloudbuild.yaml`, `deploy/helm/spp-workers/`):**
   - Cloud Build publishes `us-east1-docker.pkg.dev/microdrive-dev/spp/spp-workers:<sha>`.
   - Helm chart targets namespace `spp-platform` on GKE `md-dev` (`nodeSelector: workload=spp`, toleration `dedicated=spp:NoSchedule`) with Kubernetes ServiceAccount `spp-tf-runner` annotated with `iam.gke.io/gcp-service-account: spp-deploy@microdrive-dev.iam.gserviceaccount.com` (Workload Identity, no keys).
   - Connects to live AlloyDB (`10.80.140.2:5432`, database `spp_platform`, user `postgres`) with password synced at deploy time from Secret Manager secret `spp-alloydb-postgres-password` into K8s Secret `spp-alloydb-credentials` (zero secrets committed in repo).

---

## Deploy Prerequisites (Operator Checklist)

1. **Workload Identity IAM Binding (`spp-deploy@microdrive-dev.iam.gserviceaccount.com`):**
   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     spp-deploy@microdrive-dev.iam.gserviceaccount.com \
     --project=microdrive-dev \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:microdrive-dev.svc.id.goog[spp-platform/spp-tf-runner]"

   gcloud iam service-accounts add-iam-policy-binding \
     spp-deploy@microdrive-dev.iam.gserviceaccount.com \
     --project=microdrive-dev \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:microdrive-dev.svc.id.goog[spp-platform/spp-workers]"
   ```
2. **State Bucket & Per-Project Executor Impersonation:**
   - Ensure `gs://microdrive-dev-spp-tfstate` exists in `us-east1` with object versioning and CMEK (`projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek`) enabled, and grant `spp-deploy@microdrive-dev.iam.gserviceaccount.com` `roles/storage.objectAdmin` on `gs://microdrive-dev-spp-tfstate`.
   - Grant `spp-deploy@microdrive-dev.iam.gserviceaccount.com` `roles/iam.serviceAccountTokenCreator` on target project executor service accounts (`idp-tf@<project_id>.iam.gserviceaccount.com`).
3. **Cloud KMS CMEK Key (`projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek`):**
   - Configured with 90-day rotation in `us-east1`; GCS service agent and `spp-deploy@microdrive-dev.iam.gserviceaccount.com` already have `roles/cloudkms.cryptoKeyEncrypterDecrypter`.
4. **Temporal Namespace & Live AlloyDB (`spp_platform` at `10.80.140.2:5432`):**
   - Register Temporal namespace `platform` on `temporal-frontend.spp-temporal.svc.cluster.local:7233`.
   - Live AlloyDB (`10.80.140.2:5432`, db `spp_platform`, user `postgres`) reads its password from GCP Secret Manager secret `spp-alloydb-postgres-password` (synced to K8s Secret `spp-alloydb-credentials` by `scripts/deploy.sh`).
