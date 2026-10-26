# `ssp-workers` — SPP Temporal Workers, GCS Module (`modules/gcs-bucket`), and Drift Job

Implements backlog items **WI-002** (Temporal workers & K8s Job runner), **WI-004** (GCS bucket Terraform module at `modules/gcs-bucket`), and **WI-005** (independent alert-only drift detection job) from `github.com/vaishnavi4068/ssp-cp` `docs/backlog/` against specifications in `github.com/vaishnavi4068/ssp-doc` (`ADR-SVC-001`, `ADR-AUT-001..003`, `ADR-GLB-004/006/007/008`, `specs/examples/gcs_bucket.gates.json`, `specs/data-model/spp_state_model.sql`).

---

## Architecture & Scope Summary

1. **Temporal Workers (`workers/`, WI-002):**
   - **`GcsBucketProvisionWorkflow`** (default task queue `spp-gcs-provisioning` matching `ssp-cp`), **`RequestWorkflow`** (`req-<request_id>`), **`ApprovalGateWorkflow`** (`apr-<request_id>`), and **`StateUnitReconcilerWorkflow`** (`su-gcp-<project_id>`).
   - Accepts both `approved_spec` (dispatched by `ssp-cp`) and `approved_doc` payload keys.
   - Uses **real GCS** (`RealGcsObjectStore` via `google-cloud-storage` + Workload Identity on `gs://<spp-factory-host-project-id>-spp-tfstate` with `if_generation_match=0`) and **real AlloyDB** (`AlloyDbStateStore` via `psycopg` connecting to `10.80.140.2:5432`, db `spp_platform`, schema `idp`, password from `SPP_ALLOYDB_PASSWORD` / K8s Secret `spp-alloydb-credentials`) whenever configured, keeping `InMemoryStateStore` only for unit/replay tests.
   - Launches real deterministic Kubernetes Jobs `tf-plan-<20hex>` and `tf-apply-<20hex>` in namespace `spp-platform` under KSA `spp-tf-runner` (`iam.gke.io/gcp-service-account: spp-deploy@<spp-factory-host-project-id>.iam.gserviceaccount.com`), reattaching idempotently on `409 AlreadyExists`, waiting for completion, and reading `plan.json` and logs from GCS.
   - Evaluates Terraform `plan.json` against `modules/gcs-bucket/policies` via `conftest` (`INV-4`) and waits for human approval signal (`approve_apply` / `human_approval`) before running `tf-apply-<20hex>`.
   - Records `Observed` state in `idp.observation` and upserts `< 2000`-byte non-sensitive projections in `idp.resource_observed` only when `projection_sha256` changes.
   - **Rollback:** Restores and re-applies the last approved specification as a new `Expected` revision (`rev-(N+1)`).
   - **Decommission:** Refuses non-empty buckets (`bucket_not_empty`) and never force-deletes (`force_destroy = false`, `GCS-DEL-001`).

2. **Terraform Runner Image (`Dockerfile.runner`, `workers/runner_entrypoint.py`):**
   - Dedicated runner image (`us-east1-docker.pkg.dev/<spp-factory-host-project-id>/spp/spp-tf-runner:<tag>`, resolved from `SPP_RUNNER_IMAGE` / `SPP_RUNNER_IMAGE_TAG` rather than `:latest`) with `terraform` (`1.10.5`, `>= 1.9`), `conftest` (`v0.56.0`), `opa` (`v0.69.0`), `google-cloud-cli` (`gcloud`/`gsutil`), and `modules/gcs-bucket` baked in.
   - Inside each Job: downloads Expected `terraform.tfvars.json` from GCS, runs `terraform init` against `gs://<spp-factory-host-project-id>-spp-tfstate` (`prefix=projects/<project_id>`), runs `terraform plan -out=planfile -detailed-exitcode`, `terraform show -json planfile > plan.json`, uploads `plan.json` and `planfile.tfplan` to GCS, runs `conftest test` on `plan.json`, and in `apply` Jobs applies the saved `planfile` and uploads logs to `gs://<spp-factory-host-project-id>-spp-tfstate/projects/<project_id>/logs/<job_name>.log`.

3. **Terraform GCS Bucket Module (`modules/gcs-bucket/`, WI-004):**
   - Enforces `location = "us-east1"` only (`ADR-SVC-001 GR-1`), CMEK encryption (`projects/<spp-factory-host-project-id>/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek` with 90-day rotation), `uniform_bucket_level_access = true`, `public_access_prevention = "enforced"`, `versioning`, optional `lifecycle_rules` defaulting to `[]`, optional `retention_policy` (`is_locked = false` by default, `GCS-RET-003`), `force_destroy = false` (`GCS-DEL-001`), and the **8 mandatory labels** (`sppappid` matching `^apm[0-9]{5}$`, `costcenter`, `environmenttype` in `nonprod|prod`, `primarylob`, `datasensitivity`, `owner`, `provisionedby`, `idp_resource_id`; forbidding `environmentsubtype`).

4. **Independent Drift Detection Job (`workers/drift.py`, `drift-<project_id>`, WI-005):**
   - Reads live bucket state via GCP Storage API (`read_live_gcs_bucket_via_gcp_api`), writes snapshot to `gs://<spp-factory-host-project-id>-spp-tfstate/projects/<project_id>/observed/scan-<window>/plan.json` without advancing `state_serial`, compares `Expected` vs. `Observed` by JSON pointer minus `drift_ignore_paths` (`['/force_destroy']`), and records `idp.drift_finding` rows in **strictly alert-only mode** (`status = 'OPEN'`, zero auto-remediation).

---

## Runner & Worker Image Build Instructions

Build and push both `spp-workers:<tag>` and `spp-tf-runner:<tag>` via Cloud Build (`cloudbuild.yaml`), then upgrade the Helm release in `spp-platform`:

```bash
IMAGE_TAG="$(git rev-parse --short HEAD)"

# 1. Build & push both spp-workers:$IMAGE_TAG and spp-tf-runner:$IMAGE_TAG via Cloud Build
gcloud builds submit \
  --project=<spp-factory-host-project-id> \
  --region=us-east1 \
  --config=cloudbuild.yaml \
  --substitutions="_REGISTRY=us-east1-docker.pkg.dev/<spp-factory-host-project-id>/spp,_TAG=${IMAGE_TAG}" \
  .

# 2. Or run the full deployment script (syncs spp-alloydb-postgres-password and runs helm upgrade --install)
IMAGE_TAG="${IMAGE_TAG}" ./scripts/deploy.sh
```
