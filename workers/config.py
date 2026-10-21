"""Runtime configuration for SSP Temporal Workers and Drift Job (ADR-CLD-GCP-001, ADR-GLB-005)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str = os.getenv(
        "TEMPORAL_ADDRESS",
        "temporal-frontend.spp-temporal.svc.cluster.local:7233",
    )
    temporal_namespace: str = os.getenv("TEMPORAL_NAMESPACE", "platform")
    task_queue: str = os.getenv("TEMPORAL_TASK_QUEUE", "idp-reconciler-v1")
    k8s_namespace: str = os.getenv("SPP_K8S_NAMESPACE", "spp-platform")
    runner_ksa: str = os.getenv("SPP_RUNNER_KSA", "spp-tf-runner")
    runner_gsa: str = os.getenv(
        "SPP_RUNNER_GSA",
        "spp-deploy@microdrive-dev.iam.gserviceaccount.com",
    )
    runner_image: str = os.getenv(
        "SPP_RUNNER_IMAGE",
        "us-east1-docker.pkg.dev/microdrive-dev/spp/spp-workers:latest",
    )
    state_bucket: str = os.getenv("SPP_STATE_BUCKET", "microdrive-dev-spp-tfstate")
    default_kms_key_name: str = os.getenv(
        "SPP_DEFAULT_KMS_KEY_NAME",
        "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek",
    )
    gcp_project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT", "microdrive-dev")
    gcp_region: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-east1")
    use_k8s: bool = os.getenv("SPP_WORKER_USE_K8S", "false").lower() == "true"
    use_gcs: bool = os.getenv("SPP_WORKER_USE_GCS", "false").lower() == "true"
    use_db: bool = os.getenv("SPP_WORKER_USE_DB", "false").lower() == "true"
    alloydb_host: str = os.getenv("SPP_ALLOYDB_HOST", "10.80.140.2")
    alloydb_port: int = int(os.getenv("SPP_ALLOYDB_PORT", "5432"))
    alloydb_db: str = os.getenv("SPP_ALLOYDB_DB", "spp_platform")
    alloydb_user: str = os.getenv("SPP_ALLOYDB_USER", "postgres")
    alloydb_password_secret: str = os.getenv("SPP_ALLOYDB_PASSWORD_SECRET", "spp-alloydb-postgres-password")
    database_url: str = os.getenv("DATABASE_URL", "")
    policy_dir: str = os.getenv("SPP_POLICY_DIR", "/app/modules/gcs-bucket/policies")
    job_poll_seconds: int = int(os.getenv("SPP_JOB_POLL_SECONDS", "10"))
    job_active_deadline_seconds: int = int(os.getenv("SPP_JOB_ACTIVE_DEADLINE_SECONDS", "1800"))
