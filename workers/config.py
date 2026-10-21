"""Runtime configuration for SSP Temporal Workers and Drift Job (ADR-CLD-GCP-001, ADR-GLB-005)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote_plus


def _read_secret_file(secrets_dir: str, *names: str) -> str | None:
    base = Path(secrets_dir)
    if not base.is_dir():
        return None
    for name in names:
        candidate = base / name
        if candidate.is_file():
            val = candidate.read_text(encoding="utf-8").strip()
            if val:
                return val
    return None


def resolve_database_url(secrets_dir: str = "/etc/spp-alloydb-credentials") -> str:
    """Resolve PostgreSQL / AlloyDB connection URL from env or mounted K8s secret."""
    explicit_url = os.getenv("DATABASE_URL")
    if explicit_url:
        # Normalize SQLAlchemy driver prefixes if passed (e.g. postgresql+psycopg2:// -> postgresql://)
        if explicit_url.startswith("postgresql+psycopg2://"):
            return "postgresql://" + explicit_url[len("postgresql+psycopg2://") :]
        if explicit_url.startswith("postgresql+psycopg://"):
            return "postgresql://" + explicit_url[len("postgresql+psycopg://") :]
        return explicit_url

    alloy_host = os.getenv("SPP_ALLOYDB_HOST") or os.getenv("ALLOYDB_HOST")
    alloy_pw = (
        os.getenv("SPP_ALLOYDB_PASSWORD")
        or os.getenv("ALLOYDB_PASSWORD")
        or _read_secret_file(
            secrets_dir,
            "postgres-password",
            "spp-alloydb-postgres-password",
            "alloydb-password",
        )
        or _read_secret_file(
            "/etc/spp-cp-secrets",
            "postgres-password",
            "spp-alloydb-postgres-password",
            "alloydb-password",
        )
    )
    if alloy_host and alloy_pw:
        alloy_port = os.getenv("SPP_ALLOYDB_PORT") or os.getenv("ALLOYDB_PORT", "5432")
        alloy_db = os.getenv("SPP_ALLOYDB_DB") or os.getenv("ALLOYDB_DB", "spp_platform")
        alloy_user = os.getenv("SPP_ALLOYDB_USER") or os.getenv("ALLOYDB_USER", "postgres")
        alloy_sslmode = os.getenv("SPP_ALLOYDB_SSLMODE") or os.getenv("ALLOYDB_SSLMODE", "require")
        auth_part = f"{quote_plus(alloy_user)}:{quote_plus(alloy_pw)}"
        return f"postgresql://{auth_part}@{alloy_host}:{alloy_port}/{alloy_db}?sslmode={alloy_sslmode}"

    return ""


def is_real_db_configured() -> bool:
    """Return True when real AlloyDB / PostgreSQL is enabled via env or credentials."""
    flag = os.getenv("SPP_WORKER_USE_DB")
    if flag is not None:
        return flag.lower() == "true"
    return bool(resolve_database_url())


def is_real_gcs_configured() -> bool:
    """Return True when real Google Cloud Storage is enabled."""
    flag = os.getenv("SPP_WORKER_USE_GCS")
    if flag is not None:
        return flag.lower() == "true"
    # If running in Kubernetes (SPP_WORKER_USE_K8S=true), default to real GCS unless explicitly disabled
    return os.getenv("SPP_WORKER_USE_K8S", "false").lower() == "true"


def is_real_k8s_configured() -> bool:
    """Return True when real Kubernetes Job execution in spp-platform is enabled."""
    return os.getenv("SPP_WORKER_USE_K8S", "false").lower() == "true"


def resolve_runner_image() -> str:
    """Resolve the Terraform runner container image using configured registry, repo, and deployed tag (never :latest by default when tag is set)."""
    explicit_image = os.getenv("SPP_RUNNER_IMAGE", "").strip()
    tag = (
        os.getenv("SPP_RUNNER_IMAGE_TAG")
        or os.getenv("SPP_IMAGE_TAG")
        or os.getenv("IMAGE_TAG")
        or "v1.0.0"
    ).strip()
    if explicit_image:
        if ":" in explicit_image.rsplit("/", 1)[-1]:
            base, current_tag = explicit_image.rsplit(":", 1)
            if current_tag == "latest" and tag and tag != "latest":
                return f"{base}:{tag}"
            return explicit_image
        return f"{explicit_image}:{tag}"

    registry = os.getenv("SPP_IMAGE_REGISTRY", "us-east1-docker.pkg.dev/microdrive-dev/spp").rstrip("/")
    repo = os.getenv("SPP_RUNNER_IMAGE_REPO", "spp-tf-runner").strip("/")
    return f"{registry}/{repo}:{tag}"


@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str = field(
        default_factory=lambda: os.getenv(
            "TEMPORAL_ADDRESS",
            "temporal-frontend.spp-temporal.svc.cluster.local:7233",
        )
    )
    temporal_namespace: str = field(default_factory=lambda: os.getenv("TEMPORAL_NAMESPACE", "platform"))
    task_queue: str = field(default_factory=lambda: os.getenv("TEMPORAL_TASK_QUEUE", "spp-gcs-provisioning"))
    k8s_namespace: str = field(default_factory=lambda: os.getenv("SPP_K8S_NAMESPACE", "spp-platform"))
    runner_ksa: str = field(default_factory=lambda: os.getenv("SPP_RUNNER_KSA", "spp-tf-runner"))
    runner_gsa: str = field(
        default_factory=lambda: os.getenv(
            "SPP_RUNNER_GSA",
            "spp-deploy@microdrive-dev.iam.gserviceaccount.com",
        )
    )
    runner_image_tag: str = field(
        default_factory=lambda: os.getenv(
            "SPP_RUNNER_IMAGE_TAG",
            os.getenv("SPP_IMAGE_TAG", os.getenv("IMAGE_TAG", "v1.0.0")),
        )
    )
    runner_image: str = field(default_factory=resolve_runner_image)
    state_bucket: str = field(default_factory=lambda: os.getenv("SPP_STATE_BUCKET", "microdrive-dev-spp-tfstate"))
    default_kms_key_name: str = field(
        default_factory=lambda: os.getenv(
            "SPP_DEFAULT_KMS_KEY_NAME",
            "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek",
        )
    )
    gcp_project_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_PROJECT", "microdrive-dev"))
    gcp_region: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_LOCATION", "us-east1"))
    use_k8s: bool = field(default_factory=is_real_k8s_configured)
    use_gcs: bool = field(default_factory=is_real_gcs_configured)
    use_db: bool = field(default_factory=is_real_db_configured)
    alloydb_host: str = field(default_factory=lambda: os.getenv("SPP_ALLOYDB_HOST", "10.80.140.2"))
    alloydb_port: int = field(default_factory=lambda: int(os.getenv("SPP_ALLOYDB_PORT", "5432")))
    alloydb_db: str = field(default_factory=lambda: os.getenv("SPP_ALLOYDB_DB", "spp_platform"))
    alloydb_user: str = field(default_factory=lambda: os.getenv("SPP_ALLOYDB_USER", "postgres"))
    alloydb_password_secret: str = field(
        default_factory=lambda: os.getenv("SPP_ALLOYDB_PASSWORD_SECRET", "spp-alloydb-postgres-password")
    )
    database_url: str = field(default_factory=resolve_database_url)
    policy_dir: str = field(
        default_factory=lambda: os.getenv("SPP_POLICY_DIR", "/app/modules/gcs-bucket/policies")
    )
    job_poll_seconds: int = field(default_factory=lambda: int(os.getenv("SPP_JOB_POLL_SECONDS", "10")))
    job_active_deadline_seconds: int = field(
        default_factory=lambda: int(os.getenv("SPP_JOB_ACTIVE_DEADLINE_SECONDS", "1800"))
    )
