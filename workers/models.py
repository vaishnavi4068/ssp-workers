"""Deterministic naming, canonical JSON hashing, and Expected/Observed projection utilities (ADR-GLB-003..009, ADR-SVC-001, ADR-AUT-001)."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any

STATE_BUCKET = "microdrive-dev-spp-tfstate"
K8S_NAMESPACE = "spp-platform"
RUNNER_KSA = "spp-tf-runner"
RUNNER_GSA = "spp-deploy@microdrive-dev.iam.gserviceaccount.com"
DEFAULT_KMS_KEY_NAME = "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek"
MAX_PROJECTION_BYTES = 2000

MANDATORY_LABELS = (
    "sppappid",
    "costcenter",
    "environmenttype",
    "primarylob",
    "datasensitivity",
    "owner",
    "provisionedby",
    "idp_resource_id",
)

SECRET_KEY_NAMES = frozenset(
    {
        "password",
        "passwd",
        "client_secret",
        "private_key",
        "secret_value",
        "api_key",
        "token",
    }
)


def canonical_json(doc: Any) -> str:
    """Serialize a JSON-compatible object to canonical JSON (sorted keys, compact separators, UTF-8)."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_sha256(doc: Any) -> str:
    """Compute SHA-256 hex digest over canonical JSON (ADR-GLB-003 §4)."""
    return hashlib.sha256(canonical_json(doc).encode("utf-8")).hexdigest()


def build_operation_id(state_unit_id: str, revision_no: int, phase: str, run_key: str = "1") -> str:
    """Format operation_id = 'op:<state_unit_id>:r<revision_no>:<phase>:<run_key>' (ADR-AUT-001 §2)."""
    if phase not in {"plan", "apply", "refresh_plan", "destroy_plan", "import"}:
        raise ValueError(f"Unsupported tf_operation phase: {phase}")
    return f"op:{state_unit_id}:r{int(revision_no)}:{phase}:{run_key}"


def k8s_job_name(operation_id: str, phase: str) -> str:
    """Twin of SQL idp.k8s_job_name(p_operation_id, p_phase) (ADR-AUT-001 §2).

    Formula: 'tf-' + phase.replace('_', '-') + '-' + sha256(operation_id)[:20]
    """
    digest = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()[:20]
    name = f"tf-{phase.replace('_', '-')}-{digest}"
    if len(name) > 63:
        raise ValueError(f"Derived k8s_job_name exceeds 63 characters: {name}")
    return name


def format_revision_dir(revision_no: int) -> str:
    """Format revision directory name as rev-NNNNNN (6-digit zero-padded) or rev-N."""
    return f"rev-{int(revision_no):06d}"


def build_artifact_uri(project_id: str, revision_no: int, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/expected/rev-NNNNNN/terraform.tfvars.json."""
    rev_dir = format_revision_dir(revision_no)
    return f"gs://{state_bucket}/projects/{project_id}/expected/{rev_dir}/terraform.tfvars.json"


def build_manifest_uri(project_id: str, revision_no: int, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/expected/rev-NNNNNN/manifest.json."""
    rev_dir = format_revision_dir(revision_no)
    return f"gs://{state_bucket}/projects/{project_id}/expected/{rev_dir}/manifest.json"


def build_plan_uri(project_id: str, revision_no: int, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/expected/rev-NNNNNN/plan.json."""
    rev_dir = format_revision_dir(revision_no)
    return f"gs://{state_bucket}/projects/{project_id}/expected/{rev_dir}/plan.json"


def build_state_uri(project_id: str, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/default.tfstate."""
    return f"gs://{state_bucket}/projects/{project_id}/default.tfstate"


def build_logs_uri(project_id: str, job_name: str, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/logs/<k8s_job_name>.log."""
    return f"gs://{state_bucket}/projects/{project_id}/logs/{job_name}.log"


def build_snapshot_uri(project_id: str, window: str, state_bucket: str = STATE_BUCKET) -> str:
    """Return gs://microdrive-dev-spp-tfstate/projects/<project_id>/observed/scan-<window>/plan.json."""
    return f"gs://{state_bucket}/projects/{project_id}/observed/scan-{window}/plan.json"


def normalize_label_value(raw: str) -> str:
    """Normalize a GCP label value per LBL-002 (lowercase, replace [^a-z0-9_-] with '_', max 63 chars)."""
    lowered = raw.strip().lower()
    replaced = re.sub(r"[^a-z0-9_-]", "_", lowered)
    return replaced[:63]


def render_tf_address(tf_address_template: str, *, name: str, instance: str | None = None) -> str:
    """Render tf_address from resource_kind.tf_address_template (e.g., module.gcs_bucket[\"{name}\"])."""
    addr = tf_address_template.replace("{name}", name)
    if instance is not None:
        addr = addr.replace("{instance}", instance)
    return addr


def convert_approved_gcs_bucket_to_expected_entry(approved_doc: dict[str, Any]) -> dict[str, Any]:
    """Convert an Approved gcs_bucket document into the Expected terraform.tfvars.json entry (ADR-SVC-001 §3).

    Conversions:
    - retention_days (e.g. 90) + retention_locked (false) -> retention_policy: {retention_period: days * 86400, is_locked: false}
      (omitted/null if versioning is true)
    - soft_delete_retention_days (e.g. 7) -> soft_delete_policy: {retention_duration_seconds: days * 86400}
    - lifecycle_rules: default []; if condition.age_days is present, convert to condition.age (days)
    - kms_key_name -> default_kms_key_name
    - autoclass: false
    - access array -> authoritative per-role iam_bindings map
    - force_destroy: false (never force-destroy)
    """
    metadata = approved_doc.get("metadata") or {}
    spec = approved_doc.get("spec") or {}

    project = metadata.get("project_id") or spec.get("project")
    bucket_name = spec["name"]
    location = spec.get("location", "us-east1")
    if location != "us-east1":
        raise ValueError(f"GCS-LOC-001: bucket location must be 'us-east1', got {location!r}")

    versioning = bool(spec.get("versioning", False))
    force_destroy = bool(spec.get("force_destroy", False))
    if force_destroy:
        raise ValueError("GCS-DEL-001: force_destroy must be false")

    retention_days = spec.get("retention_days", 90)
    retention_locked = bool(spec.get("retention_locked", False))
    if retention_locked:
        raise ValueError("GCS-RET-003: retention_locked must be false by default")

    retention_policy: dict[str, Any] | None = None
    if not versioning and retention_days is not None:
        retention_Policy_seconds = int(retention_days) * 86400
        retention_policy = {
            "retention_period": retention_Policy_seconds,
            "is_locked": retention_locked,
        }

    soft_delete_days = int(spec.get("soft_delete_retention_days", 7))
    soft_delete_policy = {
        "retention_duration_seconds": soft_delete_days * 86400,
    }

    raw_rules = spec.get("lifecycle_rules") or []
    converted_rules: list[dict[str, Any]] = []
    for rule in raw_rules:
        rule_copy = copy.deepcopy(rule)
        cond = rule_copy.get("condition") or {}
        if "age_days" in cond:
            age_val = cond.pop("age_days")
            if "age" not in cond and age_val is not None:
                cond["age"] = int(age_val)
        rule_copy["condition"] = cond
        converted_rules.append(rule_copy)

    iam_bindings: dict[str, list[str]] = {}
    if "iam_bindings" in spec and isinstance(spec["iam_bindings"], dict):
        for role, members in spec["iam_bindings"].items():
            iam_bindings[role] = list(members)
    else:
        for binding in spec.get("access") or []:
            role = binding["role"]
            members = list(binding.get("members") or [])
            if role in iam_bindings:
                for m in members:
                    if m not in iam_bindings[role]:
                        iam_bindings[role].append(m)
            else:
                iam_bindings[role] = members

    labels = dict(metadata.get("labels") or spec.get("labels") or {})
    if "environmentsubtype" in labels:
        raise ValueError("LBL-001: label 'environmentsubtype' is forbidden")
    for req_label in MANDATORY_LABELS:
        if not str(labels.get(req_label, "")).strip():
            raise ValueError(f"LBL-001: missing mandatory label {req_label!r}")
    if not re.match(r"^apm[0-9]{5}$", str(labels.get("sppappid", ""))):
        raise ValueError(
            f"LBL-001: label 'sppappid' ({labels.get('sppappid')!r}) must match ^apm[0-9]{{5}}$ (lowercase 'apm' + 5 digits)"
        )

    kms_key_name = (
        spec.get("default_kms_key_name")
        or spec.get("kms_key_name")
        or DEFAULT_KMS_KEY_NAME
    )

    entry: dict[str, Any] = {
        "project": project,
        "name": bucket_name,
        "location": location,
        "storage_class": spec.get("storage_class", "STANDARD"),
        "uniform_bucket_level_access": bool(spec.get("uniform_bucket_level_access", True)),
        "public_access_prevention": spec.get("public_access_prevention", "enforced"),
        "versioning": versioning,
        "autoclass": False,
        "default_kms_key_name": kms_key_name,
        "retention_policy": retention_policy,
        "soft_delete_policy": soft_delete_policy,
        "lifecycle_rules": converted_rules,
        "labels": labels,
        "iam_bindings": iam_bindings,
        "force_destroy": False,
    }
    return entry


def strip_sensitive_values(values: Any, sensitive_mask: Any) -> Any:
    """Recursively strip any paths flagged true in Terraform's sensitive_values structure (ADR-GLB-003 §3)."""
    if sensitive_mask is True:
        return None
    if isinstance(values, dict) and isinstance(sensitive_mask, dict):
        cleaned: dict[str, Any] = {}
        for k, v in values.items():
            if k in sensitive_mask:
                if sensitive_mask[k] is True:
                    continue
                cleaned[k] = strip_sensitive_values(v, sensitive_mask[k])
            else:
                if k.lower() in SECRET_KEY_NAMES:
                    continue
                cleaned[k] = strip_sensitive_values(v, False)
        return cleaned
    if isinstance(values, list) and isinstance(sensitive_mask, list):
        return [
            strip_sensitive_values(v, sensitive_mask[idx] if idx < len(sensitive_mask) else False)
            for idx, v in enumerate(values)
            if not (idx < len(sensitive_mask) and sensitive_mask[idx] is True)
        ]
    if isinstance(values, dict):
        return {
            k: strip_sensitive_values(v, False)
            for k, v in values.items()
            if k.lower() not in SECRET_KEY_NAMES
        }
    return copy.deepcopy(values)


def build_gcs_bucket_observed_projection(
    raw_bucket: dict[str, Any],
    *,
    sensitive_mask: Any = None,
    tf_address: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Build a non-sensitive <= 2 KB (< 2000 bytes) Observed projection and its canonical SHA-256 (ADR-GLB-003 REQ-6, ADR-SVC-001 §4)."""
    cleaned = strip_sensitive_values(raw_bucket, sensitive_mask or {})
    name = cleaned["name"]
    addr = tf_address or cleaned.get("tf_address") or render_tf_address('module.gcs_bucket["{name}"]', name=name)

    versioning_val = cleaned.get("versioning", False)
    if isinstance(versioning_val, list):
        versioning_bool = bool(versioning_val[0].get("enabled", False)) if versioning_val else False
    elif isinstance(versioning_val, dict):
        versioning_bool = bool(versioning_val.get("enabled", False))
    else:
        versioning_bool = bool(versioning_val)

    autoclass_val = cleaned.get("autoclass", False)
    if isinstance(autoclass_val, list):
        autoclass_bool = bool(autoclass_val[0].get("enabled", False)) if autoclass_val else False
    elif isinstance(autoclass_val, dict):
        autoclass_bool = bool(autoclass_val.get("enabled", False))
    else:
        autoclass_bool = bool(autoclass_val)

    kms_key = cleaned.get("default_kms_key_name")
    if not kms_key:
        enc = cleaned.get("encryption")
        if isinstance(enc, list) and enc:
            kms_key = enc[0].get("default_kms_key_name", "")
        elif isinstance(enc, dict):
            kms_key = enc.get("default_kms_key_name", "")

    ret_pol = cleaned.get("retention_policy")
    if isinstance(ret_pol, list):
        ret_pol = ret_pol[0] if ret_pol else None
    if isinstance(ret_pol, dict):
        ret_pol = {
            "retention_period": int(ret_pol.get("retention_period", 0)),
            "is_locked": bool(ret_pol.get("is_locked", False)),
        }

    soft_del = cleaned.get("soft_delete_policy")
    if isinstance(soft_del, list):
        soft_del = soft_del[0] if soft_del else {"retention_duration_seconds": 604800}
    if isinstance(soft_del, dict):
        soft_del = {
            "retention_duration_seconds": int(soft_del.get("retention_duration_seconds", 604800)),
        }

    projection: dict[str, Any] = {
        "exists": bool(cleaned.get("exists", True)),
        "tf_address": addr,
        "self_link": cleaned.get("self_link", f"https://www.googleapis.com/storage/v1/b/{name}"),
        "url": cleaned.get("url", f"gs://{name}"),
        "project_number": "<EXCLUDED>",
        "time_created": cleaned.get("time_created", ""),
        "project": cleaned.get("project", ""),
        "name": name,
        "location": str(cleaned.get("location", "us-east1")).lower(),
        "storage_class": cleaned.get("storage_class", "STANDARD"),
        "uniform_bucket_level_access": bool(cleaned.get("uniform_bucket_level_access", True)),
        "public_access_prevention": cleaned.get("public_access_prevention", "enforced"),
        "versioning": versioning_bool,
        "autoclass": autoclass_bool,
        "default_kms_key_name": kms_key or "",
        "retention_policy": ret_pol,
        "soft_delete_policy": soft_del,
        "lifecycle_rules": cleaned.get("lifecycle_rules") if cleaned.get("lifecycle_rules") is not None else [],
        "labels": dict(cleaned.get("labels") or {}),
        "iam_bindings": dict(cleaned.get("iam_bindings") or {}),
    }

    encoded = canonical_json(projection).encode("utf-8")
    if len(encoded) >= MAX_PROJECTION_BYTES:
        raise ValueError(
            f"resource_observed_projection_small violation: projection size {len(encoded)} bytes >= {MAX_PROJECTION_BYTES} bytes"
        )
    return projection, canonical_sha256(projection)
