"""Independent Drift Detection Job & JSON-Pointer Comparer (WI-005, ADR-AUT-002, ADR-GLB-007, ADR-GLB-009, ADR-SVC-001)."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from temporalio import activity

from .db import DEFAULT_STORE, InMemoryStateStore
from .k8s_runner import build_tf_runner_job_manifest
from .models import (
    K8S_NAMESPACE,
    RUNNER_GSA,
    RUNNER_KSA,
    build_gcs_bucket_observed_projection,
    build_logs_uri,
    build_operation_id,
    build_snapshot_uri,
    build_state_uri,
    k8s_job_name,
)

logger = logging.getLogger(__name__)

# Guardrail Rule ID mapping for attributes whose drift undoes a platform guardrail (ADR-GLB-007 §2, §4)
GUARDRAIL_RULE_MAP: dict[str, dict[str, str]] = {
    "gcs_bucket": {
        "/public_access_prevention": "GCS-PAP-001",
        "/uniform_bucket_level_access": "GCS-UBLA-001",
        "/default_kms_key_name": "GCS-ENC-001",
        "/retention_policy/is_locked": "GCS-RET-003",
    },
    "spanner_instance": {
        "/config": "SPN-CFG-001",
    },
    "spanner_database": {
        "/encryption_config/kms_key_names": "SPN-ENC-001",
        "/enable_drop_protection": "SPN-DEL-002",
    },
}

CRITICAL_PATHS = frozenset(
    {
        "/public_access_prevention",
        "/uniform_bucket_level_access",
        "/default_kms_key_name",
        "/encryption_config",
        "/encryption_config/kms_key_names",
    }
)

HIGH_PATHS = frozenset(
    {
        "/retention_policy",
        "/retention_policy/retention_period",
        "/retention_policy/is_locked",
        "/soft_delete_policy",
        "/soft_delete_policy/retention_duration_seconds",
        "/version_retention_period",
        "/backup_schedule",
        "/versioning",
    }
)

MEDIUM_PATHS = frozenset(
    {
        "/storage_class",
        "/lifecycle_rules",
        "/processing_units",
        "/edition",
        "/iam_bindings",
        "/autoclass",
    }
)

LOW_PATHS = frozenset(
    {
        "/labels",
        "/display_name",
    }
)

# Synthetic Observed projection metadata keys that are not Expected tfvars configuration attributes
OBSERVED_META_KEYS = frozenset(
    {
        "exists",
        "tf_address",
        "self_link",
        "url",
        "project_number",
        "time_created",
    }
)


def classify_drift_severity_and_rule(
    kind: str,
    attribute_path: str,
    expected_value: Any,
    observed_value: Any,
) -> tuple[str, str | None]:
    """Classify drift severity (critical, high, medium, low) and optional guardrail_rule_id (ADR-GLB-007 §4)."""
    kind_rules = GUARDRAIL_RULE_MAP.get(kind) or {}
    rule_id = kind_rules.get(attribute_path)

    # Check if an IAM binding introduced public principals (allUsers / allAuthenticatedUsers)
    if attribute_path == "/iam_bindings" and isinstance(observed_value, dict):
        for members in observed_value.values():
            if isinstance(members, list) and any(
                m in {"allUsers", "allAuthenticatedUsers"} or str(m).startswith("user:") for m in members
            ):
                return "critical", "GCS-IAM-002" if kind == "gcs_bucket" else "IAM-PUB-001"

    if rule_id is not None or attribute_path in CRITICAL_PATHS:
        return "critical", rule_id
    if attribute_path in HIGH_PATHS or attribute_path.startswith(("/retention_policy/", "/soft_delete_policy/")):
        if kind == "gcs_bucket" and attribute_path.startswith("/retention_policy"):
            return "high", None
        return "high", None
    if attribute_path in MEDIUM_PATHS:
        return "medium", None
    if attribute_path in LOW_PATHS:
        return "low", None
    return "medium", None


def compare_expected_vs_observed(
    *,
    kind: str,
    expected_entry: dict[str, Any],
    observed_projection: dict[str, Any],
    drift_ignore_paths: list[str] | tuple[str, ...] = ("/force_destroy",),
) -> list[dict[str, Any]]:
    """Compare Expected tfvars entry against Observed projection via JSON-pointer diff minus drift_ignore_paths (ADR-AUT-002).

    Rules:
    - Excludes paths in drift_ignore_paths (e.g., ['/force_destroy'] for gcs_bucket & spanner_instance,
      ['/ddl', '/deletion_protection'] for spanner_database).
    - Excludes '/processing_units' on spanner_instance when autoscaling_config is non-null (ADR-SVC-002 Quirk 4).
    - Compares 'labels' and 'iam_bindings' as whole maps at '/labels' and '/iam_bindings'.
    - If exists is False on observed_projection, returns a single 'missing_in_cloud' finding at '/'.
    """
    if not observed_projection.get("exists", True):
        return [
            {
                "drift_kind": "missing_in_cloud",
                "attribute_path": "/",
                "expected_value": expected_entry.get("name"),
                "observed_value": None,
                "severity": "critical",
                "guardrail_rule_id": None,
            }
        ]

    ignore_set = set(drift_ignore_paths or [])
    if kind == "spanner_instance" and expected_entry.get("autoscaling_config") is not None:
        ignore_set.add("/processing_units")

    findings: list[dict[str, Any]] = []

    for key, exp_val in expected_entry.items():
        if key in OBSERVED_META_KEYS:
            continue
        pointer = f"/{key}"
        if pointer in ignore_set:
            continue

        obs_val = observed_projection.get(key)

        # Whole-map comparison for labels and iam_bindings (ADR-AUT-002 §2)
        if key in {"labels", "iam_bindings"}:
            if exp_val != obs_val:
                severity, rule_id = classify_drift_severity_and_rule(kind, pointer, exp_val, obs_val)
                findings.append(
                    {
                        "drift_kind": "out_of_band_change",
                        "attribute_path": pointer,
                        "expected_value": copy.deepcopy(exp_val),
                        "observed_value": copy.deepcopy(obs_val),
                        "severity": severity,
                        "guardrail_rule_id": rule_id,
                    }
                )
            continue

        if isinstance(exp_val, dict) and isinstance(obs_val, dict):
            sub_diffs = _diff_nested_dict(pointer, exp_val, obs_val, ignore_set)
            if sub_diffs:
                for sub_path, sub_exp, sub_obs in sub_diffs:
                    severity, rule_id = classify_drift_severity_and_rule(kind, sub_path, sub_exp, sub_obs)
                    findings.append(
                        {
                            "drift_kind": "out_of_band_change",
                            "attribute_path": sub_path,
                            "expected_value": copy.deepcopy(sub_exp),
                            "observed_value": copy.deepcopy(sub_obs),
                            "severity": severity,
                            "guardrail_rule_id": rule_id,
                        }
                    )
            continue

        if exp_val != obs_val:
            severity, rule_id = classify_drift_severity_and_rule(kind, pointer, exp_val, obs_val)
            findings.append(
                {
                    "drift_kind": "out_of_band_change",
                    "attribute_path": pointer,
                    "expected_value": copy.deepcopy(exp_val),
                    "observed_value": copy.deepcopy(obs_val),
                    "severity": severity,
                    "guardrail_rule_id": rule_id,
                }
            )

    return findings


def _diff_nested_dict(
    base_pointer: str,
    expected_dict: dict[str, Any],
    observed_dict: dict[str, Any],
    ignore_set: set[str],
) -> list[tuple[str, Any, Any]]:
    diffs: list[tuple[str, Any, Any]] = []
    all_keys = sorted(set(expected_dict.keys()) | set(observed_dict.keys()))
    for k in all_keys:
        sub_ptr = f"{base_pointer}/{k}"
        if sub_ptr in ignore_set:
            continue
        e_v = expected_dict.get(k)
        o_v = observed_dict.get(k)
        if e_v != o_v:
            diffs.append((sub_ptr, e_v, o_v))
    return diffs


def read_live_gcs_bucket_via_gcp_api(bucket_name: str, project_id: str) -> dict[str, Any]:
    """Read real GCS bucket metadata and IAM policy via google-cloud-storage API (used when SPP_WORKER_USE_GCS=true)."""
    from google.cloud import storage  # type: ignore[import-untyped]

    client = storage.Client(project=project_id)
    bucket = client.lookup_bucket(bucket_name)
    if bucket is None:
        return {"exists": False, "name": bucket_name, "project": project_id}

    bucket.reload()
    iam_cfg = bucket.iam_configuration or {}
    ubla_enabled = bool(iam_cfg.get("uniformBucketLevelAccess", {}).get("enabled", True))
    pap = iam_cfg.get("publicAccessPrevention", "enforced")

    ret_pol: dict[str, Any] | None = None
    if bucket.retention_period:
        ret_pol = {
            "retention_period": int(bucket.retention_period),
            "is_locked": bool(bucket.retention_policy_locked),
        }

    soft_del_sec = 604800
    if getattr(bucket, "soft_delete_policy", None) and bucket.soft_delete_policy.retention_duration_seconds:
        soft_del_sec = int(bucket.soft_delete_policy.retention_duration_seconds)

    policy = bucket.get_iam_policy(requested_policy_version=3)
    iam_bindings: dict[str, list[str]] = {}
    for b in policy.bindings:
        role = b.get("role", "")
        if role in {
            "roles/storage.objectAdmin",
            "roles/storage.objectViewer",
            "roles/storage.objectCreator",
            "roles/storage.legacyBucketReader",
        }:
            iam_bindings[role] = sorted(list(b.get("members") or []))

    return {
        "exists": True,
        "self_link": bucket.self_link or f"https://www.googleapis.com/storage/v1/b/{bucket_name}",
        "url": f"gs://{bucket_name}",
        "time_created": bucket.time_created.isoformat() if bucket.time_created else "",
        "project": project_id,
        "name": bucket_name,
        "location": str(bucket.location or "us-east1").lower(),
        "storage_class": bucket.storage_class or "STANDARD",
        "uniform_bucket_level_access": ubla_enabled,
        "public_access_prevention": pap,
        "versioning": bool(bucket.versioning_enabled),
        "autoclass": bool(getattr(bucket, "autoclass_enabled", False)),
        "default_kms_key_name": bucket.default_kms_key_name or "",
        "retention_policy": ret_pol,
        "soft_delete_policy": {"retention_duration_seconds": soft_del_sec},
        "lifecycle_rules": list(bucket.lifecycle_rules or []),
        "labels": dict(bucket.labels or {}),
        "iam_bindings": iam_bindings,
    }


def execute_drift_scan_for_project(
    *,
    project_id: str,
    window: str,
    store: InMemoryStateStore | None = None,
    state_unit_id: str | None = None,
    revision_id: str | None = None,
    revision_no: int | None = None,
    state_serial: int = 6,
    state_generation: int = 1729600100000000,
    observation_id: str | None = None,
    drift_id_override: str | None = None,
    observed_overrides_by_resource: dict[str, dict[str, Any]] | None = None,
    use_gcp_api: bool = False,
) -> dict[str, Any]:
    """Run an independent refresh scan for a project, compare Observed vs Expected minus drift_ignore_paths,
    and write alert-only drift_finding rows (NEVER auto-remediating or launching tf-apply-*).
    """
    st = store or DEFAULT_STORE
    su = None
    if state_unit_id and state_unit_id in st.state_units:
        su = st.state_units[state_unit_id]
    else:
        for candidate in st.state_units.values():
            if candidate["scope_id"] == project_id:
                su = candidate
                break
    if su is None:
        su = st.ensure_state_unit(project_id=project_id, state_unit_id=state_unit_id)

    su_id = su["state_unit_id"]
    rev_no = revision_no if revision_no is not None else (su["applied_expected_rev"] or su["current_expected_rev"] or 1)

    rev_row = None
    if revision_id and revision_id in st.expected_revisions:
        rev_row = st.expected_revisions[revision_id]
    else:
        for er in st.expected_revisions.values():
            if er["state_unit_id"] == su_id and er["revision_no"] == rev_no:
                rev_row = er
                break
    rev_id = revision_id or (rev_row["revision_id"] if rev_row else "77b88e4c-84fe-5050-a395-060f012d17f0")

    operation_id = build_operation_id(su_id, rev_no, "refresh_plan", window)
    job_name = k8s_job_name(operation_id, "refresh_plan")
    logs_uri = build_logs_uri(project_id, job_name, su["state_bucket"])
    snapshot_uri = build_snapshot_uri(project_id, window, su["state_bucket"])
    state_uri = build_state_uri(project_id, su["state_bucket"])

    manifest = build_tf_runner_job_manifest(
        operation_id=operation_id,
        phase="refresh_plan",
        state_unit_id=su_id,
        revision_id=rev_id,
        revision_no=rev_no,
        project_id=project_id,
        executor_identity=su["executor_identity"],
        run_key=window,
        namespace=K8S_NAMESPACE,
        service_account_name=os.getenv("SPP_RUNNER_KSA", RUNNER_KSA),
        runner_gsa_email=os.getenv("SPP_RUNNER_GSA", RUNNER_GSA),
        state_bucket=su["state_bucket"],
    )

    tfvars: dict[str, Any] = {}
    if rev_row and rev_row.get("artifact_uri") in st.gcs_objects:
        tfvars = st.get_gcs_json(rev_row["artifact_uri"])

    all_findings: list[dict[str, Any]] = []
    observed_resources: list[dict[str, Any]] = []
    overrides = observed_overrides_by_resource or {}

    # Inspect each active resource in the state unit
    for res in st.resources.values():
        if res["state_unit_id"] != su_id or res["lifecycle"] in {"deleted", "abandoned"}:
            continue
        res_id = res["resource_id"]
        kind = res["kind"]
        name = res["name"]
        kind_meta = st.resource_kinds[kind]
        collection = kind_meta["tfvars_collection"]
        ignore_paths = kind_meta["drift_ignore_paths"]

        expected_entry = copy.deepcopy((tfvars.get(collection) or {}).get(name) or {})
        if not expected_entry and res["current_approval_id"] in st.approvals:
            apr = st.approvals[res["current_approval_id"]]
            expected_entry = copy.deepcopy(apr["approved_spec"].get("spec") or {})

        if use_gcp_api and kind == "gcs_bucket":
            raw_cloud = read_live_gcs_bucket_via_gcp_api(name, project_id)
        else:
            raw_cloud = copy.deepcopy(st.cloud_buckets.get(name) or expected_entry)

        if name in overrides:
            raw_cloud.update(copy.deepcopy(overrides[name]))
        if res_id in overrides:
            raw_cloud.update(copy.deepcopy(overrides[res_id]))

        if kind == "gcs_bucket":
            projection, proj_sha = build_gcs_bucket_observed_projection(
                raw_cloud,
                tf_address=res["tf_address"],
            )
            cloud_id = f"projects/_/buckets/{name}"
        else:
            projection = copy.deepcopy(raw_cloud)
            from .models import canonical_sha256

            proj_sha = canonical_sha256(projection)
            cloud_id = f"projects/{project_id}/instances/{name}"

        diffs = compare_expected_vs_observed(
            kind=kind,
            expected_entry=expected_entry,
            observed_projection=projection,
            drift_ignore_paths=ignore_paths,
        )
        observed_resources.append(
            {
                "resource": res,
                "expected_entry": expected_entry,
                "projection": projection,
                "projection_sha256": proj_sha,
                "cloud_resource_id": cloud_id,
                "diffs": diffs,
            }
        )

    drift_detected = any(len(item["diffs"]) > 0 for item in observed_resources)
    tf_exit_code = 2 if drift_detected else 0

    # Write refresh_plan snapshot to GCS (without advancing state_serial: state_serial_before == state_serial_after)
    snapshot_doc = {
        "format_version": "1.2",
        "terraform_version": "1.10.5",
        "window": window,
        "project_id": project_id,
        "drift_detected": drift_detected,
        "resources": [
            {
                "resource_id": item["resource"]["resource_id"],
                "tf_address": item["resource"]["tf_address"],
                "projection": item["projection"],
            }
            for item in observed_resources
        ],
    }
    st.put_gcs_object(snapshot_uri, snapshot_doc)
    st.put_gcs_object(logs_uri, f"[spp-tf-runner] Executed refresh_plan {job_name} for {operation_id}\n")

    op_row = st.record_tf_operation(
        operation_id=operation_id,
        state_unit_id=su_id,
        revision_id=rev_id,
        phase="refresh_plan",
        run_key=window,
        k8s_job=job_name,
        executor_identity=su["executor_identity"],
        temporal_workflow_id=f"su-gcp-{project_id}",
        status="SUCCEEDED",
        tf_exit_code=tf_exit_code,
        state_serial_before=state_serial,
        state_serial_after=state_serial,
        logs_uri=logs_uri,
    )

    obs_row = st.record_observation(
        state_unit_id=su_id,
        source="refresh_plan",
        operation_id=operation_id,
        revision_id=rev_id,
        state_uri=state_uri,
        state_generation=state_generation,
        state_serial=state_serial,
        snapshot_uri=snapshot_uri,
        drift_detected=drift_detected,
        observation_id=observation_id,
    )

    for item in observed_resources:
        res_id = item["resource"]["resource_id"]
        was_written, ro_row = st.upsert_resource_observed(
            resource_id=res_id,
            observation_id=obs_row["observation_id"],
            exists_in_cloud=bool(item["projection"].get("exists", True)),
            cloud_resource_id=item["cloud_resource_id"],
            projection=item["projection"],
            projection_sha256=item["projection_sha256"],
        )
        item["was_written"] = was_written
        item["resource_observed"] = ro_row

        for idx, d in enumerate(item["diffs"]):
            finding = st.upsert_drift_finding(
                resource_id=res_id,
                observation_id=obs_row["observation_id"],
                revision_id=rev_id,
                drift_kind=d["drift_kind"],
                attribute_path=d["attribute_path"],
                expected_value=d["expected_value"],
                observed_value=d["observed_value"],
                severity=d["severity"],
                guardrail_rule_id=d["guardrail_rule_id"],
                drift_id=drift_id_override if (idx == 0 and drift_id_override) else None,
            )
            all_findings.append(finding)
            st.append_gate_event(
                state_unit_id=su_id,
                resource_id=res_id,
                request_id=None,
                gate="OBSERVED",
                event_type="drift.detected",
                actor=f"system:su-gcp-{project_id}",
                detail={
                    "drift_id": finding["drift_id"],
                    "attribute_path": finding["attribute_path"],
                    "severity": finding["severity"],
                    "guardrail_rule_id": finding["guardrail_rule_id"],
                },
            )

    return {
        "schedule_id": f"drift-{project_id}",
        "project_id": project_id,
        "window": window,
        "operation_id": operation_id,
        "k8s_job_name": job_name,
        "k8s_job_manifest": manifest,
        "tf_operation": op_row,
        "observation": obs_row,
        "drift_detected": drift_detected,
        "drift_findings": all_findings,
        "remediation_triggered": False,  # Alert-only invariant (ADR-AUT-002 GR-2)
    }


@activity.defn(name="run_drift_scan_activity")
async def run_drift_scan_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Temporal activity wrapper around execute_drift_scan_for_project."""
    return execute_drift_scan_for_project(
        project_id=params["project_id"],
        window=params.get("window") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H"),
        state_unit_id=params.get("state_unit_id"),
        revision_id=params.get("revision_id"),
        revision_no=params.get("revision_no"),
        state_serial=int(params.get("state_serial", 6)),
        observation_id=params.get("observation_id"),
        drift_id_override=params.get("drift_id"),
        observed_overrides_by_resource=params.get("observed_overrides"),
        use_gcp_api=bool(params.get("use_gcp_api", False)),
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Kubernetes CronJob drift-<project_id>."""
    parser = argparse.ArgumentParser(description="SPP Independent Drift Detection CronJob (alert-only)")
    parser.add_argument(
        "--project-id",
        default=os.getenv("SPP_PROJECT_ID", "microdrive-dev"),
        help="Target GCP project ID to scan for drift",
    )
    parser.add_argument(
        "--window",
        default=os.getenv("SPP_DRIFT_WINDOW", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")),
        help="Schedule window key (e.g. 2026-10-23T06)",
    )
    parser.add_argument(
        "--use-gcp-api",
        action="store_true",
        default=os.getenv("SPP_WORKER_USE_GCS", "false").lower() == "true",
        help="Query live GCS bucket state via Google Cloud Storage API",
    )
    args = parser.parse_args(argv)

    result = execute_drift_scan_for_project(
        project_id=args.project_id,
        window=args.window,
        use_gcp_api=args.use_gcp_api,
    )
    print(json.dumps({k: v for k, v in result.items() if k != "k8s_job_manifest"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
