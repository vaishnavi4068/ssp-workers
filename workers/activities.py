"""Temporal activities for ssp-workers (WI-002, WI-004, WI-005, ADR-AUT-001, ADR-AUT-002, ADR-SVC-001)."""

from __future__ import annotations

import copy
import logging
import os
import uuid
from typing import Any

from temporalio import activity

from .config import is_real_gcs_configured, is_real_k8s_configured, resolve_runner_image
from .db import AlloyDbStateStore, InMemoryStateStore, get_active_store
from .drift import read_live_gcs_bucket_via_gcp_api
from .k8s_runner import (
    build_tf_runner_job_manifest,
    evaluate_plan_with_conftest,
    launch_or_reattach_k8s_job,
    summarize_terraform_plan,
)
from .models import (
    K8S_NAMESPACE,
    RUNNER_GSA,
    RUNNER_KSA,
    build_gcs_bucket_observed_projection,
    build_spanner_database_observed_projection,
    build_spanner_instance_observed_projection,
    tfvars_collection_key,
    build_logs_uri,
    build_operation_id,
    build_plan_uri,
    build_state_uri,
    canonical_sha256,
    k8s_job_name,
)

logger = logging.getLogger(__name__)


def _get_store() -> AlloyDbStateStore | InMemoryStateStore:
    return get_active_store()


def extract_approved_doc(params: dict[str, Any]) -> dict[str, Any] | None:
    """Accept both `approved_spec` (sent by ssp-cp) and `approved_doc` payload keys."""
    doc = params.get("approved_spec")
    if doc is None:
        doc = params.get("approved_doc")
    return doc


@activity.defn(name="check_state_unit_kill_switch_activity")
async def check_state_unit_kill_switch_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Check INV-9: verify idp.state_unit.status != 'frozen' before launching plan or apply."""
    store = _get_store()
    state_unit_id = params.get("state_unit_id")
    project_id = params.get("project_id")

    su = store.get_state_unit(state_unit_id=state_unit_id, project_id=project_id)
    if not su:
        return {"frozen": False, "status": "active"}

    is_frozen = su["status"] == "frozen"
    return {
        "state_unit_id": su["state_unit_id"],
        "project_id": su["scope_id"],
        "status": su["status"],
        "frozen": is_frozen,
    }


@activity.defn(name="run_lifecycle_tasks_activity")
async def run_lifecycle_tasks_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Execute pre_apply or post_apply idp.lifecycle_task rows for an approval."""
    store = _get_store()
    approval_id = params["approval_id"]
    phase = params["phase"]
    force_fail_task = params.get("force_fail_task")

    if isinstance(store, AlloyDbStateStore):
        return store.run_lifecycle_tasks(
            approval_id=approval_id,
            phase=phase,
            force_fail_task=force_fail_task,
        )

    executed: list[dict[str, Any]] = []
    for _, task in store.lifecycle_tasks.items():
        if task["approval_id"] == approval_id and task["phase"] == phase:
            task["attempts"] = task.get("attempts", 0) + 1
            if force_fail_task and task["task_type"] == force_fail_task:
                task["status"] = "FAILED"
                task["last_error"] = f"Simulated failure in lifecycle task {task['task_key']}"
                executed.append(copy.deepcopy(task))
                return {
                    "succeeded": False,
                    "blocked_reason": "prerequisite_failed",
                    "tasks": executed,
                }
            if task["task_type"] in {"cmdb_register", "dataportal_register"}:
                task["status"] = "SKIPPED"
            else:
                task["status"] = "SUCCEEDED"
            executed.append(copy.deepcopy(task))

    return {
        "succeeded": True,
        "blocked_reason": None,
        "tasks": executed,
    }


@activity.defn(name="check_bucket_empty_for_decommission_activity")
async def check_bucket_empty_for_decommission_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Refuse decommission of non-empty GCS buckets; never force-delete (GCS-DEL-001, ADR-SVC-001 GR-3)."""
    store = _get_store()
    bucket_name = params["bucket_name"]

    if "object_count" in params and params["object_count"] is not None:
        object_count = int(params["object_count"])
    elif isinstance(store, AlloyDbStateStore) or is_real_gcs_configured():
        gcs_client = store.gcs_store if isinstance(store, AlloyDbStateStore) else getattr(store, "gcs_store", None)
        object_count = gcs_client.count_bucket_objects(bucket_name) if gcs_client else 0
    else:
        object_count = int(store.cloud_bucket_object_counts.get(bucket_name, 0))

    if object_count > 0:
        return {
            "bucket_name": bucket_name,
            "is_empty": False,
            "object_count": object_count,
            "allowed": False,
            "blocked_reason": "bucket_not_empty",
            "message": (
                f"GCS-DEL-001: Refusing to decommission non-empty bucket '{bucket_name}' "
                f"({object_count} object(s) remaining). Never force-delete."
            ),
        }

    return {
        "bucket_name": bucket_name,
        "is_empty": True,
        "object_count": 0,
        "allowed": True,
        "blocked_reason": None,
    }


@activity.defn(name="render_expected_revision_activity")
async def render_expected_revision_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Render create-only Expected revision (manifest.json + terraform.tfvars.json) to gs://microdrive-dev-spp-tfstate with ifGenerationMatch=0."""
    store = _get_store()
    approved_doc = extract_approved_doc(params)

    if approved_doc:
        reg = store.register_approved_resource(
            approved_doc=approved_doc,
            resource_id=params.get("resource_id"),
            request_id=params.get("request_id"),
            approval_id=params.get("approval_id"),
            state_unit_id=params.get("state_unit_id"),
            base_approval_id=params.get("base_approval_id"),
        )
        state_unit_id = reg["state_unit"]["state_unit_id"]
        approval_id = reg["approval"]["approval_id"]
    else:
        state_unit_id = params["state_unit_id"]
        approval_id = params["approval_id"]

    rev_row = store.compose_and_render_expected_revision(
        state_unit_id=state_unit_id,
        trigger_approval_id=approval_id,
        revision_id=params.get("revision_id"),
    )

    tfvars = store.get_gcs_json(rev_row["artifact_uri"])
    return {
        "revision_id": rev_row["revision_id"],
        "state_unit_id": state_unit_id,
        "revision_no": rev_row["revision_no"],
        "artifact_uri": rev_row["artifact_uri"],
        "artifact_generation": rev_row["artifact_generation"],
        "artifact_sha256": rev_row["artifact_sha256"],
        "manifest": rev_row["manifest"],
        "tfvars": tfvars,
    }


@activity.defn(name="run_tf_plan_job_activity")
async def run_tf_plan_job_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Verify INV-6 & INV-9, launch real K8s Job tf-plan-<20hex> in namespace spp-platform (when K8s is configured),
    read plan.json/logs from GCS, run real conftest on plan.json, and record idp.tf_operation.
    """
    store = _get_store()
    revision_id = params["revision_id"]
    run_key = str(params.get("run_key", "1"))
    phase = params.get("phase", "plan")
    allow_destroy = bool(params.get("allow_destroy", False))

    rev = store.get_expected_revision(revision_id)
    state_unit_id = rev["state_unit_id"]
    su = store.get_state_unit(state_unit_id=state_unit_id)
    if not su:
        raise KeyError(f"StateUnit {state_unit_id} not found")
    project_id = su["scope_id"]
    revision_no = int(rev["revision_no"])

    # Check INV-9 kill switch
    if su["status"] == "frozen":
        store.update_expected_revision_plan(
            revision_id,
            status="BLOCKED",
            blocked_reason="state_unit_frozen",
        )
        return {
            "succeeded": False,
            "status": "BLOCKED",
            "blocked_reason": "state_unit_frozen",
        }

    # Check INV-6 hash parity between AlloyDB idp.approval and GCS manifest.json
    hashes_ok, reason = store.verify_revision_approval_hashes(revision_id)
    if not hashes_ok:
        return {
            "succeeded": False,
            "status": "BLOCKED",
            "blocked_reason": reason,
        }

    operation_id = build_operation_id(state_unit_id, revision_no, phase, run_key)
    job_name = k8s_job_name(operation_id, phase)
    logs_uri = build_logs_uri(project_id, job_name, su["state_bucket"])
    plan_uri = build_plan_uri(project_id, revision_no, su["state_bucket"])

    manifest = build_tf_runner_job_manifest(
        operation_id=operation_id,
        phase=phase,
        state_unit_id=state_unit_id,
        revision_id=revision_id,
        revision_no=revision_no,
        project_id=project_id,
        executor_identity=su["executor_identity"],
        run_key=run_key,
        namespace=os.getenv("SPP_K8S_NAMESPACE", K8S_NAMESPACE),
        service_account_name=os.getenv("SPP_RUNNER_KSA", RUNNER_KSA),
        runner_gsa_email=os.getenv("SPP_RUNNER_GSA", RUNNER_GSA),
        runner_image=resolve_runner_image(),
        state_bucket=su["state_bucket"],
        allow_destroy=allow_destroy,
    )

    use_k8s = bool(params.get("use_k8s", is_real_k8s_configured()))
    serial_before = int(params.get("state_serial_before", 0))
    serial_after = serial_before
    state_lineage: str | None = None

    if use_k8s:
        store.record_tf_operation(
            operation_id=operation_id,
            state_unit_id=state_unit_id,
            revision_id=revision_id,
            phase=phase,
            run_key=run_key,
            k8s_job=job_name,
            executor_identity=su["executor_identity"],
            temporal_workflow_id=f"su-gcp-{project_id}",
            status="RUNNING",
            logs_uri=logs_uri,
        )

        def _hb(msg: str) -> None:
            try:
                activity.heartbeat(msg)
            except Exception:
                pass

        k8s_res = await launch_or_reattach_k8s_job(
            manifest,
            k8s_batch_api=params.get("k8s_batch_api"),
            k8s_core_api=params.get("k8s_core_api"),
            poll_seconds=float(os.getenv("SPP_JOB_POLL_SECONDS", "5")),
            heartbeat_fn=_hb,
        )
        runner_res = k8s_res.get("runner_result") or {}
        serial_before = int(runner_res.get("state_serial_before", serial_before))
        serial_after = int(runner_res.get("state_serial_after", serial_before))
        state_lineage = runner_res.get("state_lineage")

        if not k8s_res["succeeded"] and "plan_uri" not in runner_res:
            err_payload = runner_res.get("error") or {"logs": (k8s_res.get("logs") or "")[-2000:]}
            op_row = store.record_tf_operation(
                operation_id=operation_id,
                state_unit_id=state_unit_id,
                revision_id=revision_id,
                phase=phase,
                run_key=run_key,
                k8s_job=job_name,
                executor_identity=su["executor_identity"],
                temporal_workflow_id=f"su-gcp-{project_id}",
                status="FAILED",
                tf_exit_code=1,
                state_serial_before=serial_before,
                state_serial_after=serial_after,
                state_lineage=state_lineage,
                logs_uri=logs_uri,
                error=err_payload,
            )
            store.update_expected_revision_plan(
                revision_id,
                status="FAILED",
                blocked_reason="tf_plan_job_failed",
            )
            return {
                "succeeded": False,
                "status": "FAILED",
                "blocked_reason": "tf_plan_job_failed",
                "operation_id": operation_id,
                "k8s_job_name": job_name,
                "k8s_job_manifest": manifest,
                "tf_operation": op_row,
            }

        # Read plan.json uploaded to GCS by the runner Job
        plan_doc = store.get_gcs_json(plan_uri)
    else:
        # Offline / unit test fallback when K8s is not configured
        tfvars = store.get_gcs_json(rev["artifact_uri"])
        plan_doc = params.get("mock_plan_json") or _build_synthetic_plan_json(
            tfvars=tfvars,
            manifest_members=rev["manifest"].get("members") or [],
        )
        store.put_gcs_object(plan_uri, plan_doc)
        store.put_gcs_object(logs_uri, f"[spp-tf-runner] Executed {job_name} for {operation_id}\n")

    conftest_res = evaluate_plan_with_conftest(plan_doc, allow_destroy=allow_destroy)
    summary = conftest_res["plan_summary"]
    tf_exit_code = 2 if any(v > 0 for v in summary.values()) else 0

    store.record_policy_evaluation_plan(
        revision_id=revision_id,
        opa_package="main",
        bundle_revision=os.getenv("OPA_BUNDLE_REVISION", "3f9a1c2"),
        input_sha256=canonical_sha256(plan_doc),
        decision=conftest_res["decision"],
        violations=conftest_res["violations"],
    )

    op_row = store.record_tf_operation(
        operation_id=operation_id,
        state_unit_id=state_unit_id,
        revision_id=revision_id,
        phase=phase,
        run_key=run_key,
        k8s_job=job_name,
        executor_identity=su["executor_identity"],
        temporal_workflow_id=f"su-gcp-{project_id}",
        status="SUCCEEDED",
        tf_exit_code=tf_exit_code,
        state_serial_before=serial_before,
        state_serial_after=serial_after,
        state_lineage=state_lineage,
        logs_uri=logs_uri,
    )

    if conftest_res["decision"] != "allow":
        blocked_reason = conftest_res["blocked_reason"] or "policy_violation"
        store.update_expected_revision_plan(
            revision_id,
            status="BLOCKED",
            plan_uri=plan_uri,
            plan_summary=summary,
            blocked_reason=blocked_reason,
        )
        store.append_gate_event(
            state_unit_id=state_unit_id,
            resource_id=None,
            request_id=None,
            gate="EXPECTED",
            event_type="plan.blocked",
            actor=f"system:su-gcp-{project_id}",
            detail={
                "revision_id": revision_id,
                "operation_id": operation_id,
                "blocked_reason": blocked_reason,
                "violations": conftest_res["violations"],
            },
        )
        return {
            "succeeded": False,
            "status": "BLOCKED",
            "blocked_reason": blocked_reason,
            "violations": conftest_res["violations"],
            "operation_id": operation_id,
            "k8s_job_name": job_name,
            "k8s_job_manifest": manifest,
            "plan_summary": summary,
            "tf_operation": op_row,
        }

    store.update_expected_revision_plan(
        revision_id,
        status="PLANNED",
        plan_uri=plan_uri,
        plan_summary=summary,
        blocked_reason=None,
    )
    return {
        "succeeded": True,
        "status": "PLANNED",
        "blocked_reason": None,
        "violations": [],
        "operation_id": operation_id,
        "k8s_job_name": job_name,
        "k8s_job_manifest": manifest,
        "plan_uri": plan_uri,
        "logs_uri": logs_uri,
        "plan_summary": summary,
        "tf_operation": op_row,
    }


@activity.defn(name="run_tf_apply_job_activity")
async def run_tf_apply_job_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Verify INV-5 & INV-9, launch real K8s Job tf-apply-<20hex> in namespace spp-platform (when K8s is configured),
    and record Observed projection (<= 2 KB) in AlloyDB.
    """
    store = _get_store()
    revision_id = params["revision_id"]
    run_key = str(params.get("run_key", "1"))

    rev = store.get_expected_revision(revision_id)
    state_unit_id = rev["state_unit_id"]
    su = store.get_state_unit(state_unit_id=state_unit_id)
    if not su:
        raise KeyError(f"StateUnit {state_unit_id} not found")
    project_id = su["scope_id"]
    revision_no = int(rev["revision_no"])

    # Check INV-9 kill switch before apply
    if su["status"] == "frozen":
        store.update_expected_revision_plan(
            revision_id,
            status="BLOCKED",
            blocked_reason="state_unit_frozen",
        )
        return {
            "succeeded": False,
            "status": "BLOCKED",
            "blocked_reason": "state_unit_frozen",
        }

    if rev["status"] not in {"PLANNED", "APPLYING"}:
        raise ValueError(f"INV-5 violation: cannot apply revision {revision_id} in status {rev['status']}")

    store.update_expected_revision_plan(revision_id, status="APPLYING")
    operation_id = build_operation_id(state_unit_id, revision_no, "apply", run_key)
    job_name = k8s_job_name(operation_id, "apply")
    logs_uri = build_logs_uri(project_id, job_name, su["state_bucket"])
    state_uri = build_state_uri(project_id, su["state_bucket"])

    manifest = build_tf_runner_job_manifest(
        operation_id=operation_id,
        phase="apply",
        state_unit_id=state_unit_id,
        revision_id=revision_id,
        revision_no=revision_no,
        project_id=project_id,
        executor_identity=su["executor_identity"],
        run_key=run_key,
        namespace=os.getenv("SPP_K8S_NAMESPACE", K8S_NAMESPACE),
        service_account_name=os.getenv("SPP_RUNNER_KSA", RUNNER_KSA),
        runner_gsa_email=os.getenv("SPP_RUNNER_GSA", RUNNER_GSA),
        runner_image=resolve_runner_image(),
        state_bucket=su["state_bucket"],
    )

    use_k8s = bool(params.get("use_k8s", is_real_k8s_configured()))
    serial_before = int(params.get("state_serial_before", 0))
    serial_after = int(params.get("state_serial_after", serial_before + 3))
    state_gen = 1729587065000000
    state_lineage = "<uuid>"

    if use_k8s:
        store.record_tf_operation(
            operation_id=operation_id,
            state_unit_id=state_unit_id,
            revision_id=revision_id,
            phase="apply",
            run_key=run_key,
            k8s_job=job_name,
            executor_identity=su["executor_identity"],
            temporal_workflow_id=f"su-gcp-{project_id}",
            status="RUNNING",
            logs_uri=logs_uri,
        )

        def _hb(msg: str) -> None:
            try:
                activity.heartbeat(msg)
            except Exception:
                pass

        k8s_res = await launch_or_reattach_k8s_job(
            manifest,
            k8s_batch_api=params.get("k8s_batch_api"),
            k8s_core_api=params.get("k8s_core_api"),
            poll_seconds=float(os.getenv("SPP_JOB_POLL_SECONDS", "5")),
            heartbeat_fn=_hb,
        )
        runner_res = k8s_res.get("runner_result") or {}
        serial_before = int(runner_res.get("state_serial_before", serial_before))
        serial_after = int(runner_res.get("state_serial_after", serial_after))
        state_gen = int(runner_res.get("state_generation") or state_gen)
        state_lineage = str(runner_res.get("state_lineage") or state_lineage)

        if not k8s_res["succeeded"]:
            err_payload = runner_res.get("error") or {"logs": (k8s_res.get("logs") or "")[-2000:]}
            op_row = store.record_tf_operation(
                operation_id=operation_id,
                state_unit_id=state_unit_id,
                revision_id=revision_id,
                phase="apply",
                run_key=run_key,
                k8s_job=job_name,
                executor_identity=su["executor_identity"],
                temporal_workflow_id=f"su-gcp-{project_id}",
                status="FAILED",
                tf_exit_code=1,
                state_serial_before=serial_before,
                state_serial_after=serial_after,
                state_lineage=state_lineage,
                logs_uri=logs_uri,
                error=err_payload,
            )
            store.update_expected_revision_plan(
                revision_id,
                status="FAILED",
                blocked_reason="tf_apply_job_failed",
            )
            return {
                "succeeded": False,
                "status": "FAILED",
                "blocked_reason": "tf_apply_job_failed",
                "operation_id": operation_id,
                "k8s_job_name": job_name,
                "k8s_job_manifest": manifest,
                "tf_operation": op_row,
            }
    else:
        store.put_gcs_object(logs_uri, f"[spp-tf-runner] Executed {job_name} for {operation_id}\n")
        state_gen, _ = store.put_gcs_object(
            state_uri,
            {"version": 4, "serial": serial_after, "lineage": state_lineage, "revision_no": revision_no},
        )

    op_row = store.record_tf_operation(
        operation_id=operation_id,
        state_unit_id=state_unit_id,
        revision_id=revision_id,
        phase="apply",
        run_key=run_key,
        k8s_job=job_name,
        executor_identity=su["executor_identity"],
        temporal_workflow_id=f"su-gcp-{project_id}",
        status="SUCCEEDED",
        tf_exit_code=0,
        state_serial_before=serial_before,
        state_serial_after=serial_after,
        state_lineage=state_lineage,
        logs_uri=logs_uri,
    )

    store.mark_expected_revision_applied(revision_id, state_unit_id, revision_no)

    obs_row = store.record_observation(
        state_unit_id=state_unit_id,
        source="post_apply",
        operation_id=operation_id,
        revision_id=revision_id,
        state_uri=state_uri,
        state_generation=state_gen,
        state_serial=serial_after,
        state_lineage=state_lineage,
        drift_detected=False,
        observation_id=params.get("observation_id"),
    )

    tfvars = store.get_gcs_json(rev["artifact_uri"])
    gcs_buckets_map = tfvars.get("gcs_buckets") or {}
    spanner_instances_map = tfvars.get("spanner_instances") or {}
    spanner_databases_map = tfvars.get("spanner_databases") or {}
    observed_updates: list[dict[str, Any]] = []

    for m in rev["manifest"].get("members") or []:
        res_id = m["resource_id"]
        res_name = m["name"]
        res_kind = m["kind"]
        res_tf_addr = m["tf_address"]

        if m["change"] == "removed":
            store.update_resource_after_apply(res_id, lifecycle="deleted", gate="OBSERVED", sync_status="IN_SYNC")
            if isinstance(store, InMemoryStateStore):
                store.cloud_buckets.pop(res_name, None)
            continue

        if res_kind == "gcs_bucket" and res_name in gcs_buckets_map:
            if use_k8s or is_real_gcs_configured():
                try:
                    live_bucket = read_live_gcs_bucket_via_gcp_api(res_name, project_id)
                    if live_bucket.get("exists"):
                        entry = live_bucket
                    else:
                        entry = copy.deepcopy(gcs_buckets_map[res_name])
                except Exception:
                    entry = copy.deepcopy(gcs_buckets_map[res_name])
            else:
                entry = copy.deepcopy(gcs_buckets_map[res_name])
                entry["time_created"] = params.get("time_created", "2026-10-22T09:43:58.412Z")

            projection, proj_sha = build_gcs_bucket_observed_projection(
                entry,
                tf_address=res_tf_addr,
            )
            cloud_id = f"projects/_/buckets/{res_name}"
            was_written, ro_row = store.upsert_resource_observed(
                resource_id=res_id,
                observation_id=obs_row["observation_id"],
                exists_in_cloud=True,
                cloud_resource_id=cloud_id,
                projection=projection,
                projection_sha256=proj_sha,
            )
            if isinstance(store, InMemoryStateStore):
                store.cloud_buckets[res_name] = copy.deepcopy(projection)
            store.update_resource_after_apply(res_id, lifecycle="active", gate="OBSERVED", sync_status="IN_SYNC")
            observed_updates.append(
                {
                    "resource_id": res_id,
                    "cloud_resource_id": cloud_id,
                    "projection_sha256": proj_sha,
                    "was_written": was_written,
                    "projection": projection,
                    "resource_observed": ro_row,
                }
            )
            continue

        if res_kind == "spanner_instance" and res_name in spanner_instances_map:
            entry = copy.deepcopy(spanner_instances_map[res_name])
            projection, proj_sha = build_spanner_instance_observed_projection(entry, tf_address=res_tf_addr)
            cloud_id = f"projects/{entry['project']}/instances/{res_name}"
            was_written, ro_row = store.upsert_resource_observed(
                resource_id=res_id,
                observation_id=obs_row["observation_id"],
                exists_in_cloud=True,
                cloud_resource_id=cloud_id,
                projection=projection,
                projection_sha256=proj_sha,
            )
            store.update_resource_after_apply(res_id, lifecycle="active", gate="OBSERVED", sync_status="IN_SYNC")
            observed_updates.append(
                {
                    "resource_id": res_id,
                    "cloud_resource_id": cloud_id,
                    "projection_sha256": proj_sha,
                    "was_written": was_written,
                    "projection": projection,
                    "resource_observed": ro_row,
                }
            )
            continue

        if res_kind == "spanner_database":
            map_key = None
            for key in spanner_databases_map:
                if key.endswith(f"/{res_name}"):
                    map_key = key
                    break
            if map_key:
                entry = copy.deepcopy(spanner_databases_map[map_key])
                projection, proj_sha = build_spanner_database_observed_projection(entry, tf_address=res_tf_addr)
                cloud_id = (
                    f"projects/{entry['project']}/instances/{entry['instance']}/databases/{res_name}"
                )
                was_written, ro_row = store.upsert_resource_observed(
                    resource_id=res_id,
                    observation_id=obs_row["observation_id"],
                    exists_in_cloud=True,
                    cloud_resource_id=cloud_id,
                    projection=projection,
                    projection_sha256=proj_sha,
                )
                store.update_resource_after_apply(res_id, lifecycle="active", gate="OBSERVED", sync_status="IN_SYNC")
                observed_updates.append(
                    {
                        "resource_id": res_id,
                        "cloud_resource_id": cloud_id,
                        "projection_sha256": proj_sha,
                        "was_written": was_written,
                        "projection": projection,
                        "resource_observed": ro_row,
                    }
                )

    store.append_gate_event(
        state_unit_id=state_unit_id,
        resource_id=observed_updates[0]["resource_id"] if observed_updates else None,
        request_id=None,
        gate="OBSERVED",
        event_type="apply.succeeded",
        actor=f"system:su-gcp-{project_id}",
        detail={
            "revision_id": revision_id,
            "operation_id": operation_id,
            "observation_id": obs_row["observation_id"],
        },
    )

    return {
        "succeeded": True,
        "status": "APPLIED",
        "operation_id": operation_id,
        "k8s_job_name": job_name,
        "k8s_job_manifest": manifest,
        "logs_uri": logs_uri,
        "tf_operation": op_row,
        "observation": obs_row,
        "observed_resources": observed_updates,
    }


@activity.defn(name="prepare_rollback_revision_activity")
async def prepare_rollback_revision_activity(params: dict[str, Any]) -> dict[str, Any]:
    """Prepare a rollback by creating a new approval referencing the resource's last known-good approved_spec (ADR-AUT-002, WI-002)."""
    store = _get_store()
    resource_id = params["resource_id"]
    target_approval_id = params.get("target_approval_id")

    if isinstance(store, AlloyDbStateStore):
        prior_approval = store.get_rollback_target_approval(resource_id, target_approval_id)
        target_approval_id = prior_approval["approval_id"]
    else:
        res = store.resources[resource_id]
        if not target_approval_id:
            curr_req = store.requests[res["current_request_id"]]
            target_approval_id = curr_req.get("base_approval_id")
        if not target_approval_id or target_approval_id not in store.approvals:
            raise ValueError(f"No prior approved specification found to roll back resource {resource_id}")
        prior_approval = store.approvals[target_approval_id]

    restored_doc = copy.deepcopy(prior_approval["approved_spec"])
    restored_doc["action"] = "update"

    reg = store.register_approved_resource(
        approved_doc=restored_doc,
        resource_id=resource_id,
        base_approval_id=target_approval_id,
    )
    return {
        "resource_id": resource_id,
        "restored_from_approval_id": target_approval_id,
        "new_approval_id": reg["approval"]["approval_id"],
        "approved_spec_sha256": reg["approval"]["approved_spec_sha256"],
        "state_unit_id": reg["state_unit"]["state_unit_id"],
    }


def _build_synthetic_plan_json(
    *,
    tfvars: dict[str, Any],
    manifest_members: list[dict[str, Any]],
) -> dict[str, Any]:
    """Construct a Terraform plan.json matching terraform show -json structure from Expected tfvars (offline unit test helper)."""
    resource_changes: list[dict[str, Any]] = []
    gcs_buckets = tfvars.get("gcs_buckets") or {}
    spanner_instances = tfvars.get("spanner_instances") or {}
    spanner_databases = tfvars.get("spanner_databases") or {}

    for m in manifest_members:
        name = m["name"]
        change = m["change"]
        if m["kind"] == "gcs_bucket":
            if change == "removed":
                resource_changes.append(
                    {
                        "address": f'module.gcs_bucket["{name}"].google_storage_bucket.this',
                        "type": "google_storage_bucket",
                        "change": {
                            "actions": ["delete"],
                            "before": {"name": name},
                            "after": None,
                        },
                    }
                )
            elif name in gcs_buckets:
                entry = gcs_buckets[name]
                actions = ["create"] if change == "added" else (["update"] if change == "modified" else ["no-op"])
                resource_changes.append(
                    {
                        "address": f'module.gcs_bucket["{name}"].google_storage_bucket.this',
                        "type": "google_storage_bucket",
                        "change": {
                            "actions": actions,
                            "after": {
                                "project": entry["project"],
                                "name": entry["name"],
                                "location": entry["location"],
                                "storage_class": entry["storage_class"],
                                "uniform_bucket_level_access": entry["uniform_bucket_level_access"],
                                "public_access_prevention": entry["public_access_prevention"],
                                "force_destroy": entry["force_destroy"],
                                "encryption": [{"default_kms_key_name": entry["default_kms_key_name"]}],
                                "retention_policy": [entry["retention_policy"]] if entry.get("retention_policy") else [],
                                "soft_delete_policy": [entry["soft_delete_policy"]],
                                "lifecycle_rule": entry.get("lifecycle_rules") or [],
                                "labels": entry["labels"],
                            },
                            "after_sensitive": {
                                "labels": {},
                                "encryption": [{}],
                            },
                        },
                    }
                )
                if change == "added":
                    for role, members in (entry.get("iam_bindings") or {}).items():
                        resource_changes.append(
                            {
                                "address": f'module.gcs_bucket["{name}"].google_storage_bucket_iam_binding.bindings["{role}"]',
                                "type": "google_storage_bucket_iam_binding",
                                "change": {
                                    "actions": ["create"],
                                    "after": {
                                        "bucket": name,
                                        "role": role,
                                        "members": members,
                                    },
                                },
                            }
                        )

        elif m["kind"] == "spanner_instance" and name in spanner_instances:
            entry = spanner_instances[name]
            actions = ["create"] if change == "added" else (["update"] if change == "modified" else ["no-op"])
            resource_changes.append(
                {
                    "address": f'module.spanner_instance["{name}"].google_spanner_instance.this',
                    "type": "google_spanner_instance",
                    "change": {
                        "actions": actions,
                        "after": {
                            "project": entry["project"],
                            "name": entry["name"],
                            "config": entry.get("config"),
                            "labels": entry.get("labels") or {},
                        },
                    },
                }
            )
        elif m["kind"] == "spanner_database":
            map_key = next((k for k in spanner_databases if k.endswith(f"/{name}")), None)
            if map_key and map_key in spanner_databases:
                entry = spanner_databases[map_key]
                actions = ["create"] if change == "added" else (["update"] if change == "modified" else ["no-op"])
                resource_changes.append(
                    {
                        "address": f'module.spanner_database["{map_key}"].google_spanner_database.this',
                        "type": "google_spanner_database",
                        "change": {
                            "actions": actions,
                            "after": {
                                "project": entry["project"],
                                "instance": entry["instance"],
                                "name": entry["name"],
                                "deletion_protection": entry.get("deletion_protection", True),
                            },
                        },
                    }
                )

    return {
        "format_version": "1.2",
        "terraform_version": "1.10.5",
        "resource_changes": resource_changes,
    }
