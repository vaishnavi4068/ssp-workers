"""Kubernetes Job builder, launcher, and Conftest plan policy evaluator for ssp-workers (ADR-AUT-001, ADR-CLD-GCP-001, ADR-GLB-005)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from .config import resolve_runner_image
from .models import (
    K8S_NAMESPACE,
    RUNNER_GSA,
    RUNNER_KSA,
    STATE_BUCKET,
    build_artifact_uri,
    build_logs_uri,
    build_plan_uri,
    build_snapshot_uri,
    format_revision_dir,
    k8s_job_name,
)

logger = logging.getLogger(__name__)


def build_tf_runner_job_manifest(
    *,
    operation_id: str,
    phase: str,
    state_unit_id: str,
    revision_id: str,
    revision_no: int,
    project_id: str,
    executor_identity: str,
    run_key: str = "1",
    namespace: str = K8S_NAMESPACE,
    service_account_name: str = RUNNER_KSA,
    runner_gsa_email: str = RUNNER_GSA,
    runner_image: str | None = None,
    state_bucket: str = STATE_BUCKET,
    gcp_region: str = "us-east1",
    allow_destroy: bool = False,
    active_deadline_seconds: int = 1800,
) -> dict[str, Any]:
    """Construct the deterministic Kubernetes Job manifest for a Terraform operation in namespace spp-platform.

    Enforces:
    - k8s_job_name(operation_id, phase) = tf-<phase>-<20hex>
    - namespace = spp-platform
    - serviceAccountName = spp-tf-runner (annotated with iam.gke.io/gcp-service-account: spp-deploy@microdrive-dev.iam.gserviceaccount.com)
    - nodeSelector: workload=spp, toleration: dedicated=spp:NoSchedule
    - Non-root securityContext (UID/GID 10001)
    - Runner image resolved from config/deployed tag (not hardcoded :latest)
    """
    job_name = k8s_job_name(operation_id, phase)
    effective_image = runner_image or resolve_runner_image()
    artifact_uri = build_artifact_uri(project_id, revision_no, state_bucket)
    rev_dir = format_revision_dir(revision_no)
    planfile_uri = f"gs://{state_bucket}/projects/{project_id}/expected/{rev_dir}/planfile.tfplan"
    logs_uri = build_logs_uri(project_id, job_name, state_bucket)
    plan_uri = (
        build_snapshot_uri(project_id, run_key, state_bucket)
        if phase == "refresh_plan"
        else build_plan_uri(project_id, revision_no, state_bucket)
    )

    labels = {
        "app.kubernetes.io/name": "spp-tf-runner",
        "app.kubernetes.io/part-of": "spp-platform",
        "app.kubernetes.io/component": "terraform-runner-job",
        "spp.platform/phase": phase.replace("_", "-"),
        "spp.platform/project-id": re.sub(r"[^a-zA-Z0-9_.-]", "-", project_id)[:63],
        "spp.platform/revision-no": str(revision_no),
    }

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": labels,
            "annotations": {
                "spp.platform/operation-id": operation_id,
                "spp.platform/runner-gsa": runner_gsa_email,
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": active_deadline_seconds,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {
                    "labels": labels,
                },
                "spec": {
                    "serviceAccountName": service_account_name,
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "nodeSelector": {
                        "workload": "spp",
                    },
                    "tolerations": [
                        {
                            "key": "dedicated",
                            "operator": "Equal",
                            "value": "spp",
                            "effect": "NoSchedule",
                        }
                    ],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                    },
                    "containers": [
                        {
                            "name": "tf-runner",
                            "image": effective_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-m", "workers.runner_entrypoint"],
                            "env": [
                                {"name": "SPP_OPERATION_ID", "value": operation_id},
                                {"name": "SPP_PHASE", "value": phase},
                                {"name": "SPP_STATE_UNIT_ID", "value": state_unit_id},
                                {"name": "SPP_REVISION_ID", "value": revision_id},
                                {"name": "SPP_REVISION_NO", "value": str(revision_no)},
                                {"name": "SPP_PROJECT_ID", "value": project_id},
                                {"name": "SPP_RUN_KEY", "value": run_key},
                                {"name": "SPP_STATE_BUCKET", "value": state_bucket},
                                {"name": "SPP_ARTIFACT_URI", "value": artifact_uri},
                                {"name": "SPP_PLANFILE_URI", "value": planfile_uri},
                                {"name": "SPP_PLAN_URI", "value": plan_uri},
                                {"name": "SPP_LOGS_URI", "value": logs_uri},
                                {"name": "SPP_ALLOW_DESTROY", "value": "true" if allow_destroy else "false"},
                                {"name": "SPP_RUNNER_DRY_RUN", "value": "false"},
                                {"name": "GOOGLE_CLOUD_PROJECT", "value": project_id},
                                {"name": "GOOGLE_CLOUD_LOCATION", "value": gcp_region},
                                {"name": "SPP_EXECUTOR_IDENTITY", "value": executor_identity},
                            ],
                            "resources": {
                                "requests": {"cpu": "500m", "memory": "1Gi"},
                                "limits": {"cpu": "2", "memory": "4Gi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                        }
                    ],
                },
            },
        },
    }


def summarize_terraform_plan(plan_doc: dict[str, Any]) -> dict[str, int]:
    """Compute plan_summary counts {add, change, destroy, replace} from terraform show -json output."""
    summary = {"add": 0, "change": 0, "destroy": 0, "replace": 0}
    for rc in plan_doc.get("resource_changes") or []:
        actions = (rc.get("change") or {}).get("actions") or []
        if actions == ["create"]:
            summary["add"] += 1
        elif actions == ["update"]:
            summary["change"] += 1
        elif actions == ["delete"]:
            summary["destroy"] += 1
        elif "create" in actions and "delete" in actions:
            summary["replace"] += 1
    return summary


def evaluate_plan_with_conftest(
    plan_doc: dict[str, Any],
    *,
    policy_dir: str | Path | None = None,
    allow_destroy: bool = False,
) -> dict[str, Any]:
    """Evaluate Terraform plan.json against modules/gcs-bucket/policies using the real `conftest` CLI binary (or `opa eval`).

    Also enforces INV-4 / ADR-AUT-002: any destroy > 0 or replace > 0 without an explicit human-approved delete request
    blocks the revision with blocked_reason = 'destroy_without_delete_request'.
    """
    summary = summarize_terraform_plan(plan_doc)
    violations: list[str] = []

    if (summary["destroy"] > 0 or summary["replace"] > 0) and not allow_destroy:
        violations.append(
            f"destroy_without_delete_request: plan contains destroy={summary['destroy']}, replace={summary['replace']} without human-approved delete request"
        )

    resolved_policy_dir = Path(
        policy_dir
        or os.getenv(
            "SPP_POLICY_DIR",
            str(Path(__file__).resolve().parent.parent / "modules" / "gcs-bucket" / "policies"),
        )
    )

    plan_payload = dict(plan_doc)
    plan_payload["allow_destroy"] = allow_destroy

    conftest_bin = _find_binary("conftest")
    opa_bin = _find_binary("opa")

    if conftest_bin and resolved_policy_dir.exists():
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
            json.dump(plan_payload, tmp)
            tmp_path = tmp.name
        try:
            proc = subprocess.run(
                [conftest_bin, "test", "--policy", str(resolved_policy_dir), "--output", "json", tmp_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                try:
                    parsed = json.loads(proc.stdout)
                    for item in parsed:
                        for failure in item.get("failures") or []:
                            msg = failure.get("msg", "")
                            if msg and msg not in violations:
                                violations.append(msg)
                except ValueError:
                    err_text = (proc.stdout or proc.stderr or "conftest failed").strip()
                    if err_text not in violations:
                        violations.append(err_text)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    elif opa_bin and resolved_policy_dir.exists():
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
            json.dump(plan_payload, tmp)
            tmp_path = tmp.name
        try:
            proc = subprocess.run(
                [
                    opa_bin,
                    "eval",
                    "-f",
                    "json",
                    "-d",
                    str(resolved_policy_dir),
                    "-i",
                    tmp_path,
                    "data.idp.gcp.gcs_bucket.deny",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                out = json.loads(proc.stdout)
                exprs = (out.get("result") or [{}])[0].get("expressions") or []
                for msg in (exprs[0].get("value") if exprs else []) or []:
                    if msg and msg not in violations:
                        violations.append(msg)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    else:
        for rc in plan_doc.get("resource_changes") or []:
            if rc.get("type") == "google_storage_bucket":
                actions = (rc.get("change") or {}).get("actions") or []
                if "delete" not in actions:
                    after = (rc.get("change") or {}).get("after") or {}
                    if str(after.get("location", "")).lower() != "us-east1":
                        violations.append("GCS-LOC-001 (plan): location must be 'us-east1'")
                    if after.get("uniform_bucket_level_access") is not True:
                        violations.append("GCS-UBLA-001 (plan): uniform_bucket_level_access must be true")
                    if after.get("public_access_prevention") != "enforced":
                        violations.append("GCS-PAP-001 (plan): public_access_prevention must be 'enforced'")
                    if after.get("force_destroy") is not False:
                        violations.append("GCS-DEL-001 (plan): force_destroy must be false")

    blocked_reason: str | None = None
    if any("destroy_without_delete_request" in v for v in violations):
        blocked_reason = "destroy_without_delete_request"
    elif violations:
        blocked_reason = "policy_violation"

    return {
        "decision": "allow" if not violations else "deny",
        "violations": violations,
        "plan_summary": summary,
        "blocked_reason": blocked_reason,
    }


def _find_binary(name: str) -> str | None:
    import shutil

    path_with_local = f"{Path.home() / '.local' / 'bin'}:/usr/local/bin:{os.environ.get('PATH', '')}"
    return shutil.which(name, path=path_with_local)


def load_k8s_clients() -> tuple[Any, Any]:
    """Load in-cluster or kubeconfig Kubernetes BatchV1Api and CoreV1Api clients."""
    from kubernetes import client, config  # type: ignore[import-untyped]

    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.BatchV1Api(), client.CoreV1Api()


async def launch_or_reattach_k8s_job(
    manifest: dict[str, Any],
    *,
    k8s_batch_api: Any | None = None,
    k8s_core_api: Any | None = None,
    poll_seconds: float = 2.0,
    heartbeat_fn: Any = None,
) -> dict[str, Any]:
    """Launch a Kubernetes Job in spp-platform or idempotently reattach on 409 AlreadyExists (INV-7, ADR-AUT-001).

    Polls Job status until status.succeeded >= 1 or status.failed >= 1 and parses SPP_TF_RESULT_JSON from logs if present.
    """
    if k8s_batch_api is None:
        k8s_batch_api, k8s_core_api = load_k8s_clients()

    job_name = manifest["metadata"]["name"]
    namespace = manifest["metadata"]["namespace"]
    reattached = False

    try:
        await asyncio.to_thread(k8s_batch_api.read_namespaced_job, name=job_name, namespace=namespace)
        reattached = True
        logger.info("Reattached to existing Kubernetes Job %s in namespace %s", job_name, namespace)
    except Exception as read_exc:
        status_code = getattr(read_exc, "status", None)
        if status_code != 404:
            raise
        try:
            await asyncio.to_thread(k8s_batch_api.create_namespaced_job, namespace=namespace, body=manifest)
            logger.info("Created Kubernetes Job %s in namespace %s", job_name, namespace)
        except Exception as create_exc:
            if getattr(create_exc, "status", None) == 409:
                reattached = True
                logger.info("409 AlreadyExists on create; reattached to Job %s in namespace %s", job_name, namespace)
            else:
                raise

    succeeded = False
    while True:
        if heartbeat_fn is not None:
            heartbeat_fn(job_name)
        job_obj = await asyncio.to_thread(k8s_batch_api.read_namespaced_job_status, name=job_name, namespace=namespace)
        if getattr(job_obj.status, "succeeded", 0) and job_obj.status.succeeded >= 1:
            succeeded = True
            break
        if getattr(job_obj.status, "failed", 0) and job_obj.status.failed >= 1:
            succeeded = False
            break
        await asyncio.sleep(poll_seconds)

    logs = ""
    if k8s_core_api is not None:
        try:
            pods = await asyncio.to_thread(
                k8s_core_api.list_namespaced_pod,
                namespace=namespace,
                label_selector=f"job-name={job_name}",
            )
            if pods.items:
                pod_name = pods.items[0].metadata.name
                logs = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_pod_log, name=pod_name, namespace=namespace
                )
        except Exception as exc:
            logs = f"Completed job {job_name}, log collection warning: {exc}"

    runner_result: dict[str, Any] = {}
    for line in reversed((logs or "").splitlines()):
        if line.startswith("SPP_TF_RESULT_JSON="):
            try:
                runner_result = json.loads(line[len("SPP_TF_RESULT_JSON=") :])
            except ValueError:
                pass
            break

    return {
        "k8s_job_name": job_name,
        "k8s_namespace": namespace,
        "reattached": reattached,
        "succeeded": succeeded,
        "logs": logs,
        "runner_result": runner_result,
    }
