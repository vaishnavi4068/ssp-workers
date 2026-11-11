"""Control-plane AlloyDB (`idp` schema) & GCS (`gs://microdrive-dev-spp-tfstate`) Stores for ssp-workers (ADR-GLB-003, ADR-GLB-004, ADR-AUT-001, ADR-AUT-002).

Provides:
- `RealGcsObjectStore`: Real Google Cloud Storage operations via `google-cloud-storage` + Workload Identity (`if_generation_match=0`).
- `AlloyDbStateStore`: Real PostgreSQL / AlloyDB (`10.80.140.2:5432`, db `spp_platform`, schema `idp`) + `RealGcsObjectStore`.
- `InMemoryStateStore`: In-memory twin used ONLY when real DB/GCS is not configured (unit/replay tests).
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from .config import is_real_db_configured, is_real_gcs_configured, resolve_database_url
from .models import (
    MAX_PROJECTION_BYTES,
    SECRET_KEY_NAMES,
    STATE_BUCKET,
    build_artifact_uri,
    canonical_json,
    canonical_sha256,
    convert_approved_gcs_bucket_to_expected_entry,
    convert_approved_spanner_database_to_expected_entry,
    convert_approved_spanner_instance_to_expected_entry,
    convert_approved_to_expected_entry,
    tfvars_collection_key,
    k8s_job_name,
    render_tf_address,
)

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _check_no_secrets(doc: Any, context: str = "document") -> None:
    if isinstance(doc, dict):
        for k, v in doc.items():
            if str(k).lower() in SECRET_KEY_NAMES:
                raise ValueError(f"INV-1 violation: secret key {k!r} found in {context}")
            _check_no_secrets(v, context)
    elif isinstance(doc, list):
        for item in doc:
            _check_no_secrets(item, context)


def parse_gs_uri(uri: str) -> tuple[str, str]:
    """Parse gs://<bucket>/<blob_path> into (bucket_name, blob_path)."""
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI (must start with gs://): {uri}")
    without_scheme = uri[len("gs://") :]
    parts = without_scheme.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid GCS URI (missing bucket or object path): {uri}")
    return parts[0], parts[1]


class GCSArtifactExistError(RuntimeError):
    """Raised when ifGenerationMatch=0 fails because the object already exists in GCS (INV-5)."""


class OptimisticConcurrencyError(RuntimeError):
    """Raised when advancing state_unit.current_expected_rev loses an optimistic concurrency race."""


class RealGcsObjectStore:
    """Real Google Cloud Storage client using Workload Identity (`google-cloud-storage`)."""

    def __init__(self, project_id: str | None = None, client: Any | None = None) -> None:
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "microdrive-dev")
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            from google.cloud import storage  # type: ignore[import-untyped]

            self._client = storage.Client(project=self.project_id)
        return self._client

    def put_object(
        self,
        uri: str,
        payload: dict[str, Any] | str | bytes,
        *,
        if_generation_match: int | None = None,
        content_type: str | None = None,
    ) -> tuple[int, str]:
        """Upload an object to GCS, enforcing if_generation_match=0 when requested (INV-5)."""
        bucket_name, blob_name = parse_gs_uri(uri)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if isinstance(payload, bytes):
            data_bytes = payload
            sha = hashlib.sha256(data_bytes).hexdigest()
            ctype = content_type or "application/octet-stream"
        elif isinstance(payload, str):
            data_bytes = payload.encode("utf-8")
            sha = hashlib.sha256(data_bytes).hexdigest()
            ctype = content_type or ("application/json" if uri.endswith(".json") else "text/plain; charset=utf-8")
        else:
            text = canonical_json(payload)
            data_bytes = text.encode("utf-8")
            sha = canonical_sha256(payload)
            ctype = content_type or "application/json"

        try:
            upload_kwargs: dict[str, Any] = {"content_type": ctype}
            if if_generation_match is not None:
                upload_kwargs["if_generation_match"] = if_generation_match
            blob.upload_from_string(data_bytes, **upload_kwargs)
        except Exception as exc:
            exc_name = type(exc).__name__
            if if_generation_match == 0 and (
                "PreconditionFailed" in exc_name or "412" in str(exc)
            ):
                raise GCSArtifactExistError(
                    f"GCS PreconditionFailed (ifGenerationMatch=0): {uri} already exists"
                ) from exc
            raise

        generation = int(getattr(blob, "generation", None) or 1)
        return generation, sha

    def get_text(self, uri: str) -> str:
        bucket_name, blob_name = parse_gs_uri(uri)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            raise KeyError(f"GCS object not found: {uri}")
        return blob.download_as_text(encoding="utf-8")

    def get_json(self, uri: str) -> dict[str, Any]:
        return json.loads(self.get_text(uri))

    def get_blob_metadata(self, uri: str) -> dict[str, Any] | None:
        bucket_name, blob_name = parse_gs_uri(uri)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        if blob is None:
            return None
        return {
            "uri": uri,
            "generation": int(blob.generation or 0),
            "size": int(blob.size or 0),
            "updated": blob.updated.isoformat() if blob.updated else None,
        }

    def count_bucket_objects(self, bucket_name: str, max_results: int = 5) -> int:
        """Return number of objects in a target GCS bucket (up to max_results) for decommission checks."""
        bucket = self.client.lookup_bucket(bucket_name)
        if bucket is None:
            return 0
        blobs = list(self.client.list_blobs(bucket, max_results=max_results))
        return len(blobs)


DEFAULT_RESOURCE_KINDS: dict[str, dict[str, Any]] = {
    "gcs_bucket": {
        "kind": "gcs_bucket",
        "cloud": "gcp",
        "parent_kind": None,
        "display_name": "Cloud Storage bucket",
        "schema_uri": "schemas/gcs_bucket.schema.json",
        "schema_version": "1.0.0",
        "tf_module_source": "git::https://github.com/vaishnavi4068/ssp-module-gcs-bucket.git//modules/bucket?ref=v1.0.0",
        "tfvars_collection": "gcs_buckets",
        "tf_address_template": 'module.gcs_bucket["{name}"]',
        "opa_package": "idp.gcp.gcs_bucket",
        "approval_gate_key": "gcs_bucket_owner",
        "drift_ignore_paths": ["/force_destroy"],
        "enabled": True,
    },
    "spanner_instance": {
        "kind": "spanner_instance",
        "cloud": "gcp",
        "parent_kind": None,
        "display_name": "Spanner instance",
        "schema_uri": "schemas/spanner.schema.json",
        "schema_version": "1.0.0",
        "tf_module_source": "git::https://github.com/vaishnavi4068/ssp-module-spanner.git//modules/instance?ref=v1.0.0",
        "tfvars_collection": "spanner_instances",
        "tf_address_template": 'module.spanner_instance["{name}"]',
        "opa_package": "idp.gcp.spanner_instance",
        "approval_gate_key": "spanner_data_owner",
        "drift_ignore_paths": ["/force_destroy"],
        "enabled": True,
    },
    "spanner_database": {
        "kind": "spanner_database",
        "cloud": "gcp",
        "parent_kind": "spanner_instance",
        "display_name": "Spanner database",
        "schema_uri": "schemas/spanner.schema.json",
        "schema_version": "1.0.0",
        "tf_module_source": "git::https://github.com/vaishnavi4068/ssp-module-spanner.git//modules/database?ref=v1.0.0",
        "tfvars_collection": "spanner_databases",
        "tf_address_template": 'module.spanner_database["{instance}/{name}"]',
        "opa_package": "idp.gcp.spanner_database",
        "approval_gate_key": "spanner_data_owner",
        "drift_ignore_paths": ["/ddl", "/deletion_protection"],
        "enabled": True,
    },
}


class InMemoryStateStore:
    """In-memory twin of the AlloyDB idp schema and gs://microdrive-dev-spp-tfstate bucket (for unit/replay tests)."""

    def __init__(self, gcs_store: RealGcsObjectStore | None = None) -> None:
        self.gcs_store = gcs_store
        self.approval_gate_configs: dict[str, dict[str, Any]] = {
            "spanner_data_owner": {
                "gate_key": "spanner_data_owner",
                "version": 1,
                "enabled": True,
                "config": {
                    "order": "sequential",
                    "self_approval": "deny",
                    "require_human_approval_on_delete": True,
                    "approvers": [
                        {
                            "key": "data_owner",
                            "ticket_field": "/metadata/owner",
                            "mandatory": True,
                            "sla_days": 5,
                        },
                        {
                            "key": "dbaas_reviewer",
                            "role": "dbaas_reviewer",
                            "mandatory": True,
                            "run_after_approver_keys": ["data_owner"],
                            "skip_condition": "metadata.environment not in ('prod') and metadata.resolved.app_tier != '1'",
                            "sla_days": 3,
                        },
                    ],
                    "max_wait_days": 13,
                },
            },
            "gcs_bucket_owner": {
                "gate_key": "gcs_bucket_owner",
                "version": 1,
                "enabled": True,
                "config": {
                    "order": "sequential",
                    "self_approval": "deny",
                    "require_human_approval_on_delete": True,
                    "approvers": [
                        {
                            "key": "bucket_owner",
                            "ticket_field": "/metadata/owner",
                            "mandatory": True,
                            "sla_days": 5,
                            "max_follow_ups": 4,
                            "follow_up_interval_days": 1,
                            "escalation": {
                                "type": "manager",
                                "manager_sla_days": 2,
                                "max_follow_ups": 1,
                            },
                        }
                    ],
                    "max_wait_days": 13,
                    "auto_close": {
                        "enabled": True,
                        "warning_lead_days": 2,
                        "final_reminder_after_days": 1,
                    },
                    "email_attribute_list": ["/spec/name", "/metadata/project_id", "/metadata/owner"],
                },
            }
        }
        self.resource_kinds: dict[str, dict[str, Any]] = copy.deepcopy(DEFAULT_RESOURCE_KINDS)
        self.state_units: dict[str, dict[str, Any]] = {}
        self.resources: dict[str, dict[str, Any]] = {}
        self.requests: dict[str, dict[str, Any]] = {}
        self.approvals: dict[str, dict[str, Any]] = {}
        self.approval_decisions: dict[str, dict[str, Any]] = {}
        self.expected_revisions: dict[str, dict[str, Any]] = {}
        self.expected_revision_members: list[dict[str, Any]] = []
        self.tf_operations: dict[str, dict[str, Any]] = {}
        self.observations: dict[str, dict[str, Any]] = {}
        self.resource_observed: dict[str, dict[str, Any]] = {}
        self.resource_observed_write_count: dict[str, int] = {}
        self.drift_findings: dict[str, dict[str, Any]] = {}
        self.lifecycle_tasks: dict[str, dict[str, Any]] = {}
        self.policy_evaluations: dict[str, dict[str, Any]] = {}
        self.gate_events: list[dict[str, Any]] = []

        self.gcs_objects: dict[str, dict[str, Any]] = {}
        self._next_generation: int = 1729587000000000

        self.cloud_bucket_object_counts: dict[str, int] = {}
        self.cloud_buckets: dict[str, dict[str, Any]] = {}

    def put_gcs_object(
        self,
        uri: str,
        payload: dict[str, Any] | str,
        *,
        if_generation_match: int | None = None,
    ) -> tuple[int, str]:
        """Write an object to GCS (real GCS when configured, otherwise in-memory), honoring ifGenerationMatch=0."""
        if self.gcs_store is not None:
            gen, sha = self.gcs_store.put_object(uri, payload, if_generation_match=if_generation_match)
            text = payload if isinstance(payload, str) else canonical_json(payload)
            self.gcs_objects[uri] = {"content": text, "generation": gen, "sha256": sha}
            return gen, sha

        if if_generation_match == 0 and uri in self.gcs_objects:
            raise GCSArtifactExistError(f"GCS PreconditionFailed (ifGenerationMatch=0): {uri} already exists")
        text = payload if isinstance(payload, str) else canonical_json(payload)
        sha = hashlib.sha256(text.encode("utf-8")).hexdigest() if isinstance(payload, str) else canonical_sha256(payload)
        gen = self._next_generation
        self._next_generation += 1
        self.gcs_objects[uri] = {
            "content": text,
            "generation": gen,
            "sha256": sha,
        }
        return gen, sha

    def get_gcs_text(self, uri: str) -> str:
        if self.gcs_store is not None:
            try:
                return self.gcs_store.get_text(uri)
            except KeyError:
                if uri in self.gcs_objects:
                    return str(self.gcs_objects[uri]["content"])
                raise
        if uri not in self.gcs_objects:
            raise KeyError(f"GCS object not found: {uri}")
        return str(self.gcs_objects[uri]["content"])

    def get_gcs_json(self, uri: str) -> dict[str, Any]:
        if self.gcs_store is not None:
            try:
                return self.gcs_store.get_json(uri)
            except KeyError:
                if uri in self.gcs_objects:
                    return json.loads(self.gcs_objects[uri]["content"])
                raise
        if uri not in self.gcs_objects:
            raise KeyError(f"GCS object not found: {uri}")
        return json.loads(self.gcs_objects[uri]["content"])

    def get_state_unit(self, *, state_unit_id: str | None = None, project_id: str | None = None) -> dict[str, Any] | None:
        if state_unit_id and state_unit_id in self.state_units:
            return self.state_units[state_unit_id]
        if project_id:
            for su in self.state_units.values():
                if su["scope_id"] == project_id:
                    return su
        return None

    def get_expected_revision(self, revision_id: str) -> dict[str, Any]:
        return self.expected_revisions[revision_id]

    def update_expected_revision_plan(
        self,
        revision_id: str,
        *,
        status: str,
        plan_uri: str | None = None,
        plan_summary: dict[str, int] | None = None,
        blocked_reason: str | None = None,
    ) -> dict[str, Any]:
        rev = self.expected_revisions[revision_id]
        rev["status"] = status
        if plan_uri is not None:
            rev["plan_uri"] = plan_uri
        if plan_summary is not None:
            rev["plan_summary"] = plan_summary
        rev["blocked_reason"] = blocked_reason
        return rev

    def mark_expected_revision_applied(self, revision_id: str, state_unit_id: str, revision_no: int) -> None:
        rev = self.expected_revisions[revision_id]
        rev["status"] = "APPLIED"
        if state_unit_id in self.state_units:
            self.state_units[state_unit_id]["applied_expected_rev"] = revision_no

    def record_policy_evaluation_plan(
        self,
        *,
        revision_id: str,
        opa_package: str,
        bundle_revision: str,
        input_sha256: str,
        decision: str,
        violations: list[str],
    ) -> dict[str, Any]:
        eval_id = str(uuid.uuid4())
        row = {
            "evaluation_id": eval_id,
            "stage": "plan",
            "request_id": None,
            "revision_id": revision_id,
            "opa_package": opa_package,
            "bundle_revision": bundle_revision,
            "input_sha256": input_sha256,
            "decision": decision,
            "violations": violations,
            "mutations": [],
            "evaluated_at": _utc_now_iso(),
        }
        self.policy_evaluations[eval_id] = row
        return row

    def ensure_state_unit(
        self,
        *,
        project_id: str,
        state_unit_id: str | None = None,
        lob: str = "spp",
        environment: str = "nonprod",
        apm_id: str = "APM12345",
        executor_identity: str | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        for su in self.state_units.values():
            if su["scope_id"] == project_id:
                return su
        su_id = state_unit_id or str(uuid.uuid5(uuid.NAMESPACE_DNS, f"gcp:project:{project_id}"))
        exec_id = executor_identity or f"idp-tf@{project_id}.iam.gserviceaccount.com"
        row = {
            "state_unit_id": su_id,
            "cloud": "gcp",
            "scope_type": "project",
            "scope_id": project_id,
            "lob": lob,
            "environment": environment,
            "apm_id": str(apm_id).lower(),
            "state_bucket": STATE_BUCKET,
            "state_prefix": f"projects/{project_id}",
            "executor_identity": exec_id,
            "current_expected_rev": None,
            "applied_expected_rev": None,
            "status": status,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        self.state_units[su_id] = row
        return row

    def set_state_unit_status(self, state_unit_id: str, status: str) -> None:
        if status not in {"active", "frozen", "decommissioning", "retired"}:
            raise ValueError(f"Invalid state_unit status: {status}")
        self.state_units[state_unit_id]["status"] = status
        self.state_units[state_unit_id]["updated_at"] = _utc_now_iso()

    def register_approved_resource(
        self,
        *,
        approved_doc: dict[str, Any],
        resource_id: str | None = None,
        request_id: str | None = None,
        approval_id: str | None = None,
        state_unit_id: str | None = None,
        base_approval_id: str | None = None,
        parent_resource_id: str | None = None,
        approval_status: str = "APPROVED",
    ) -> dict[str, Any]:
        """Seed or record a resource, request, and decided approval in the store (Gate 1 & Gate 2)."""
        _check_no_secrets(approved_doc, "approved_spec")
        action = approved_doc.get("action", "create")
        if approval_status == "AUTO_APPROVED" and action == "delete":
            raise ValueError("approval_no_auto_approve_delete: AUTO_APPROVED is forbidden when action == 'delete'")

        metadata = approved_doc["metadata"]
        spec = approved_doc["spec"]
        kind = approved_doc.get("kind", "gcs_bucket")
        project_id = metadata["project_id"]
        name = spec["name"]

        su = self.ensure_state_unit(
            project_id=project_id,
            state_unit_id=state_unit_id,
            lob=metadata.get("lob", "spp"),
            environment=metadata.get("environment", "nonprod"),
            apm_id=metadata.get("apm_id", "APM12345"),
        )
        su_id = su["state_unit_id"]

        res_id = resource_id or (metadata.get("labels") or {}).get("idp_resource_id") or str(uuid.uuid4())
        req_id = request_id or (approved_doc.get("approval") or {}).get("request_id") or str(uuid.uuid4())
        apr_id = approval_id or str(uuid.uuid4())

        kind_meta = self.resource_kinds[kind]
        instance_name = spec.get("instance") if kind == "spanner_database" else None
        tf_address = render_tf_address(
            kind_meta["tf_address_template"],
            name=name,
            instance=instance_name,
        )
        resource_key = (
            f"gcp:{project_id}:{kind}:{instance_name}/{name}"
            if kind == "spanner_database"
            else f"gcp:{project_id}:{kind}:{name}"
        )

        if parent_resource_id is None:
            parent_resource_id = approved_doc.get("parent_resource_id")
        if kind == "spanner_database" and instance_name and not parent_resource_id:
            for existing in self.resources.values():
                if (
                    existing["state_unit_id"] == su_id
                    and existing["kind"] == "spanner_instance"
                    and existing["name"] == instance_name
                ):
                    parent_resource_id = existing["resource_id"]
                    break

        approved_sha = canonical_sha256(approved_doc)

        existing_res = self.resources.get(res_id)
        prior_approval_id = base_approval_id or (existing_res["current_approval_id"] if existing_res else None)

        res_row = {
            "resource_id": res_id,
            "state_unit_id": su_id,
            "kind": kind,
            "name": name,
            "parent_resource_id": parent_resource_id,
            "resource_key": resource_key,
            "tf_address": tf_address,
            "apm_id": str(metadata["apm_id"]).lower(),
            "lifecycle": "deleting" if action == "delete" else (existing_res["lifecycle"] if existing_res else "proposed"),
            "gate": "APPROVED",
            "sync_status": "PENDING",
            "current_request_id": req_id,
            "current_approval_id": apr_id,
            "created_at": existing_res["created_at"] if existing_res else _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "deleted_at": None,
        }
        self.resources[res_id] = res_row

        req_row = {
            "request_id": req_id,
            "resource_id": res_id,
            "kind": kind,
            "action": action,
            "schema_version": "1.0.0",
            "requested_spec": copy.deepcopy(approved_doc),
            "requested_spec_sha256": approved_sha,
            "base_approval_id": prior_approval_id,
            "status": "APPROVED",
            "temporal_workflow_id": f"req-{req_id}",
            "requested_by": metadata.get("requested_by", "requester@example.com"),
            "submitted_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        self.requests[req_id] = req_row

        apr_row = {
            "approval_id": apr_id,
            "request_id": req_id,
            "resource_id": res_id,
            "gate_key": kind_meta["approval_gate_key"],
            "gate_version": 1,
            "status": approval_status,
            "approved_spec": copy.deepcopy(approved_doc),
            "approved_spec_sha256": approved_sha,
            "guardrail_trace": copy.deepcopy(approved_doc.get("guardrail_trace") or []),
            "temporal_workflow_id": f"apr-{req_id}",
            "decided_at": _utc_now_iso(),
            "created_at": _utc_now_iso(),
        }
        self.approvals[apr_id] = apr_row

        for prereq in approved_doc.get("prerequisites") or []:
            t_type = prereq["type"]
            target = prereq["target"]
            task_key = f"{t_type}:{target}"
            self.lifecycle_tasks[f"{apr_id}:{task_key}"] = {
                "task_id": str(uuid.uuid4()),
                "resource_id": res_id,
                "approval_id": apr_id,
                "phase": "pre_apply",
                "task_type": t_type,
                "task_key": task_key,
                "spec": copy.deepcopy(prereq),
                "status": "PENDING",
                "attempts": 0,
                "last_error": None,
                "updated_at": _utc_now_iso(),
            }

        for post in approved_doc.get("post_apply") or []:
            t_type = post["type"]
            target = post["target"]
            task_key = f"{t_type}:{target}"
            self.lifecycle_tasks[f"{apr_id}:{task_key}"] = {
                "task_id": str(uuid.uuid4()),
                "resource_id": res_id,
                "approval_id": apr_id,
                "phase": "post_apply",
                "task_type": t_type,
                "task_key": task_key,
                "spec": copy.deepcopy(post),
                "status": "PENDING",
                "attempts": 0,
                "last_error": None,
                "updated_at": _utc_now_iso(),
            }

        return {
            "state_unit": su,
            "resource": res_row,
            "request": req_row,
            "approval": apr_row,
        }

    def advance_current_expected_rev(self, state_unit_id: str, prev_rev: int | None, new_rev: int) -> bool:
        su = self.state_units[state_unit_id]
        if su["current_expected_rev"] != prev_rev:
            return False
        su["current_expected_rev"] = new_rev
        su["updated_at"] = _utc_now_iso()
        return True

    def compose_and_render_expected_revision(
        self,
        *,
        state_unit_id: str,
        trigger_approval_id: str,
        revision_id: str | None = None,
    ) -> dict[str, Any]:
        """Compose terraform.tfvars.json and manifest.json for a state unit and write with ifGenerationMatch=0 (INV-5)."""
        su = self.state_units[state_unit_id]
        project_id = su["scope_id"]
        prev_rev = su["current_expected_rev"]
        new_rev = (prev_rev or 0) + 1
        rev_id = revision_id or str(uuid.uuid4())

        tfvars: dict[str, dict[str, Any]] = {}
        manifest_members: list[dict[str, Any]] = []
        member_rows: list[dict[str, Any]] = []

        prior_member_approvals: dict[str, str] = {}
        if prev_rev is not None:
            for er in self.expected_revisions.values():
                if er["state_unit_id"] == state_unit_id and er["revision_no"] == prev_rev:
                    for m in self.expected_revision_members:
                        if m["revision_id"] == er["revision_id"] and m["change"] != "removed":
                            prior_member_approvals[m["resource_id"]] = m["approval_id"]

        for res in self.resources.values():
            if res["state_unit_id"] != state_unit_id:
                continue
            if res["lifecycle"] in {"deleted", "abandoned"}:
                continue

            apr_id = res["current_approval_id"]
            if not apr_id or apr_id not in self.approvals:
                continue
            apr = self.approvals[apr_id]
            if apr["status"] not in {"APPROVED", "AUTO_APPROVED"}:
                continue

            req = self.requests[res["current_request_id"]]
            action = req["action"]
            kind = res["kind"]
            kind_meta = self.resource_kinds[kind]
            collection = kind_meta["tfvars_collection"]
            tfvars.setdefault(collection, {})

            res_id = res["resource_id"]
            if action == "delete":
                change = "removed"
            elif res_id not in prior_member_approvals:
                change = "added"
            elif prior_member_approvals[res_id] != apr_id:
                change = "modified"
            else:
                change = "unchanged"

            if change != "removed":
                approved_spec = apr["approved_spec"]
                entry = convert_approved_to_expected_entry(kind, approved_spec)
                map_key = tfvars_collection_key(kind, name=res["name"], spec=approved_spec.get("spec") or {})
                tfvars[collection][map_key] = entry

            manifest_members.append(
                {
                    "resource_id": res_id,
                    "kind": kind,
                    "name": res["name"],
                    "approval_id": apr_id,
                    "approved_spec_sha256": apr["approved_spec_sha256"],
                    "change": change,
                    "tf_address": res["tf_address"],
                    "action": action,
                }
            )
            member_rows.append(
                {
                    "revision_id": rev_id,
                    "resource_id": res_id,
                    "approval_id": apr_id,
                    "change": change,
                    "tf_address": res["tf_address"],
                }
            )

        manifest = {
            "schema_version": "1.0.0",
            "state_unit_id": state_unit_id,
            "project_id": project_id,
            "revision_no": new_rev,
            "parent_revision_no": prev_rev,
            "trigger_approval_id": trigger_approval_id,
            "terraform_version": "1.10.5",
            "providers": {
                "hashicorp/google": ">= 6.0.0, < 7.0.0",
            },
            "modules": {
                k: v["tf_module_source"] for k, v in self.resource_kinds.items()
            },
            "members": manifest_members,
            "secret_references": {},
        }

        artifact_uri = build_artifact_uri(project_id, new_rev, su["state_bucket"])
        manifest_uri = artifact_uri.replace("/terraform.tfvars.json", "/manifest.json")

        artifact_gen, artifact_sha = self.put_gcs_object(artifact_uri, tfvars, if_generation_match=0)
        self.put_gcs_object(manifest_uri, manifest, if_generation_match=0)

        if not self.advance_current_expected_rev(state_unit_id, prev_rev, new_rev):
            raise OptimisticConcurrencyError(
                f"Lost optimistic concurrency race advancing current_expected_rev from {prev_rev} to {new_rev}"
            )

        rev_row = {
            "revision_id": rev_id,
            "state_unit_id": state_unit_id,
            "revision_no": new_rev,
            "parent_revision_no": prev_rev,
            "artifact_uri": artifact_uri,
            "artifact_generation": artifact_gen,
            "artifact_sha256": artifact_sha,
            "manifest": manifest,
            "plan_uri": None,
            "plan_summary": None,
            "status": "RENDERED",
            "blocked_reason": None,
            "created_at": _utc_now_iso(),
        }
        self.expected_revisions[rev_id] = rev_row
        self.expected_revision_members.extend(member_rows)

        for m in member_rows:
            self.resources[m["resource_id"]]["gate"] = "EXPECTED"

        self.append_gate_event(
            state_unit_id=state_unit_id,
            resource_id=member_rows[0]["resource_id"] if member_rows else None,
            request_id=None,
            gate="EXPECTED",
            event_type="expected.rendered",
            actor=f"system:su-gcp-{project_id}",
            detail={"revision_id": rev_id, "revision_no": new_rev, "artifact_uri": artifact_uri},
        )
        return rev_row

    def verify_revision_approval_hashes(self, revision_id: str) -> tuple[bool, str | None]:
        """Verify INV-6: every member's approved_spec_sha256 in manifest.json equals idp.approval.approved_spec_sha256."""
        rev = self.expected_revisions[revision_id]
        su = self.state_units[rev["state_unit_id"]]
        manifest_uri = rev["artifact_uri"].replace("/terraform.tfvars.json", "/manifest.json")
        gcs_manifest = self.get_gcs_json(manifest_uri)

        for member in gcs_manifest.get("members") or []:
            apr_id = member["approval_id"]
            manifest_hash = member.get("approved_spec_sha256")
            db_apr = self.approvals.get(apr_id)
            if not db_apr or db_apr["approved_spec_sha256"] != manifest_hash:
                rev["status"] = "BLOCKED"
                rev["blocked_reason"] = "approved_hash_mismatch"
                self.append_gate_event(
                    state_unit_id=su["state_unit_id"],
                    resource_id=member.get("resource_id"),
                    request_id=db_apr["request_id"] if db_apr else None,
                    gate="EXPECTED",
                    event_type="plan.blocked",
                    actor=f"system:su-gcp-{su['scope_id']}",
                    detail={"revision_id": revision_id, "blocked_reason": "approved_hash_mismatch"},
                )
                return False, "approved_hash_mismatch"
        return True, None

    def record_tf_operation(
        self,
        *,
        operation_id: str,
        state_unit_id: str,
        revision_id: str,
        phase: str,
        run_key: str,
        k8s_job: str,
        executor_identity: str,
        temporal_workflow_id: str,
        status: str,
        tf_exit_code: int | None = None,
        state_serial_before: int | None = None,
        state_serial_after: int | None = None,
        state_lineage: str | None = None,
        logs_uri: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        expected_job_name = k8s_job_name(operation_id, phase)
        if k8s_job != expected_job_name:
            raise ValueError(
                f"tf_operation_job_name_derived violation: {k8s_job!r} != {expected_job_name!r}"
            )
        if len(k8s_job) > 63:
            raise ValueError(f"tf_operation_job_name_len violation: {len(k8s_job)} > 63")
        if not operation_id.startswith(f"op:{state_unit_id}:r") or not operation_id.endswith(f":{phase}:{run_key}"):
            raise ValueError(f"tf_operation_id_format violation: {operation_id}")

        now = _utc_now_iso()
        existing = self.tf_operations.get(operation_id)
        row = {
            "operation_id": operation_id,
            "state_unit_id": state_unit_id,
            "revision_id": revision_id,
            "phase": phase,
            "run_key": run_key,
            "k8s_namespace": "spp-platform",
            "k8s_job_name": k8s_job,
            "executor_identity": executor_identity,
            "temporal_workflow_id": temporal_workflow_id,
            "status": status,
            "tf_exit_code": tf_exit_code,
            "state_serial_before": state_serial_before,
            "state_serial_after": state_serial_after,
            "state_lineage": state_lineage or str(uuid.uuid4()),
            "logs_uri": logs_uri,
            "error": error,
            "created_at": existing["created_at"] if existing else now,
            "started_at": existing["started_at"] if existing and existing.get("started_at") else now,
            "finished_at": now if status in {"SUCCEEDED", "FAILED", "CANCELLED"} else None,
        }
        self.tf_operations[operation_id] = row
        return row

    def record_observation(
        self,
        *,
        state_unit_id: str,
        source: str,
        operation_id: str | None,
        revision_id: str | None,
        state_uri: str,
        state_generation: int,
        state_serial: int,
        state_lineage: str = "<uuid>",
        snapshot_uri: str | None = None,
        drift_detected: bool = False,
        observation_id: str | None = None,
    ) -> dict[str, Any]:
        obs_id = observation_id or str(uuid.uuid4())
        row = {
            "observation_id": obs_id,
            "state_unit_id": state_unit_id,
            "source": source,
            "operation_id": operation_id,
            "revision_id": revision_id,
            "state_uri": state_uri,
            "state_generation": state_generation,
            "state_serial": state_serial,
            "state_lineage": state_lineage,
            "snapshot_uri": snapshot_uri,
            "drift_detected": drift_detected,
            "captured_at": _utc_now_iso(),
        }
        self.observations[obs_id] = row
        return row

    def upsert_resource_observed(
        self,
        *,
        resource_id: str,
        observation_id: str,
        exists_in_cloud: bool,
        cloud_resource_id: str | None,
        projection: dict[str, Any],
        projection_sha256: str | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        _check_no_secrets(projection, "resource_observed.projection")
        encoded = canonical_json(projection).encode("utf-8")
        if len(encoded) >= MAX_PROJECTION_BYTES:
            raise ValueError(
                f"resource_observed_projection_small check failed: {len(encoded)} bytes >= {MAX_PROJECTION_BYTES}"
            )
        sha = projection_sha256 or canonical_sha256(projection)

        existing = self.resource_observed.get(resource_id)
        if existing and existing["projection_sha256"] == sha and existing["exists_in_cloud"] == exists_in_cloud:
            return False, existing

        now = _utc_now_iso()
        row = {
            "resource_id": resource_id,
            "changed_in_observation_id": observation_id,
            "exists_in_cloud": exists_in_cloud,
            "cloud_resource_id": cloud_resource_id,
            "projection": copy.deepcopy(projection),
            "projection_sha256": sha,
            "changed_at": now,
        }
        self.resource_observed[resource_id] = row
        self.resource_observed_write_count[resource_id] = self.resource_observed_write_count.get(resource_id, 0) + 1
        return True, row

    def update_resource_after_apply(
        self,
        resource_id: str,
        *,
        lifecycle: str,
        gate: str = "OBSERVED",
        sync_status: str = "IN_SYNC",
    ) -> None:
        if resource_id in self.resources:
            self.resources[resource_id]["lifecycle"] = lifecycle
            self.resources[resource_id]["gate"] = gate
            self.resources[resource_id]["sync_status"] = sync_status
            if lifecycle == "deleted":
                self.resources[resource_id]["deleted_at"] = _utc_now_iso()

    def upsert_drift_finding(
        self,
        *,
        resource_id: str,
        observation_id: str,
        revision_id: str,
        drift_kind: str,
        attribute_path: str,
        expected_value: Any,
        observed_value: Any,
        severity: str,
        guardrail_rule_id: str | None = None,
        drift_id: str | None = None,
    ) -> dict[str, Any]:
        if drift_kind not in {"out_of_band_change", "config_divergence", "missing_in_cloud", "unmanaged_in_cloud"}:
            raise ValueError(f"Invalid drift_kind: {drift_kind}")
        if severity not in {"critical", "high", "medium", "low"}:
            raise ValueError(f"Invalid drift severity: {severity}")

        for existing in self.drift_findings.values():
            if (
                existing["resource_id"] == resource_id
                and existing["attribute_path"] == attribute_path
                and existing["status"] in {"OPEN", "ACKNOWLEDGED"}
            ):
                existing["observation_id"] = observation_id
                existing["revision_id"] = revision_id
                existing["drift_kind"] = drift_kind
                existing["expected_value"] = copy.deepcopy(expected_value)
                existing["observed_value"] = copy.deepcopy(observed_value)
                existing["severity"] = severity
                existing["guardrail_rule_id"] = guardrail_rule_id
                existing["detected_at"] = _utc_now_iso()
                return existing

        d_id = drift_id or str(uuid.uuid4())
        row = {
            "drift_id": d_id,
            "resource_id": resource_id,
            "observation_id": observation_id,
            "revision_id": revision_id,
            "drift_kind": drift_kind,
            "attribute_path": attribute_path,
            "expected_value": copy.deepcopy(expected_value),
            "observed_value": copy.deepcopy(observed_value),
            "severity": severity,
            "guardrail_rule_id": guardrail_rule_id,
            "status": "OPEN",
            "resolution": None,
            "detected_at": _utc_now_iso(),
            "resolved_at": None,
        }
        self.drift_findings[d_id] = row
        if resource_id in self.resources:
            self.resources[resource_id]["sync_status"] = "DRIFTED"
        return row

    def append_gate_event(
        self,
        *,
        state_unit_id: str | None,
        resource_id: str | None,
        request_id: str | None,
        gate: str,
        event_type: str,
        actor: str,
        detail: dict[str, Any],
    ) -> dict[str, Any]:
        event = {
            "event_id": len(self.gate_events) + 1,
            "occurred_at": _utc_now_iso(),
            "state_unit_id": state_unit_id,
            "resource_id": resource_id,
            "request_id": request_id,
            "gate": gate,
            "event_type": event_type,
            "actor": actor,
            "detail": copy.deepcopy(detail),
            "published_at": None,
        }
        self.gate_events.append(event)
        return event


class AlloyDbStateStore:
    """Real PostgreSQL / AlloyDB (`10.80.140.2:5432`, db `spp_platform`, schema `idp`) and GCS store (`gs://microdrive-dev-spp-tfstate`)."""

    def __init__(
        self,
        database_url: str | None = None,
        gcs_store: RealGcsObjectStore | None = None,
    ) -> None:
        self.database_url = database_url or resolve_database_url()
        if not self.database_url:
            raise ValueError("AlloyDbStateStore requires DATABASE_URL or SPP_ALLOYDB_* environment variables")
        self.gcs_store = gcs_store or RealGcsObjectStore()
        self.resource_kinds: dict[str, dict[str, Any]] = copy.deepcopy(DEFAULT_RESOURCE_KINDS)

    def _connect(self) -> Any:
        import psycopg  # type: ignore[import-untyped]
        from psycopg.rows import dict_row  # type: ignore[import-untyped]

        return psycopg.connect(self.database_url, row_factory=dict_row)

    @staticmethod
    def _stringify_uuids(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        out: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, uuid.UUID):
                out[k] = str(v)
            elif isinstance(v, datetime):
                out[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                out[k] = v
        return out

    def put_gcs_object(
        self,
        uri: str,
        payload: dict[str, Any] | str,
        *,
        if_generation_match: int | None = None,
    ) -> tuple[int, str]:
        return self.gcs_store.put_object(uri, payload, if_generation_match=if_generation_match)

    def get_gcs_text(self, uri: str) -> str:
        return self.gcs_store.get_text(uri)

    def get_gcs_json(self, uri: str) -> dict[str, Any]:
        return self.gcs_store.get_json(uri)

    def get_state_unit(
        self,
        *,
        state_unit_id: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if state_unit_id:
                    cur.execute(
                        "SELECT * FROM idp.state_unit WHERE state_unit_id = %s",
                        (state_unit_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return self._stringify_uuids(dict(row))
                if project_id:
                    cur.execute(
                        "SELECT * FROM idp.state_unit WHERE cloud = 'gcp' AND scope_id = %s",
                        (project_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return self._stringify_uuids(dict(row))
        return None

    def ensure_state_unit(
        self,
        *,
        project_id: str,
        state_unit_id: str | None = None,
        lob: str = "spp",
        environment: str = "nonprod",
        apm_id: str = "APM12345",
        executor_identity: str | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        existing = self.get_state_unit(state_unit_id=state_unit_id, project_id=project_id)
        if existing:
            return existing

        su_id = state_unit_id or str(uuid.uuid5(uuid.NAMESPACE_DNS, f"gcp:project:{project_id}"))
        exec_id = executor_identity or f"idp-tf@{project_id}.iam.gserviceaccount.com"
        norm_apm = str(apm_id).lower()

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.state_unit
                      (state_unit_id, cloud, scope_type, scope_id, lob, environment, apm_id,
                       state_bucket, state_prefix, executor_identity, status)
                    VALUES (%s, 'gcp', 'project', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cloud, scope_id) DO UPDATE
                      SET updated_at = now()
                    RETURNING *
                    """,
                    (
                        su_id,
                        project_id,
                        lob,
                        environment,
                        norm_apm,
                        STATE_BUCKET,
                        f"projects/{project_id}",
                        exec_id,
                        status,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) or {}

    def register_approved_resource(
        self,
        *,
        approved_doc: dict[str, Any],
        resource_id: str | None = None,
        request_id: str | None = None,
        approval_id: str | None = None,
        state_unit_id: str | None = None,
        base_approval_id: str | None = None,
        approval_status: str = "APPROVED",
    ) -> dict[str, Any]:
        """Ensure state_unit, resource, request, and approval exist in AlloyDB (idempotent if ssp-cp already inserted them)."""
        _check_no_secrets(approved_doc, "approved_spec")
        metadata = approved_doc.get("metadata") or {}
        spec = approved_doc.get("spec") or {}
        kind = approved_doc.get("kind", "gcs_bucket")
        action = approved_doc.get("action", "create")
        project_id = metadata.get("project_id", "microdrive-dev")
        name = spec["name"]
        apm_id = str(metadata.get("apm_id", "apm12345")).lower()

        su = self.ensure_state_unit(
            project_id=project_id,
            state_unit_id=state_unit_id,
            lob=metadata.get("lob", "spp"),
            environment=metadata.get("environment", "nonprod"),
            apm_id=apm_id,
        )
        su_id = su["state_unit_id"]

        res_id = resource_id or (metadata.get("labels") or {}).get("idp_resource_id") or str(uuid.uuid4())
        req_id = request_id or (approved_doc.get("approval") or {}).get("request_id") or str(uuid.uuid4())
        apr_id = approval_id or str(uuid.uuid4())

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM idp.approval WHERE approval_id = %s", (apr_id,))
                existing_apr = cur.fetchone()
                if existing_apr:
                    cur.execute("SELECT * FROM idp.resource WHERE resource_id = %s", (existing_apr["resource_id"],))
                    existing_res = cur.fetchone()
                    cur.execute("SELECT * FROM idp.request WHERE request_id = %s", (existing_apr["request_id"],))
                    existing_req = cur.fetchone()
                    return {
                        "state_unit": su,
                        "resource": self._stringify_uuids(dict(existing_res)) if existing_res else {},
                        "request": self._stringify_uuids(dict(existing_req)) if existing_req else {},
                        "approval": self._stringify_uuids(dict(existing_apr)),
                    }

                # Otherwise insert resource, submission, request, and approval (e.g., for standalone workflow runs or rollbacks)
                kind_meta = self.resource_kinds.get(kind) or DEFAULT_RESOURCE_KINDS["gcs_bucket"]
                instance_name = spec.get("instance") if kind == "spanner_database" else None
                tf_address = render_tf_address(
                    kind_meta["tf_address_template"],
                    name=name,
                    instance=instance_name,
                )
                resource_key = (
                    f"gcp:{project_id}:{kind}:{instance_name}/{name}"
                    if kind == "spanner_database"
                    else f"gcp:{project_id}:{kind}:{name}"
                )
                approved_sha = canonical_sha256(approved_doc)
                sub_id = str(uuid.uuid4())

                cur.execute(
                    """
                    INSERT INTO idp.submission
                      (submission_id, channel, apm_id, submitted_by, idempotency_key, raw_payload, raw_payload_sha256)
                    VALUES (%s, 'api', %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        sub_id,
                        apm_id,
                        metadata.get("requested_by", "requester@example.com"),
                        f"worker:{req_id}",
                        canonical_json(approved_doc),
                        approved_sha,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO idp.resource
                      (resource_id, state_unit_id, kind, name, resource_key, tf_address, apm_id, lifecycle, gate, sync_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'APPROVED', 'PENDING')
                    ON CONFLICT (resource_id) DO UPDATE
                      SET gate = 'APPROVED',
                          sync_status = 'PENDING',
                          lifecycle = CASE WHEN %s = 'delete' THEN 'deleting' ELSE idp.resource.lifecycle END,
                          updated_at = now()
                    RETURNING *
                    """,
                    (
                        res_id,
                        su_id,
                        kind,
                        name,
                        resource_key,
                        tf_address,
                        apm_id,
                        "deleting" if action == "delete" else "proposed",
                        action,
                    ),
                )
                res_row = cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO idp.request
                      (request_id, submission_id, resource_id, kind, action, schema_version,
                       requested_spec, requested_spec_sha256, base_approval_id, status, temporal_workflow_id, requested_by)
                    VALUES (%s, %s, %s, %s, %s, '1.0.0', %s::jsonb, %s, %s, 'APPROVED', %s, %s)
                    ON CONFLICT (request_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        req_id,
                        sub_id,
                        res_id,
                        kind,
                        action,
                        canonical_json(approved_doc),
                        approved_sha,
                        base_approval_id,
                        f"req-{req_id}",
                        metadata.get("requested_by", "requester@example.com"),
                    ),
                )
                req_row = cur.fetchone()
                if not req_row:
                    cur.execute("SELECT * FROM idp.request WHERE request_id = %s", (req_id,))
                    req_row = cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO idp.approval
                      (approval_id, request_id, resource_id, gate_key, gate_version, status,
                       approved_spec, approved_spec_sha256, guardrail_trace, temporal_workflow_id, decided_at)
                    VALUES (%s, %s, %s, %s, 1, %s, %s::jsonb, %s, %s::jsonb, %s, now())
                    ON CONFLICT (approval_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        apr_id,
                        req_id,
                        res_id,
                        kind_meta["approval_gate_key"],
                        approval_status,
                        canonical_json(approved_doc),
                        approved_sha,
                        canonical_json(approved_doc.get("guardrail_trace") or []),
                        f"apr-{req_id}",
                    ),
                )
                apr_row = cur.fetchone()
                if not apr_row:
                    cur.execute("SELECT * FROM idp.approval WHERE approval_id = %s", (apr_id,))
                    apr_row = cur.fetchone()

                cur.execute(
                    """
                    UPDATE idp.resource
                       SET current_request_id = %s,
                           current_approval_id = %s,
                           updated_at = now()
                     WHERE resource_id = %s
                    """,
                    (req_id, apr_id, res_id),
                )
            conn.commit()

        return {
            "state_unit": su,
            "resource": self._stringify_uuids(dict(res_row)) if res_row else {},
            "request": self._stringify_uuids(dict(req_row)) if req_row else {},
            "approval": self._stringify_uuids(dict(apr_row)) if apr_row else {},
        }

    def run_lifecycle_tasks(
        self,
        *,
        approval_id: str,
        phase: str,
        force_fail_task: str | None = None,
    ) -> dict[str, Any]:
        executed: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM idp.lifecycle_task
                     WHERE approval_id = %s AND phase = %s
                     ORDER BY task_key ASC
                    """,
                    (approval_id, phase),
                )
                rows = cur.fetchall()
                for row in rows:
                    t_id = row["task_id"]
                    t_type = row["task_type"]
                    attempts = int(row.get("attempts") or 0) + 1
                    if force_fail_task and t_type == force_fail_task:
                        err = f"Simulated failure in lifecycle task {row['task_key']}"
                        cur.execute(
                            """
                            UPDATE idp.lifecycle_task
                               SET status = 'FAILED', attempts = %s, last_error = %s, updated_at = now()
                             WHERE task_id = %s
                             RETURNING *
                            """,
                            (attempts, err, t_id),
                        )
                        updated = self._stringify_uuids(dict(cur.fetchone() or row))
                        executed.append(updated or {})
                        conn.commit()
                        return {"succeeded": False, "blocked_reason": "prerequisite_failed", "tasks": executed}

                    new_status = "SKIPPED" if t_type in {"cmdb_register", "dataportal_register"} else "SUCCEEDED"
                    cur.execute(
                        """
                        UPDATE idp.lifecycle_task
                           SET status = %s, attempts = %s, updated_at = now()
                         WHERE task_id = %s
                         RETURNING *
                        """,
                        (new_status, attempts, t_id),
                    )
                    updated = self._stringify_uuids(dict(cur.fetchone() or row))
                    executed.append(updated or {})
            conn.commit()
        return {"succeeded": True, "blocked_reason": None, "tasks": executed}

    def compose_and_render_expected_revision(
        self,
        *,
        state_unit_id: str,
        trigger_approval_id: str,
        revision_id: str | None = None,
    ) -> dict[str, Any]:
        """Compose terraform.tfvars.json and manifest.json from AlloyDB state_unit resources, write to GCS with ifGenerationMatch=0, and persist idp.expected_revision."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM idp.state_unit WHERE state_unit_id = %s", (state_unit_id,))
                su = cur.fetchone()
                if not su:
                    raise KeyError(f"StateUnit {state_unit_id} not found in AlloyDB")

                project_id = su["scope_id"]
                prev_rev = su["current_expected_rev"]
                new_rev = (prev_rev or 0) + 1
                rev_id = revision_id or str(uuid.uuid4())

                # Fetch resource kinds
                cur.execute("SELECT * FROM idp.resource_kind WHERE enabled = true")
                kind_rows = {r["kind"]: dict(r) for r in cur.fetchall()}
                if not kind_rows:
                    kind_rows = copy.deepcopy(DEFAULT_RESOURCE_KINDS)

                # Determine prior revision member approvals
                prior_member_approvals: dict[str, str] = {}
                if prev_rev is not None:
                    cur.execute(
                        """
                        SELECT m.resource_id, m.approval_id, m.change
                          FROM idp.expected_revision_member m
                          JOIN idp.expected_revision e ON e.revision_id = m.revision_id
                         WHERE e.state_unit_id = %s AND e.revision_no = %s
                        """,
                        (state_unit_id, prev_rev),
                    )
                    for mr in cur.fetchall():
                        if mr["change"] != "removed":
                            prior_member_approvals[str(mr["resource_id"])] = str(mr["approval_id"])

                # Fetch active/proposed/deleting resources in this state unit
                cur.execute(
                    """
                    SELECT r.*, rq.action AS request_action, ap.approval_id AS ap_id,
                           ap.status AS ap_status, ap.approved_spec, ap.approved_spec_sha256
                      FROM idp.resource r
                      LEFT JOIN idp.request rq ON rq.request_id = r.current_request_id
                      LEFT JOIN idp.approval ap ON ap.approval_id = r.current_approval_id
                     WHERE r.state_unit_id = %s
                       AND r.lifecycle NOT IN ('deleted', 'abandoned')
                     ORDER BY r.created_at ASC
                    """,
                    (state_unit_id,),
                )
                resources = cur.fetchall()

                tfvars: dict[str, dict[str, Any]] = {}
                manifest_members: list[dict[str, Any]] = []
                member_rows: list[dict[str, Any]] = []

                for res in resources:
                    apr_id = str(res["ap_id"]) if res.get("ap_id") else None
                    if not apr_id or res.get("ap_status") not in {"APPROVED", "AUTO_APPROVED"}:
                        continue

                    res_id = str(res["resource_id"])
                    action = res.get("request_action") or "create"
                    kind = res["kind"]
                    kind_meta = kind_rows.get(kind) or DEFAULT_RESOURCE_KINDS["gcs_bucket"]
                    collection = kind_meta["tfvars_collection"]
                    tfvars.setdefault(collection, {})

                    if action == "delete":
                        change = "removed"
                    elif res_id not in prior_member_approvals:
                        change = "added"
                    elif prior_member_approvals[res_id] != apr_id:
                        change = "modified"
                    else:
                        change = "unchanged"

                    approved_spec = res["approved_spec"]
                    if change != "removed":
                        entry = convert_approved_to_expected_entry(kind, approved_spec)
                        spec_body = approved_spec.get("spec") or {}
                        map_key = tfvars_collection_key(kind, name=res["name"], spec=spec_body)
                        tfvars[collection][map_key] = entry

                    manifest_members.append(
                        {
                            "resource_id": res_id,
                            "kind": kind,
                            "name": res["name"],
                            "approval_id": apr_id,
                            "approved_spec_sha256": res["approved_spec_sha256"],
                            "change": change,
                            "tf_address": res["tf_address"],
                            "action": action,
                        }
                    )
                    member_rows.append(
                        {
                            "revision_id": rev_id,
                            "resource_id": res_id,
                            "approval_id": apr_id,
                            "change": change,
                            "tf_address": res["tf_address"],
                        }
                    )

                manifest = {
                    "schema_version": "1.0.0",
                    "state_unit_id": str(state_unit_id),
                    "project_id": project_id,
                    "revision_no": new_rev,
                    "parent_revision_no": prev_rev,
                    "trigger_approval_id": str(trigger_approval_id),
                    "terraform_version": "1.10.5",
                    "providers": {"hashicorp/google": ">= 6.0.0, < 7.0.0"},
                    "modules": {k: v["tf_module_source"] for k, v in kind_rows.items()},
                    "members": manifest_members,
                    "secret_references": {},
                }

                artifact_uri = build_artifact_uri(project_id, new_rev, su["state_bucket"])
                manifest_uri = artifact_uri.replace("/terraform.tfvars.json", "/manifest.json")

                # Upload Expected artifacts to GCS with ifGenerationMatch=0 (create-only, INV-5)
                artifact_gen, artifact_sha = self.put_gcs_object(artifact_uri, tfvars, if_generation_match=0)
                self.put_gcs_object(manifest_uri, manifest, if_generation_match=0)

                # Advance state_unit.current_expected_rev with optimistic concurrency
                cur.execute(
                    """
                    UPDATE idp.state_unit
                       SET current_expected_rev = %s,
                           updated_at = now()
                     WHERE state_unit_id = %s
                       AND current_expected_rev IS NOT DISTINCT FROM %s
                    """,
                    (new_rev, state_unit_id, prev_rev),
                )
                if cur.rowcount == 0:
                    raise OptimisticConcurrencyError(
                        f"Lost optimistic concurrency race advancing current_expected_rev from {prev_rev} to {new_rev}"
                    )

                cur.execute(
                    """
                    INSERT INTO idp.expected_revision
                      (revision_id, state_unit_id, revision_no, parent_revision_no,
                       artifact_uri, artifact_generation, artifact_sha256, manifest, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'RENDERED')
                    RETURNING *
                    """,
                    (
                        rev_id,
                        state_unit_id,
                        new_rev,
                        prev_rev,
                        artifact_uri,
                        artifact_gen,
                        artifact_sha,
                        canonical_json(manifest),
                    ),
                )
                rev_row = self._stringify_uuids(dict(cur.fetchone() or {}))

                for m in member_rows:
                    cur.execute(
                        """
                        INSERT INTO idp.expected_revision_member
                          (revision_id, resource_id, approval_id, change, tf_address)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            m["revision_id"],
                            m["resource_id"],
                            m["approval_id"],
                            m["change"],
                            m["tf_address"],
                        ),
                    )
                    cur.execute(
                        "UPDATE idp.resource SET gate = 'EXPECTED', updated_at = now() WHERE resource_id = %s",
                        (m["resource_id"],),
                    )

                cur.execute(
                    """
                    INSERT INTO idp.gate_event
                      (state_unit_id, resource_id, gate, event_type, actor, detail)
                    VALUES (%s, %s, 'EXPECTED', 'expected.rendered', %s, %s::jsonb)
                    """,
                    (
                        state_unit_id,
                        member_rows[0]["resource_id"] if member_rows else None,
                        f"system:su-gcp-{project_id}",
                        canonical_json(
                            {"revision_id": rev_id, "revision_no": new_rev, "artifact_uri": artifact_uri}
                        ),
                    ),
                )
            conn.commit()
        return rev_row or {}

    def get_expected_revision(self, revision_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM idp.expected_revision WHERE revision_id = %s", (revision_id,))
                row = cur.fetchone()
                if not row:
                    raise KeyError(f"ExpectedRevision {revision_id} not found in AlloyDB")
                return self._stringify_uuids(dict(row)) or {}

    def verify_revision_approval_hashes(self, revision_id: str) -> tuple[bool, str | None]:
        """Verify INV-6: every member's approved_spec_sha256 in GCS manifest.json equals idp.approval.approved_spec_sha256 in AlloyDB."""
        rev = self.get_expected_revision(revision_id)
        su = self.get_state_unit(state_unit_id=rev["state_unit_id"]) or {}
        manifest_uri = rev["artifact_uri"].replace("/terraform.tfvars.json", "/manifest.json")
        gcs_manifest = self.get_gcs_json(manifest_uri)

        with self._connect() as conn:
            with conn.cursor() as cur:
                for member in gcs_manifest.get("members") or []:
                    apr_id = member["approval_id"]
                    manifest_hash = member.get("approved_spec_sha256")
                    cur.execute(
                        "SELECT request_id, approved_spec_sha256 FROM idp.approval WHERE approval_id = %s",
                        (apr_id,),
                    )
                    db_apr = cur.fetchone()
                    if not db_apr or db_apr["approved_spec_sha256"] != manifest_hash:
                        cur.execute(
                            """
                            UPDATE idp.expected_revision
                               SET status = 'BLOCKED', blocked_reason = 'approved_hash_mismatch'
                             WHERE revision_id = %s
                            """,
                            (revision_id,),
                        )
                        cur.execute(
                            """
                            INSERT INTO idp.gate_event
                              (state_unit_id, resource_id, request_id, gate, event_type, actor, detail)
                            VALUES (%s, %s, %s, 'EXPECTED', 'plan.blocked', %s, %s::jsonb)
                            """,
                            (
                                rev["state_unit_id"],
                                member.get("resource_id"),
                                str(db_apr["request_id"]) if db_apr else None,
                                f"system:su-gcp-{su.get('scope_id', 'unknown')}",
                                canonical_json(
                                    {"revision_id": revision_id, "blocked_reason": "approved_hash_mismatch"}
                                ),
                            ),
                        )
                        conn.commit()
                        return False, "approved_hash_mismatch"
        return True, None

    def update_expected_revision_plan(
        self,
        revision_id: str,
        *,
        status: str,
        plan_uri: str | None = None,
        plan_summary: dict[str, int] | None = None,
        blocked_reason: str | None = None,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE idp.expected_revision
                       SET status = %s,
                           plan_uri = COALESCE(%s, plan_uri),
                           plan_summary = COALESCE(%s::jsonb, plan_summary),
                           blocked_reason = %s
                     WHERE revision_id = %s
                     RETURNING *
                    """,
                    (
                        status,
                        plan_uri,
                        canonical_json(plan_summary) if plan_summary is not None else None,
                        blocked_reason,
                        revision_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def mark_expected_revision_applied(self, revision_id: str, state_unit_id: str, revision_no: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE idp.expected_revision SET status = 'APPLIED' WHERE revision_id = %s",
                    (revision_id,),
                )
                cur.execute(
                    "UPDATE idp.state_unit SET applied_expected_rev = %s, updated_at = now() WHERE state_unit_id = %s",
                    (revision_no, state_unit_id),
                )
            conn.commit()

    def record_policy_evaluation_plan(
        self,
        *,
        revision_id: str,
        opa_package: str,
        bundle_revision: str,
        input_sha256: str,
        decision: str,
        violations: list[str],
    ) -> dict[str, Any]:
        eval_id = str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.policy_evaluation
                      (evaluation_id, stage, request_id, revision_id, opa_package,
                       bundle_revision, input_sha256, decision, violations, mutations)
                    VALUES (%s, 'plan', NULL, %s, %s, %s, %s, %s, %s::jsonb, '[]'::jsonb)
                    RETURNING *
                    """,
                    (
                        eval_id,
                        revision_id,
                        opa_package,
                        bundle_revision,
                        input_sha256,
                        decision,
                        canonical_json(violations),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def record_tf_operation(
        self,
        *,
        operation_id: str,
        state_unit_id: str,
        revision_id: str,
        phase: str,
        run_key: str,
        k8s_job: str,
        executor_identity: str,
        temporal_workflow_id: str,
        status: str,
        tf_exit_code: int | None = None,
        state_serial_before: int | None = None,
        state_serial_after: int | None = None,
        state_lineage: str | None = None,
        logs_uri: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        expected_job_name = k8s_job_name(operation_id, phase)
        if k8s_job != expected_job_name:
            raise ValueError(
                f"tf_operation_job_name_derived violation: {k8s_job!r} != {expected_job_name!r}"
            )
        lineage = state_lineage or str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.tf_operation
                      (operation_id, state_unit_id, revision_id, phase, run_key,
                       k8s_namespace, k8s_job_name, executor_identity, temporal_workflow_id,
                       status, tf_exit_code, state_serial_before, state_serial_after,
                       state_lineage, logs_uri, error, started_at, finished_at)
                    VALUES (%s, %s, %s, %s, %s, 'spp-platform', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now(),
                            CASE WHEN %s IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN now() ELSE NULL END)
                    ON CONFLICT (operation_id) DO UPDATE
                      SET status = EXCLUDED.status,
                          tf_exit_code = EXCLUDED.tf_exit_code,
                          state_serial_before = EXCLUDED.state_serial_before,
                          state_serial_after = EXCLUDED.state_serial_after,
                          state_lineage = EXCLUDED.state_lineage,
                          logs_uri = EXCLUDED.logs_uri,
                          error = EXCLUDED.error,
                          finished_at = CASE WHEN EXCLUDED.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN now() ELSE idp.tf_operation.finished_at END
                    RETURNING *
                    """,
                    (
                        operation_id,
                        state_unit_id,
                        revision_id,
                        phase,
                        run_key,
                        k8s_job,
                        executor_identity,
                        temporal_workflow_id,
                        status,
                        tf_exit_code,
                        state_serial_before,
                        state_serial_after,
                        lineage,
                        logs_uri,
                        canonical_json(error) if error is not None else None,
                        status,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def record_observation(
        self,
        *,
        state_unit_id: str,
        source: str,
        operation_id: str | None,
        revision_id: str | None,
        state_uri: str,
        state_generation: int,
        state_serial: int,
        state_lineage: str = "<uuid>",
        snapshot_uri: str | None = None,
        drift_detected: bool = False,
        observation_id: str | None = None,
    ) -> dict[str, Any]:
        obs_id = observation_id or str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.observation
                      (observation_id, state_unit_id, source, operation_id, revision_id,
                       state_uri, state_generation, state_serial, state_lineage,
                       snapshot_uri, drift_detected, captured_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    RETURNING *
                    """,
                    (
                        obs_id,
                        state_unit_id,
                        source,
                        operation_id,
                        revision_id,
                        state_uri,
                        state_generation,
                        state_serial,
                        state_lineage,
                        snapshot_uri,
                        drift_detected,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def upsert_resource_observed(
        self,
        *,
        resource_id: str,
        observation_id: str,
        exists_in_cloud: bool,
        cloud_resource_id: str | None,
        projection: dict[str, Any],
        projection_sha256: str | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Upsert idp.resource_observed ONLY when projection_sha256 changes (ADR-GLB-003 REQ-6)."""
        _check_no_secrets(projection, "resource_observed.projection")
        encoded = canonical_json(projection).encode("utf-8")
        if len(encoded) >= MAX_PROJECTION_BYTES:
            raise ValueError(
                f"resource_observed_projection_small check failed: {len(encoded)} bytes >= {MAX_PROJECTION_BYTES}"
            )
        sha = projection_sha256 or canonical_sha256(projection)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.resource_observed AS ro
                      (resource_id, changed_in_observation_id, exists_in_cloud,
                       cloud_resource_id, projection, projection_sha256, changed_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, now())
                    ON CONFLICT (resource_id) DO UPDATE
                      SET changed_in_observation_id = EXCLUDED.changed_in_observation_id,
                          exists_in_cloud           = EXCLUDED.exists_in_cloud,
                          cloud_resource_id         = EXCLUDED.cloud_resource_id,
                          projection                = EXCLUDED.projection,
                          projection_sha256         = EXCLUDED.projection_sha256,
                          changed_at                = EXCLUDED.changed_at
                    WHERE ro.projection_sha256 IS DISTINCT FROM EXCLUDED.projection_sha256
                       OR ro.exists_in_cloud IS DISTINCT FROM EXCLUDED.exists_in_cloud
                    RETURNING *
                    """,
                    (
                        resource_id,
                        observation_id,
                        exists_in_cloud,
                        cloud_resource_id,
                        canonical_json(projection),
                        sha,
                    ),
                )
                row = cur.fetchone()
                was_written = row is not None
                if row is None:
                    cur.execute("SELECT * FROM idp.resource_observed WHERE resource_id = %s", (resource_id,))
                    row = cur.fetchone()
            conn.commit()
        return was_written, (self._stringify_uuids(dict(row)) if row else {})

    def update_resource_after_apply(
        self,
        resource_id: str,
        *,
        lifecycle: str,
        gate: str = "OBSERVED",
        sync_status: str = "IN_SYNC",
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE idp.resource
                       SET lifecycle = %s,
                           gate = %s,
                           sync_status = %s,
                           deleted_at = CASE WHEN %s = 'deleted' THEN now() ELSE deleted_at END,
                           updated_at = now()
                     WHERE resource_id = %s
                    """,
                    (lifecycle, gate, sync_status, lifecycle, resource_id),
                )
            conn.commit()

    def upsert_drift_finding(
        self,
        *,
        resource_id: str,
        observation_id: str,
        revision_id: str,
        drift_kind: str,
        attribute_path: str,
        expected_value: Any,
        observed_value: Any,
        severity: str,
        guardrail_rule_id: str | None = None,
        drift_id: str | None = None,
    ) -> dict[str, Any]:
        d_id = drift_id or str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM idp.drift_finding
                     WHERE resource_id = %s
                       AND attribute_path = %s
                       AND status IN ('OPEN', 'ACKNOWLEDGED')
                     LIMIT 1
                    """,
                    (resource_id, attribute_path),
                )
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        """
                        UPDATE idp.drift_finding
                           SET observation_id = %s,
                               revision_id = %s,
                               drift_kind = %s,
                               expected_value = %s::jsonb,
                               observed_value = %s::jsonb,
                               severity = %s,
                               guardrail_rule_id = %s,
                               detected_at = now()
                         WHERE drift_id = %s
                         RETURNING *
                        """,
                        (
                            observation_id,
                            revision_id,
                            drift_kind,
                            canonical_json(expected_value),
                            canonical_json(observed_value),
                            severity,
                            guardrail_rule_id,
                            existing["drift_id"],
                        ),
                    )
                    row = cur.fetchone()
                else:
                    cur.execute(
                        """
                        INSERT INTO idp.drift_finding
                          (drift_id, resource_id, observation_id, revision_id,
                           drift_kind, attribute_path, expected_value, observed_value,
                           severity, guardrail_rule_id, status, detected_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, 'OPEN', now())
                        RETURNING *
                        """,
                        (
                            d_id,
                            resource_id,
                            observation_id,
                            revision_id,
                            drift_kind,
                            attribute_path,
                            canonical_json(expected_value),
                            canonical_json(observed_value),
                            severity,
                            guardrail_rule_id,
                        ),
                    )
                    row = cur.fetchone()

                cur.execute(
                    "UPDATE idp.resource SET sync_status = 'DRIFTED', updated_at = now() WHERE resource_id = %s",
                    (resource_id,),
                )
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def append_gate_event(
        self,
        *,
        state_unit_id: str | None,
        resource_id: str | None,
        request_id: str | None,
        gate: str,
        event_type: str,
        actor: str,
        detail: dict[str, Any],
    ) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idp.gate_event
                      (state_unit_id, resource_id, request_id, gate, event_type, actor, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING *
                    """,
                    (
                        state_unit_id,
                        resource_id,
                        request_id,
                        gate,
                        event_type,
                        actor,
                        canonical_json(detail),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._stringify_uuids(dict(row)) if row else {}

    def list_active_resources_for_state_unit(self, state_unit_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT r.*, ap.approved_spec
                      FROM idp.resource r
                      LEFT JOIN idp.approval ap ON ap.approval_id = r.current_approval_id
                     WHERE r.state_unit_id = %s
                       AND r.lifecycle NOT IN ('deleted', 'abandoned')
                     ORDER BY r.created_at ASC
                    """,
                    (state_unit_id,),
                )
                return [self._stringify_uuids(dict(r)) or {} for r in cur.fetchall()]

    def get_latest_expected_revision_for_state_unit(
        self, state_unit_id: str, revision_no: int | None = None
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if revision_no is not None:
                    cur.execute(
                        "SELECT * FROM idp.expected_revision WHERE state_unit_id = %s AND revision_no = %s",
                        (state_unit_id, revision_no),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM idp.expected_revision
                         WHERE state_unit_id = %s
                         ORDER BY revision_no DESC
                         LIMIT 1
                        """,
                        (state_unit_id,),
                    )
                row = cur.fetchone()
                return self._stringify_uuids(dict(row)) if row else None

    def get_rollback_target_approval(
        self, resource_id: str, target_approval_id: str | None = None
    ) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM idp.resource WHERE resource_id = %s", (resource_id,))
                res = cur.fetchone()
                if not res:
                    raise KeyError(f"Resource {resource_id} not found")
                if not target_approval_id and res.get("current_request_id"):
                    cur.execute(
                        "SELECT base_approval_id FROM idp.request WHERE request_id = %s",
                        (res["current_request_id"],),
                    )
                    req = cur.fetchone()
                    if req and req.get("base_approval_id"):
                        target_approval_id = str(req["base_approval_id"])
                if not target_approval_id:
                    raise ValueError(f"No prior approved specification found to roll back resource {resource_id}")
                cur.execute("SELECT * FROM idp.approval WHERE approval_id = %s", (target_approval_id,))
                apr = cur.fetchone()
                if not apr:
                    raise ValueError(f"Approval {target_approval_id} not found for rollback")
                return self._stringify_uuids(dict(apr)) or {}


DEFAULT_STORE = InMemoryStateStore()
_REAL_STORE: AlloyDbStateStore | None = None


def get_active_store() -> AlloyDbStateStore | InMemoryStateStore:
    """Return `AlloyDbStateStore` (backed by real AlloyDB + real GCS) whenever configured, or `DEFAULT_STORE` in unit tests."""
    global _REAL_STORE
    if is_real_db_configured():
        if _REAL_STORE is None:
            _REAL_STORE = AlloyDbStateStore(
                database_url=resolve_database_url(),
                gcs_store=RealGcsObjectStore(),
            )
        return _REAL_STORE
    if is_real_gcs_configured() and DEFAULT_STORE.gcs_store is None:
        DEFAULT_STORE.gcs_store = RealGcsObjectStore()
    return DEFAULT_STORE
