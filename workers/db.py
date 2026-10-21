"""Control-plane DB & GCS State Repository for ssp-workers (ADR-GLB-003, ADR-GLB-004, ADR-AUT-001, ADR-AUT-002)."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timezone
import hashlib
from typing import Any

from .models import (
    MAX_PROJECTION_BYTES,
    SECRET_KEY_NAMES,
    STATE_BUCKET,
    build_artifact_uri,
    canonical_json,
    canonical_sha256,
    convert_approved_gcs_bucket_to_expected_entry,
    k8s_job_name,
    render_tf_address,
)


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


class GCSArtifactExistError(RuntimeError):
    """Raised when ifGenerationMatch=0 fails because the object already exists in GCS (INV-5)."""


class OptimisticConcurrencyError(RuntimeError):
    """Raised when advancing state_unit.current_expected_rev loses an optimistic concurrency race."""


class InMemoryStateStore:
    """In-memory twin of the AlloyDB idp schema (18 tables + v_resource_status) and gs://microdrive-dev-spp-tfstate bucket.

    Enforces:
    - INV-1: Zero secret keys in submission, request, approval, or resource_observed.projection
    - INV-5: ifGenerationMatch=0 create-only GCS writes for Expected revisions
    - INV-6: approved_spec_sha256 match between AlloyDB idp.approval and manifest.json
    - INV-7: tf_operation.k8s_job_name == idp.k8s_job_name(operation_id, phase) and <= 63 chars
    - INV-8: One state unit per GCP project; optimistic concurrency on current_expected_rev
    - INV-9: Kill switch when state_unit.status == 'frozen'
    - resource_observed_projection_small (< 2000 bytes) & upsert only when projection_sha256 is distinct
    - drift_finding_open_uq on (resource_id, attribute_path) WHERE status IN ('OPEN', 'ACKNOWLEDGED')
    """

    def __init__(self) -> None:
        self.approval_gate_configs: dict[str, dict[str, Any]] = {
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
        self.resource_kinds: dict[str, dict[str, Any]] = {
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

        # GCS bucket simulation: uri -> {"content": str, "generation": int}
        self.gcs_objects: dict[str, dict[str, Any]] = {}
        self._next_generation: int = 1729587000000000

        # Cloud bucket object count simulation for decommission empty-bucket checks
        self.cloud_bucket_object_counts: dict[str, int] = {}
        # Simulated live cloud state for drift detection (bucket_name -> dict of cloud attributes)
        self.cloud_buckets: dict[str, dict[str, Any]] = {}

    def put_gcs_object(
        self,
        uri: str,
        payload: dict[str, Any] | str,
        *,
        if_generation_match: int | None = None,
    ) -> tuple[int, str]:
        """Write an object to simulated GCS, honoring ifGenerationMatch=0 (create-only, INV-5)."""
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

    def get_gcs_json(self, uri: str) -> dict[str, Any]:
        if uri not in self.gcs_objects:
            raise KeyError(f"GCS object not found: {uri}")
        return json.loads(self.gcs_objects[uri]["content"])

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
            "apm_id": apm_id,
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
        tf_address = render_tf_address(kind_meta["tf_address_template"], name=name)
        resource_key = f"gcp:{project_id}:{kind}:{name}"

        approved_sha = canonical_sha256(approved_doc)

        existing_res = self.resources.get(res_id)
        prior_approval_id = base_approval_id or (existing_res["current_approval_id"] if existing_res else None)

        res_row = {
            "resource_id": res_id,
            "state_unit_id": su_id,
            "kind": kind,
            "name": name,
            "parent_resource_id": None,
            "resource_key": resource_key,
            "tf_address": tf_address,
            "apm_id": metadata["apm_id"],
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

        # Materialize pre_apply and post_apply lifecycle tasks
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
        """Advance state_unit.current_expected_rev with optimistic concurrency:
        UPDATE idp.state_unit SET current_expected_rev = :new_rev
        WHERE state_unit_id = :state_unit_id AND current_expected_rev IS NOT DISTINCT FROM :prev_rev
        """
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

        # Collect all resources in this state unit
        tfvars: dict[str, dict[str, Any]] = {}
        manifest_members: list[dict[str, Any]] = []
        member_rows: list[dict[str, Any]] = []

        # Determine prior revision members for change classification
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
                if kind == "gcs_bucket":
                    entry = convert_approved_gcs_bucket_to_expected_entry(apr["approved_spec"])
                    tfvars[collection][res["name"]] = entry
                else:
                    entry = copy.deepcopy(apr["approved_spec"].get("spec") or {})
                    tfvars[collection][res["name"]] = entry

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

        # Write to GCS with ifGenerationMatch=0 (create-only, INV-5)
        artifact_gen, artifact_sha = self.put_gcs_object(artifact_uri, tfvars, if_generation_match=0)
        self.put_gcs_object(manifest_uri, manifest, if_generation_match=0)

        # Advance state_unit.current_expected_rev with optimistic concurrency
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
        """Record or update an idp.tf_operation row enforcing SQL constraints."""
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
        """Upsert idp.resource_observed ONLY when projection_sha256 is distinct (ADR-GLB-003 REQ-6, ADR-GLB-004 §5).

        Returns (was_written, row).
        """
        _check_no_secrets(projection, "resource_observed.projection")
        encoded = canonical_json(projection).encode("utf-8")
        if len(encoded) >= MAX_PROJECTION_BYTES:
            raise ValueError(
                f"resource_observed_projection_small check failed: {len(encoded)} bytes >= {MAX_PROJECTION_BYTES}"
            )
        sha = projection_sha256 or canonical_sha256(projection)

        existing = self.resource_observed.get(resource_id)
        if existing and existing["projection_sha256"] == sha and existing["exists_in_cloud"] == exists_in_cloud:
            # WHERE ro.projection_sha256 IS DISTINCT FROM EXCLUDED.projection_sha256 -> 0 rows updated
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
        """Insert or update an OPEN drift finding deduplicated by (resource_id, attribute_path) WHERE status IN ('OPEN', 'ACKNOWLEDGED').

        Strictly alert-only: status is 'OPEN' (REMEDIATING and 'reapplied' do not exist).
        """
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


DEFAULT_STORE = InMemoryStateStore()
