"""Temporal Workflows for ssp-workers (WI-002, WI-004, WI-005, ADR-AUT-001, ADR-AUT-002, ADR-BP-003)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow

ACTIVITY_TIMEOUT = timedelta(minutes=30)


@workflow.defn(name="GcsBucketProvisionWorkflow")
class GcsBucketProvisionWorkflow:
    """Orchestrates end-to-end GCS bucket provisioning, rollback, and decommission (WI-002, WI-004):

    1. Check state_unit kill switch (status != 'frozen', INV-9).
    2. For decommission/delete: verify bucket is empty (never force-delete, GCS-DEL-001).
    3. For rollback: restore last approved specification as a new revision.
    4. Execute pre_apply lifecycle tasks (ad_group, vpc_sc_ingress).
    5. Render create-only Expected revision (manifest.json + terraform.tfvars.json, tfvars_collection='gcs_buckets',
       tf_address='module.gcs_bucket[\"{name}\"]', seconds conversions) to gs://microdrive-dev-spp-tfstate/projects/<project>/expected/rev-NNNNNN/
       with ifGenerationMatch=0.
    6. Launch Terraform plan K8s Job tf-plan-<20hex> in namespace spp-platform and run conftest against ssp-cp policy bundle.
    7. Wait for human approval signal before apply (human_approval / approve_apply).
    8. Launch Terraform apply K8s Job tf-apply-<20hex> in namespace spp-platform.
    9. Record Observed (<= 2 KB projection) back to control-plane DB and execute post_apply lifecycle tasks.
    """

    def __init__(self) -> None:
        self.status: str = "RUNNING"
        self.blocked_reason: str | None = None
        self.awaiting_apply_approval: bool = False
        self.revision_id: str | None = None
        self.revision_no: int | None = None
        self.plan_job_name: str | None = None
        self.apply_job_name: str | None = None
        self.plan_summary: dict[str, int] | None = None
        self._apply_approval_signal: dict[str, Any] | None = None
        self._rollback_requested: dict[str, Any] | None = None
        self.history: list[dict[str, Any]] = []

    @workflow.signal
    def approve_apply(self, payload: dict[str, Any]) -> None:
        """Human approval signal before executing tf-apply-<20hex>."""
        self._apply_approval_signal = payload

    @workflow.signal
    def human_approval(self, payload: dict[str, Any]) -> None:
        """Alias signal for human approval before apply."""
        self._apply_approval_signal = payload

    @workflow.signal
    def request_rollback(self, payload: dict[str, Any]) -> None:
        """Trigger rollback to the last approved specification."""
        self._rollback_requested = payload

    @workflow.query
    def get_state(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "blocked_reason": self.blocked_reason,
            "awaiting_apply_approval": self.awaiting_apply_approval,
            "revision_id": self.revision_id,
            "revision_no": self.revision_no,
            "plan_job_name": self.plan_job_name,
            "apply_job_name": self.apply_job_name,
            "plan_summary": self.plan_summary,
            "history": self.history,
        }

    @workflow.run
    async def run(self, params: dict[str, Any]) -> dict[str, Any]:
        approved_payload = params.get("approved_spec") or params.get("approved_doc") or {}
        action = params.get("action") or approved_payload.get("action", "create")
        project_id = params.get("project_id") or (approved_payload.get("metadata") or {}).get(
            "project_id", "microdrive-dev"
        )
        state_unit_id = params.get("state_unit_id")
        require_apply_signal = bool(params.get("require_apply_signal", True))

        # Step 1: Kill switch check (INV-9)
        kill_check = await workflow.execute_activity(
            "check_state_unit_kill_switch_activity",
            {"state_unit_id": state_unit_id, "project_id": project_id},
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        if kill_check.get("frozen"):
            self.status = "BLOCKED"
            self.blocked_reason = "state_unit_frozen"
            return self.get_state()

        # Step 2: Decommission guard — refuse non-empty buckets; never force-delete (GCS-DEL-001)
        if action in {"delete", "decommission"}:
            bucket_name = params.get("bucket_name") or (approved_payload.get("spec") or {}).get("name", "")
            empty_check = await workflow.execute_activity(
                "check_bucket_empty_for_decommission_activity",
                {
                    "bucket_name": bucket_name,
                    "object_count": params.get("bucket_object_count", 0),
                },
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            self.history.append({"step": "decommission_empty_check", "result": empty_check})
            if not empty_check.get("allowed", False):
                self.status = "BLOCKED"
                self.blocked_reason = empty_check.get("blocked_reason", "bucket_not_empty")
                return self.get_state()

        # Step 3: Rollback preparation if action == 'rollback'
        render_params = dict(params)
        if action == "rollback":
            rb_res = await workflow.execute_activity(
                "prepare_rollback_revision_activity",
                {
                    "resource_id": params["resource_id"],
                    "target_approval_id": params.get("target_approval_id"),
                },
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            self.history.append({"step": "prepare_rollback", "result": rb_res})
            render_params = {
                "state_unit_id": rb_res["state_unit_id"],
                "approval_id": rb_res["new_approval_id"],
            }

        # Step 4: Render create-only Expected revision to gs://microdrive-dev-spp-tfstate (ifGenerationMatch=0)
        rendered = await workflow.execute_activity(
            "render_expected_revision_activity",
            render_params,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        self.revision_id = rendered["revision_id"]
        self.revision_no = rendered["revision_no"]
        state_unit_id = rendered["state_unit_id"]
        approval_id = rendered["manifest"]["trigger_approval_id"]
        self.history.append({"step": "render_expected", "revision_id": self.revision_id, "revision_no": self.revision_no})

        # Step 5: Execute pre_apply lifecycle tasks
        pre_res = await workflow.execute_activity(
            "run_lifecycle_tasks_activity",
            {
                "approval_id": approval_id,
                "phase": "pre_apply",
                "force_fail_task": params.get("force_fail_pre_apply_task"),
            },
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        self.history.append({"step": "pre_apply", "result": pre_res})
        if not pre_res.get("succeeded", True):
            self.status = "BLOCKED"
            self.blocked_reason = pre_res.get("blocked_reason", "prerequisite_failed")
            return self.get_state()

        # Step 6: Run tf-plan-<20hex> K8s Job in namespace spp-platform + Conftest policy check
        allow_destroy = bool(params.get("allow_destroy", False)) or (
            action in {"delete", "decommission"} and bool(params.get("human_approved_delete", False))
        )
        plan_res = await workflow.execute_activity(
            "run_tf_plan_job_activity",
            {
                "revision_id": self.revision_id,
                "run_key": str(params.get("run_key", "1")),
                "phase": "destroy_plan" if action in {"delete", "decommission"} and params.get("use_destroy_plan") else "plan",
                "allow_destroy": allow_destroy,
                "mock_plan_json": params.get("mock_plan_json"),
                "state_serial_before": params.get("state_serial_before", 0),
            },
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        self.plan_job_name = plan_res.get("k8s_job_name")
        self.plan_summary = plan_res.get("plan_summary")
        self.history.append({"step": "tf_plan", "result": {k: v for k, v in plan_res.items() if k != "k8s_job_manifest"}})

        if not plan_res.get("succeeded", False):
            self.status = "BLOCKED"
            self.blocked_reason = plan_res.get("blocked_reason", "plan_failed")
            return self.get_state()

        # Step 7: Human approval signal before apply
        if require_apply_signal:
            self.status = "AWAITING_APPLY_APPROVAL"
            self.awaiting_apply_approval = True
            await workflow.wait_condition(lambda: self._apply_approval_signal is not None)
            self.awaiting_apply_approval = False
            sig = self._apply_approval_signal or {}
            self._apply_approval_signal = None
            decision = str(sig.get("decision", sig.get("status", "approved"))).lower()
            if decision in {"deny", "denied", "reject", "rejected"}:
                self.status = "DENIED"
                self.blocked_reason = sig.get("comment") or "apply_denied_by_human"
                return self.get_state()
            self.status = "APPLYING"

        # Step 8: Run tf-apply-<20hex> K8s Job in namespace spp-platform and record Observed projection
        apply_res = await workflow.execute_activity(
            "run_tf_apply_job_activity",
            {
                "revision_id": self.revision_id,
                "run_key": str(params.get("run_key", "1")),
                "state_serial_before": params.get("state_serial_before", 0),
                "state_serial_after": params.get("state_serial_after", 3),
                "observation_id": params.get("observation_id"),
            },
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        self.apply_job_name = apply_res.get("k8s_job_name")
        self.history.append({"step": "tf_apply", "result": {k: v for k, v in apply_res.items() if k != "k8s_job_manifest"}})

        if not apply_res.get("succeeded", False):
            self.status = "BLOCKED"
            self.blocked_reason = apply_res.get("blocked_reason", "apply_failed")
            return self.get_state()

        # Step 9: Execute post_apply lifecycle tasks
        post_res = await workflow.execute_activity(
            "run_lifecycle_tasks_activity",
            {
                "approval_id": approval_id,
                "phase": "post_apply",
            },
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        self.history.append({"step": "post_apply", "result": post_res})
        self.status = "OBSERVED"
        return self.get_state()


@workflow.defn(name="ApprovalGateWorkflow")
class ApprovalGateWorkflow:
    """Sequential human approval workflow apr-<request_id> enforcing self_approval='deny' and human approval on delete (ADR-BP-003)."""

    def __init__(self) -> None:
        self.status: str = "PENDING"
        self.decisions: list[dict[str, Any]] = []
        self._responses: list[dict[str, Any]] = []

    @workflow.signal
    def approval_response(self, payload: dict[str, Any]) -> None:
        self._responses.append(payload)

    @workflow.query
    def get_state(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "decisions": self.decisions,
        }

    @workflow.run
    async def run(self, params: dict[str, Any]) -> dict[str, Any]:
        requested_by = str(params.get("requested_by", "")).lower()
        action = str(params.get("action", "create")).lower()
        approvers = params.get("approvers") or [{"key": "bucket_owner", "identity": params.get("owner", "bucket.owner@example.com")}]
        max_wait_days = int(params.get("max_wait_days", 13))

        for approver in approvers:
            key = approver["key"]
            skip_cond = approver.get("skip_condition")
            if skip_cond and params.get("environment", "nonprod") != "prod" and str(params.get("app_tier", "2")) != "1":
                self.decisions.append({"approver_key": key, "status": "skipped"})
                continue

            while True:
                got_response = await workflow.wait_condition(
                    lambda: len(self._responses) > 0,
                    timeout=timedelta(days=max_wait_days),
                )
                if not got_response:
                    self.status = "EXPIRED"
                    self.decisions.append({"approver_key": key, "status": "expired"})
                    return self.get_state()

                resp = self._responses.pop(0)
                responder = str(resp.get("responded_by", "")).lower()
                resp_status = str(resp.get("status", "")).lower()

                # Enforce self_approval = "deny" (ADR-BP-003 REQ-2, GR-1)
                if responder and responder == requested_by:
                    self.decisions.append(
                        {
                            "approver_key": key,
                            "status": "rejected_self_approval",
                            "responded_by": responder,
                            "reason": "self_approval = deny: requester may not approve their own request",
                        }
                    )
                    continue

                # Enforce no AUTO_APPROVED on delete (ADR-BP-003 REQ-5, GR-2)
                if action == "delete" and resp_status == "auto_approved":
                    self.status = "DENIED"
                    self.decisions.append(
                        {
                            "approver_key": key,
                            "status": "denied",
                            "reason": "auto_approved is forbidden for delete requests",
                        }
                    )
                    return self.get_state()

                if resp_status == "denied":
                    self.status = "DENIED"
                    self.decisions.append(
                        {
                            "approver_key": key,
                            "status": "denied",
                            "responded_by": responder,
                            "comment": resp.get("comment"),
                        }
                    )
                    return self.get_state()

                if resp_status == "approved":
                    self.decisions.append(
                        {
                            "approver_key": key,
                            "status": "approved",
                            "responded_by": responder,
                            "comment": resp.get("comment"),
                        }
                    )
                    break

        self.status = "APPROVED"
        return self.get_state()


@workflow.defn(name="RequestWorkflow")
class RequestWorkflow:
    """Drives REQUESTED -> APPROVED via ApprovalGateWorkflow (req-<request_id>, ADR-AUT-001)."""

    def __init__(self) -> None:
        self.status: str = "PENDING_APPROVAL"
        self._cancelled: bool = False

    @workflow.signal
    def cancel(self) -> None:
        self._cancelled = True

    @workflow.run
    async def run(self, params: dict[str, Any]) -> dict[str, Any]:
        if self._cancelled:
            self.status = "CANCELLED"
            return {"status": self.status}

        request_id = params.get("request_id", "default")
        gate_res = await workflow.execute_child_workflow(
            ApprovalGateWorkflow.run,
            params,
            id=f"apr-{request_id}",
        )
        self.status = gate_res["status"]
        return {"request_id": request_id, "status": self.status, "decisions": gate_res["decisions"]}


@workflow.defn(name="StateUnitReconcilerWorkflow")
class StateUnitReconcilerWorkflow:
    """Serializes all state-file operations for a single GCP project (su-gcp-<project_id>, INV-8, ADR-AUT-001)."""

    def __init__(self) -> None:
        self.project_id: str = ""
        self.state_unit_id: str = ""
        self.frozen: bool = False
        self._approval_queue: list[dict[str, Any]] = []
        self._drift_queue: list[dict[str, Any]] = []
        self._stop: bool = False
        self.processed_revisions: list[dict[str, Any]] = []
        self.processed_drift_scans: list[dict[str, Any]] = []

    @workflow.signal
    def approval_ready(self, payload: dict[str, Any]) -> None:
        self._approval_queue.append(payload)

    @workflow.signal
    def drift_scan(self, payload: dict[str, Any]) -> None:
        self._drift_queue.append(payload)

    @workflow.signal
    def freeze(self) -> None:
        self.frozen = True

    @workflow.signal
    def unfreeze(self) -> None:
        self.frozen = False

    @workflow.signal
    def stop_reconciler(self) -> None:
        self._stop = True

    @workflow.query
    def get_state(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "state_unit_id": self.state_unit_id,
            "frozen": self.frozen,
            "queued_approvals": len(self._approval_queue),
            "queued_drift_scans": len(self._drift_queue),
            "processed_revisions": self.processed_revisions,
            "processed_drift_scans": self.processed_drift_scans,
        }

    @workflow.run
    async def run(self, params: dict[str, Any]) -> dict[str, Any]:
        self.project_id = params.get("project_id", "microdrive-dev")
        self.state_unit_id = params.get("state_unit_id", "")
        initial_approvals = params.get("initial_approvals") or []
        for item in initial_approvals:
            self._approval_queue.append(item)

        auto_exit_when_drained = bool(params.get("exit_when_drained", True))

        while True:
            if self._stop:
                break

            if not self._approval_queue and not self._drift_queue:
                if auto_exit_when_drained:
                    break
                await workflow.wait_condition(
                    lambda: bool(self._approval_queue or self._drift_queue or self._stop)
                )
                continue

            if self._drift_queue:
                scan_req = self._drift_queue.pop(0)
                drift_res = await workflow.execute_activity(
                    "run_drift_scan_activity",
                    {
                        "project_id": self.project_id,
                        "state_unit_id": self.state_unit_id or scan_req.get("state_unit_id"),
                        "window": scan_req.get("window", "2026-10-23T06"),
                        "revision_id": scan_req.get("revision_id"),
                        "revision_no": scan_req.get("revision_no"),
                        "observed_overrides": scan_req.get("observed_overrides"),
                    },
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                self.processed_drift_scans.append(
                    {
                        "window": drift_res["window"],
                        "k8s_job_name": drift_res["k8s_job_name"],
                        "drift_detected": drift_res["drift_detected"],
                        "finding_count": len(drift_res["drift_findings"]),
                    }
                )
                continue

            if self._approval_queue:
                if self.frozen:
                    await workflow.wait_condition(lambda: not self.frozen or self._stop)
                    if self._stop:
                        break

                item = self._approval_queue.pop(0)
                kill_check = await workflow.execute_activity(
                    "check_state_unit_kill_switch_activity",
                    {
                        "state_unit_id": self.state_unit_id or item.get("state_unit_id"),
                        "project_id": self.project_id,
                    },
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                if kill_check.get("frozen"):
                    self.processed_revisions.append(
                        {
                            "approval_id": item.get("approval_id"),
                            "status": "BLOCKED",
                            "blocked_reason": "state_unit_frozen",
                        }
                    )
                    continue

                rendered = await workflow.execute_activity(
                    "render_expected_revision_activity",
                    item,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                rev_id = rendered["revision_id"]
                self.state_unit_id = rendered["state_unit_id"]

                plan_res = await workflow.execute_activity(
                    "run_tf_plan_job_activity",
                    {
                        "revision_id": rev_id,
                        "run_key": str(item.get("run_key", "1")),
                        "allow_destroy": bool(item.get("allow_destroy", False)),
                        "mock_plan_json": item.get("mock_plan_json"),
                    },
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                if not plan_res.get("succeeded", False):
                    self.processed_revisions.append(
                        {
                            "revision_id": rev_id,
                            "revision_no": rendered["revision_no"],
                            "status": "BLOCKED",
                            "blocked_reason": plan_res.get("blocked_reason"),
                            "plan_job_name": plan_res.get("k8s_job_name"),
                        }
                    )
                    continue

                apply_res = await workflow.execute_activity(
                    "run_tf_apply_job_activity",
                    {
                        "revision_id": rev_id,
                        "run_key": str(item.get("run_key", "1")),
                    },
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                self.processed_revisions.append(
                    {
                        "revision_id": rev_id,
                        "revision_no": rendered["revision_no"],
                        "status": apply_res.get("status", "APPLIED"),
                        "plan_job_name": plan_res.get("k8s_job_name"),
                        "apply_job_name": apply_res.get("k8s_job_name"),
                    }
                )

        return self.get_state()
