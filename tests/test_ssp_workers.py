"""Comprehensive unit and Temporal replay/integration tests for WI-002, WI-004, and WI-005."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workers.activities import (
    check_bucket_empty_for_decommission_activity,
    check_state_unit_kill_switch_activity,
    prepare_rollback_revision_activity,
    render_expected_revision_activity,
    run_lifecycle_tasks_activity,
    run_tf_apply_job_activity,
    run_tf_plan_job_activity,
)
from workers.db import DEFAULT_STORE, GCSArtifactExistError, InMemoryStateStore
from workers.drift import (
    compare_expected_vs_observed,
    execute_drift_scan_for_project,
    run_drift_scan_activity,
)
from workers.k8s_runner import (
    build_tf_runner_job_manifest,
    evaluate_plan_with_conftest,
    launch_or_reattach_k8s_job,
)
from workers.main import ACTIVITIES, WORKFLOWS
from workers.models import (
    MAX_PROJECTION_BYTES,
    build_artifact_uri,
    build_gcs_bucket_observed_projection,
    build_logs_uri,
    build_operation_id,
    build_snapshot_uri,
    build_state_uri,
    canonical_json,
    canonical_sha256,
    convert_approved_gcs_bucket_to_expected_entry,
    k8s_job_name,
    strip_sensitive_values,
)
from workers.workflows import (
    ApprovalGateWorkflow,
    GcsBucketProvisionWorkflow,
    RequestWorkflow,
    StateUnitReconcilerWorkflow,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _sample_approved_gcs_doc(
    *,
    name: str = "spp-nonprod-orders-landing-a1b2",
    project_id: str = "spp-nonprod-orders-svc",
    storage_class: str = "STANDARD",
    retention_days: int = 90,
    lifecycle_rules: list[dict[str, Any]] | None = None,
    action: str = "create",
) -> dict[str, Any]:
    return {
        "api_version": "idp.spp/v1",
        "kind": "gcs_bucket",
        "action": action,
        "metadata": {
            "apm_id": "APM12345",
            "project_id": project_id,
            "environment": "nonprod",
            "lob": "spp",
            "data_sensitivity": "restricted",
            "data_classification": "proprietary",
            "owner": "bucket.owner@example.com",
            "shared_mailbox": "spp-orders-team@example.com",
            "requested_by": "requester@example.com",
            "cost_center": "000000",
            "tenancy": "dedicated",
            "labels": {
                "app": "orders",
                "costcenter": "000000",
                "dataclassification": "proprietary",
                "datasensitivity": "restricted",
                "environmenttype": "nonprod",
                "idp_resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                "owner": "bucket_owner_example_com",
                "primarylob": "spp",
                "provisionedby": "idp",
                "sharedemailaddress": "spp-orders-team_example_com",
                "sppappid": "apm12345",
                "tenancy": "dedicated",
            },
            "resolved": {
                "app_tier": "2",
                "environment_type": "nonprod",
                "vpc_sc_perimeter": "spp_nonprod_restricted_perimeter",
                "cost_center_validated": True,
                "resolved_at": "2026-10-21T14:02:13Z",
            },
        },
        "spec": {
            "name": name,
            "location": "us-east1",
            "storage_class": storage_class,
            "versioning": False,
            "lifecycle_rules": lifecycle_rules if lifecycle_rules is not None else [],
            "access": [
                {
                    "role": "roles/storage.objectAdmin",
                    "members": [
                        f"serviceAccount:orders-pipeline@{project_id}.iam.gserviceaccount.com",
                    ],
                },
                {
                    "role": "roles/storage.objectViewer",
                    "members": [
                        "group:gcp-spp-nonprod-orders-read@example.com",
                    ],
                },
            ],
            "purpose": "Landing zone for order events",
            "retention_days": retention_days,
            "retention_locked": False,
            "uniform_bucket_level_access": True,
            "public_access_prevention": "enforced",
            "kms_key_name": "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek",
            "soft_delete_retention_days": 7,
            "force_destroy": False,
        },
        "prerequisites": [
            {
                "type": "ad_group",
                "target": "group:gcp-spp-nonprod-orders-read@example.com",
                "rule_id": "PRQ-001",
            },
            {
                "type": "vpc_sc_ingress",
                "target": "group:gcp-spp-nonprod-orders-read@example.com",
                "params": {"perimeter": "spp_nonprod_restricted_perimeter"},
                "rule_id": "PRQ-002",
            },
        ],
        "post_apply": [
            {
                "type": "notify_owner",
                "target": "bucket.owner@example.com",
            }
        ],
        "approval": {
            "request_id": "22120aab-3834-51d3-b631-0f7d3c43f56e",
            "gate_key": "gcs_bucket_owner",
        },
    }


@pytest.fixture(autouse=True)
def reset_default_store():
    """Reset the in-memory store before each test."""
    DEFAULT_STORE.__init__()
    yield


# ---------------------------------------------------------------------------
# 1. Deterministic k8s_job_name & URI Formula Tests (WI-002 AC-1, WI-005 AC-1)
# ---------------------------------------------------------------------------


def test_k8s_job_name_matches_canonical_spec_test_vectors():
    plan_op = "op:af42f828-3378-5ee2-8e9b-2378d2314f97:r1:plan:1"
    apply_op = "op:af42f828-3378-5ee2-8e9b-2378d2314f97:r1:apply:1"
    refresh_op = "op:af42f828-3378-5ee2-8e9b-2378d2314f97:r2:refresh_plan:2026-10-23T06"

    assert k8s_job_name(plan_op, "plan") == "tf-plan-eba5f9ebf4fafecad8e9"
    assert k8s_job_name(apply_op, "apply") == "tf-apply-b85d57b4511c03e98373"
    assert k8s_job_name(refresh_op, "refresh_plan") == "tf-refresh-plan-415a15a935cbd63f95e1"


def test_gcs_state_bucket_uri_builders():
    project = "spp-nonprod-orders-svc"
    assert (
        build_artifact_uri(project, 1)
        == "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/expected/rev-000001/terraform.tfvars.json"
    )
    assert (
        build_state_uri(project)
        == "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/default.tfstate"
    )
    assert (
        build_logs_uri(project, "tf-plan-eba5f9ebf4fafecad8e9")
        == "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/logs/tf-plan-eba5f9ebf4fafecad8e9.log"
    )
    assert (
        build_snapshot_uri(project, "2026-10-23T06")
        == "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/observed/scan-2026-10-23T06/plan.json"
    )


# ---------------------------------------------------------------------------
# 2. Expected tfvars Conversions, APM ID 5-digit Rule, and Schema Validation
# ---------------------------------------------------------------------------


def test_expected_entry_conversions_and_5_digit_apm_id():
    doc = _sample_approved_gcs_doc(
        lifecycle_rules=[
            {
                "action": {"type": "Delete"},
                "condition": {"age_days": 180, "with_state": "ANY"},
            }
        ]
    )
    entry = convert_approved_gcs_bucket_to_expected_entry(doc)

    assert entry["project"] == "spp-nonprod-orders-svc"
    assert entry["name"] == "spp-nonprod-orders-landing-a1b2"
    assert entry["location"] == "us-east1"
    assert entry["uniform_bucket_level_access"] is True
    assert entry["public_access_prevention"] == "enforced"
    assert entry["autoclass"] is False
    assert entry["force_destroy"] is False
    # 90 days * 86400 = 7776000 seconds; retention_locked = false -> is_locked = false
    assert entry["retention_policy"] == {"retention_period": 7776000, "is_locked": False}
    # 7 days * 86400 = 604800 seconds
    assert entry["soft_delete_policy"] == {"retention_duration_seconds": 604800}
    # age_days -> age
    assert entry["lifecycle_rules"] == [
        {"action": {"type": "Delete"}, "condition": {"age": 180, "with_state": "ANY"}}
    ]
    assert entry["labels"]["sppappid"] == "apm12345"
    assert "environmentsubtype" not in entry["labels"]
    assert entry["iam_bindings"] == {
        "roles/storage.objectAdmin": [
            "serviceAccount:orders-pipeline@spp-nonprod-orders-svc.iam.gserviceaccount.com"
        ],
        "roles/storage.objectViewer": ["group:gcp-spp-nonprod-orders-read@example.com"],
    }


def test_expected_entry_rejects_7_digit_apm_id_or_environmentsubtype():
    bad_apm = _sample_approved_gcs_doc()
    bad_apm["metadata"]["labels"]["sppappid"] = "apm0017299"
    with pytest.raises(ValueError, match="sppappid"):
        convert_approved_gcs_bucket_to_expected_entry(bad_apm)

    bad_sub = _sample_approved_gcs_doc()
    bad_sub["metadata"]["labels"]["environmentsubtype"] = "dev"
    with pytest.raises(ValueError, match="environmentsubtype"):
        convert_approved_gcs_bucket_to_expected_entry(bad_sub)


def test_gcs_bucket_json_schema_validates_examples():
    schema_path = REPO_ROOT / "modules" / "gcs-bucket" / "schemas" / "gcs_bucket.schema.json"
    req_path = REPO_ROOT / "modules" / "gcs-bucket" / "examples" / "requested.json"
    apr_path = REPO_ROOT / "modules" / "gcs-bucket" / "examples" / "approved.json"

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    req_doc = json.loads(req_path.read_text(encoding="utf-8"))
    apr_doc = json.loads(apr_path.read_text(encoding="utf-8"))

    validator = Draft202012Validator(schema)
    validator.validate(req_doc)
    validator.validate(apr_doc)

    expected_entry = convert_approved_gcs_bucket_to_expected_entry(apr_doc)
    validator.validate(expected_entry)


# ---------------------------------------------------------------------------
# 3. K8s Runner Job Manifest, Workload Identity SA, & 409 AlreadyExists Reattachment
# ---------------------------------------------------------------------------


def test_tf_runner_job_manifest_uses_spp_tf_runner_and_spp_deploy_gsa():
    op_id = "op:af42f828-3378-5ee2-8e9b-2378d2314f97:r1:plan:1"
    manifest = build_tf_runner_job_manifest(
        operation_id=op_id,
        phase="plan",
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        revision_id="fa5d29a6-b406-5b66-81f2-8e45d24fb019",
        revision_no=1,
        project_id="spp-nonprod-orders-svc",
        executor_identity="idp-tf@spp-nonprod-orders-svc.iam.gserviceaccount.com",
    )

    assert manifest["metadata"]["name"] == "tf-plan-eba5f9ebf4fafecad8e9"
    assert manifest["metadata"]["namespace"] == "spp-platform"
    assert (
        manifest["metadata"]["annotations"]["spp.platform/runner-gsa"]
        == "spp-deploy@microdrive-dev.iam.gserviceaccount.com"
    )

    pod_spec = manifest["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "spp-tf-runner"
    assert pod_spec["nodeSelector"] == {"workload": "spp"}
    assert pod_spec["tolerations"] == [
        {"key": "dedicated", "operator": "Equal", "value": "spp", "effect": "NoSchedule"}
    ]
    assert pod_spec["securityContext"]["runAsNonRoot"] is True


@pytest.mark.asyncio
async def test_k8s_job_reattachment_on_409_already_exists():
    class FakeJobStatus:
        succeeded = 1
        failed = 0

    class FakeJobObj:
        status = FakeJobStatus()

    class FakeApiError(Exception):
        def __init__(self, status: int):
            super().__init__(f"HTTP {status}")
            self.status = status

    class FakeBatchV1Api:
        def __init__(self):
            self.create_calls = 0

        def read_namespaced_job(self, name: str, namespace: str):
            raise FakeApiError(404)

        def create_namespaced_job(self, namespace: str, body: dict[str, Any]):
            self.create_calls += 1
            raise FakeApiError(409)  # Simulate race / retry where Job already exists

        def read_namespaced_job_status(self, name: str, namespace: str):
            return FakeJobObj()

    op_id = "op:af42f828-3378-5ee2-8e9b-2378d2314f97:r1:plan:1"
    manifest = build_tf_runner_job_manifest(
        operation_id=op_id,
        phase="plan",
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        revision_id="fa5d29a6-b406-5b66-81f2-8e45d24fb019",
        revision_no=1,
        project_id="spp-nonprod-orders-svc",
        executor_identity="idp-tf@spp-nonprod-orders-svc.iam.gserviceaccount.com",
    )

    fake_batch = FakeBatchV1Api()
    res = await launch_or_reattach_k8s_job(manifest, k8s_batch_api=fake_batch, poll_seconds=0.01)
    assert res["reattached"] is True
    assert res["succeeded"] is True
    assert res["k8s_job_name"] == "tf-plan-eba5f9ebf4fafecad8e9"
    assert fake_batch.create_calls == 1


# ---------------------------------------------------------------------------
# 4. GCS ifGenerationMatch=0, INV-6 Hash Mismatch, & Upsert-on-Change Projection
# ---------------------------------------------------------------------------


def test_if_generation_match_0_rejects_overwriting_expected_revision():
    store = InMemoryStateStore()
    uri = "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/expected/rev-000001/terraform.tfvars.json"
    store.put_gcs_object(uri, {"gcs_buckets": {}}, if_generation_match=0)

    with pytest.raises(GCSArtifactExistError):
        store.put_gcs_object(uri, {"gcs_buckets": {"tampered": {}}}, if_generation_match=0)


def test_observed_projection_under_2kb_strips_sensitive_and_upserts_only_on_change():
    store = InMemoryStateStore()
    doc = _sample_approved_gcs_doc()
    entry = convert_approved_gcs_bucket_to_expected_entry(doc)
    entry["secret_value"] = "should-be-stripped"

    proj, sha = build_gcs_bucket_observed_projection(
        entry,
        sensitive_mask={"custom_secret": True},
        tf_address='module.gcs_bucket["spp-nonprod-orders-landing-a1b2"]',
    )
    assert "secret_value" not in proj
    assert len(canonical_json(proj).encode("utf-8")) < MAX_PROJECTION_BYTES

    res_id = "9478bcfc-c2cf-583b-8bd8-096476647840"
    wrote_1, _ = store.upsert_resource_observed(
        resource_id=res_id,
        observation_id="obs-1",
        exists_in_cloud=True,
        cloud_resource_id="projects/_/buckets/spp-nonprod-orders-landing-a1b2",
        projection=proj,
        projection_sha256=sha,
    )
    assert wrote_1 is True
    assert store.resource_observed_write_count[res_id] == 1

    # Re-observing unchanged bucket (e.g., during rev-000002) must NOT write to resource_observed
    wrote_2, row_2 = store.upsert_resource_observed(
        resource_id=res_id,
        observation_id="obs-2",
        exists_in_cloud=True,
        cloud_resource_id="projects/_/buckets/spp-nonprod-orders-landing-a1b2",
        projection=proj,
        projection_sha256=sha,
    )
    assert wrote_2 is False
    assert store.resource_observed_write_count[res_id] == 1
    assert row_2["changed_in_observation_id"] == "obs-1"


# ---------------------------------------------------------------------------
# 5. Temporal Workflow Replay & E2E Provision, Rollback, Decommission, Kill Switch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gcs_bucket_provision_workflow_e2e_with_human_approval_signal():
    doc = _sample_approved_gcs_doc()

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="idp-reconciler-v1",
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            handle = await env.client.start_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "approved_doc": doc,
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                    "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                    "request_id": "22120aab-3834-51d3-b631-0f7d3c43f56e",
                    "approval_id": "761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
                    "revision_id": "fa5d29a6-b406-5b66-81f2-8e45d24fb019",
                    "observation_id": "f75d522f-53ab-5026-8eca-7150ab0cb0cb",
                    "require_apply_signal": True,
                },
                id="gcs-provision-e2e-01",
                task_queue="idp-reconciler-v1",
            )

            for _ in range(40):
                st = await handle.query(GcsBucketProvisionWorkflow.get_state)
                if st["awaiting_apply_approval"]:
                    break
                await asyncio.sleep(0.05)

            st = await handle.query(GcsBucketProvisionWorkflow.get_state)
            assert st["status"] == "AWAITING_APPLY_APPROVAL"
            assert st["plan_job_name"] == "tf-plan-eba5f9ebf4fafecad8e9"
            assert st["plan_summary"] == {"add": 3, "change": 0, "destroy": 0, "replace": 0}

            # Send human approval signal to proceed with apply
            await handle.signal(
                GcsBucketProvisionWorkflow.approve_apply,
                {"decision": "approved", "responded_by": "bucket.owner@example.com"},
            )

            final = await handle.result()
            assert final["status"] == "OBSERVED"
            assert final["revision_no"] == 1
            assert final["plan_job_name"] == "tf-plan-eba5f9ebf4fafecad8e9"
            assert final["apply_job_name"] == "tf-apply-b85d57b4511c03e98373"

            ro = DEFAULT_STORE.resource_observed["9478bcfc-c2cf-583b-8bd8-096476647840"]
            assert ro["cloud_resource_id"] == "projects/_/buckets/spp-nonprod-orders-landing-a1b2"
            assert ro["exists_in_cloud"] is True


@pytest.mark.asyncio
async def test_inv6_approved_hash_mismatch_blocks_before_plan():
    reg = DEFAULT_STORE.register_approved_resource(
        approved_doc=_sample_approved_gcs_doc(),
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        resource_id="9478bcfc-c2cf-583b-8bd8-096476647840",
        approval_id="761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
    )
    rev = DEFAULT_STORE.compose_and_render_expected_revision(
        state_unit_id=reg["state_unit"]["state_unit_id"],
        trigger_approval_id=reg["approval"]["approval_id"],
    )
    # Simulate tampering between DB approval hash and manifest hash
    DEFAULT_STORE.approvals["761d8bf8-d23e-5da3-8e52-02e6eb6c2afb"]["approved_spec_sha256"] = "0" * 64

    res = await run_tf_plan_job_activity({"revision_id": rev["revision_id"]})
    assert res["succeeded"] is False
    assert res["status"] == "BLOCKED"
    assert res["blocked_reason"] == "approved_hash_mismatch"


@pytest.mark.asyncio
async def test_inv9_frozen_state_unit_blocks_workflow():
    DEFAULT_STORE.ensure_state_unit(
        project_id="spp-nonprod-orders-svc",
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        status="frozen",
    )

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="idp-reconciler-v1",
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            res = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "approved_doc": _sample_approved_gcs_doc(),
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                },
                id="gcs-frozen-test",
                task_queue="idp-reconciler-v1",
            )
            assert res["status"] == "BLOCKED"
            assert res["blocked_reason"] == "state_unit_frozen"


@pytest.mark.asyncio
async def test_rollback_restores_and_reapplies_last_approved_spec():
    doc_v1 = _sample_approved_gcs_doc(storage_class="STANDARD")
    doc_v2 = _sample_approved_gcs_doc(storage_class="NEARLINE", action="update")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="idp-reconciler-v1",
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            # 1. Apply rev-1 (STANDARD)
            r1 = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "approved_doc": doc_v1,
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                    "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                    "approval_id": "apr-v1",
                    "require_apply_signal": False,
                },
                id="gcs-rb-rev1",
                task_queue="idp-reconciler-v1",
            )
            assert r1["status"] == "OBSERVED"
            assert r1["revision_no"] == 1

            # 2. Apply rev-2 (NEARLINE) referencing base_approval_id = apr-v1
            r2 = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "approved_doc": doc_v2,
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                    "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                    "approval_id": "apr-v2",
                    "base_approval_id": "apr-v1",
                    "require_apply_signal": False,
                },
                id="gcs-rb-rev2",
                task_queue="idp-reconciler-v1",
            )
            assert r2["status"] == "OBSERVED"
            assert r2["revision_no"] == 2
            assert (
                DEFAULT_STORE.resource_observed["9478bcfc-c2cf-583b-8bd8-096476647840"]["projection"]["storage_class"]
                == "NEARLINE"
            )

            # 3. Trigger Rollback -> renders rev-3 restoring apr-v1 (STANDARD) and applies cleanly
            r3 = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "action": "rollback",
                    "project_id": "spp-nonprod-orders-svc",
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                    "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                    "require_apply_signal": False,
                },
                id="gcs-rb-rev3",
                task_queue="idp-reconciler-v1",
            )
            assert r3["status"] == "OBSERVED"
            assert r3["revision_no"] == 3
            assert (
                DEFAULT_STORE.resource_observed["9478bcfc-c2cf-583b-8bd8-096476647840"]["projection"]["storage_class"]
                == "STANDARD"
            )


@pytest.mark.asyncio
async def test_decommission_refuses_non_empty_bucket_and_blocks_unapproved_destroy():
    doc = _sample_approved_gcs_doc(action="delete")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="idp-reconciler-v1",
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            # Non-empty bucket (42 objects) -> refused with bucket_not_empty
            res_non_empty = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "action": "decommission",
                    "approved_doc": doc,
                    "bucket_name": "spp-nonprod-orders-landing-a1b2",
                    "bucket_object_count": 42,
                    "require_apply_signal": False,
                },
                id="gcs-decomm-non-empty",
                task_queue="idp-reconciler-v1",
            )
            assert res_non_empty["status"] == "BLOCKED"
            assert res_non_empty["blocked_reason"] == "bucket_not_empty"

            # Empty bucket but missing human_approved_delete -> blocked at plan gate with destroy_without_delete_request
            res_unapproved = await env.client.execute_workflow(
                GcsBucketProvisionWorkflow.run,
                {
                    "action": "delete",
                    "approved_doc": doc,
                    "bucket_name": "spp-nonprod-orders-landing-a1b2",
                    "bucket_object_count": 0,
                    "human_approved_delete": False,
                    "require_apply_signal": False,
                },
                id="gcs-decomm-unapproved",
                task_queue="idp-reconciler-v1",
            )
            assert res_unapproved["status"] == "BLOCKED"
            assert res_unapproved["blocked_reason"] == "destroy_without_delete_request"


@pytest.mark.asyncio
async def test_state_unit_reconciler_workflow_serializes_concurrent_signals():
    doc1 = _sample_approved_gcs_doc(name="spp-nonprod-orders-landing-a1b2")
    doc2 = _sample_approved_gcs_doc(name="spp-nonprod-orders-archive-c3d4")
    doc2["metadata"]["labels"]["idp_resource_id"] = "11111111-2222-3333-4444-555555555555"

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="idp-reconciler-v1",
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            res = await env.client.execute_workflow(
                StateUnitReconcilerWorkflow.run,
                {
                    "project_id": "spp-nonprod-orders-svc",
                    "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                    "exit_when_drained": True,
                    "initial_approvals": [
                        {
                            "approved_doc": doc1,
                            "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                            "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
                            "approval_id": "apr-1",
                        },
                        {
                            "approved_doc": doc2,
                            "state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
                            "resource_id": "11111111-2222-3333-4444-555555555555",
                            "approval_id": "apr-2",
                        },
                    ],
                },
                id="su-gcp-spp-nonprod-orders-svc",
                task_queue="idp-reconciler-v1",
            )
            assert len(res["processed_revisions"]) == 2
            assert [r["revision_no"] for r in res["processed_revisions"]] == [1, 2]
            assert all(r["status"] == "APPLIED" for r in res["processed_revisions"])
            # Bucket 1 was unchanged in rev-2, so its resource_observed row was written only ONCE
            assert DEFAULT_STORE.resource_observed_write_count["9478bcfc-c2cf-583b-8bd8-096476647840"] == 1


# ---------------------------------------------------------------------------
# 6. Drift Detection Tests (WI-005 AC-1..4)
# ---------------------------------------------------------------------------


def test_drift_detection_gcs_pap_inherited_critical_alert_only():
    # Seed applied rev-1 & rev-2 state matching specs/examples/gcs_bucket.gates.json
    doc = _sample_approved_gcs_doc()
    DEFAULT_STORE.register_approved_resource(
        approved_doc=doc,
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        resource_id="9478bcfc-c2cf-583b-8bd8-096476647840",
        approval_id="761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
    )
    DEFAULT_STORE.compose_and_render_expected_revision(
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        trigger_approval_id="761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
        revision_id="fa5d29a6-b406-5b66-81f2-8e45d24fb019",
    )
    rev2 = DEFAULT_STORE.compose_and_render_expected_revision(
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        trigger_approval_id="761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
        revision_id="77b88e4c-84fe-5050-a395-060f012d17f0",
    )
    DEFAULT_STORE.state_units["af42f828-3378-5ee2-8e9b-2378d2314f97"]["applied_expected_rev"] = 2

    # Out-of-band console change: public_access_prevention switched from "enforced" to "inherited"
    drift_res = execute_drift_scan_for_project(
        project_id="spp-nonprod-orders-svc",
        window="2026-10-23T06",
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        revision_id=rev2["revision_id"],
        revision_no=2,
        state_serial=6,
        observation_id="58f1ad89-2e29-5b67-97f9-67bde1276ac4",
        drift_id_override="bc02e690-5151-57ea-b6e1-115e926402c0",
        observed_overrides_by_resource={
            "spp-nonprod-orders-landing-a1b2": {
                "public_access_prevention": "inherited",
                "force_destroy": True,  # ignored via drift_ignore_paths = ['/force_destroy']
            }
        },
    )

    assert drift_res["k8s_job_name"] == "tf-refresh-plan-415a15a935cbd63f95e1"
    assert drift_res["tf_operation"]["state_serial_before"] == 6
    assert drift_res["tf_operation"]["state_serial_after"] == 6
    assert drift_res["observation"]["source"] == "refresh_plan"
    assert (
        drift_res["observation"]["snapshot_uri"]
        == "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/observed/scan-2026-10-23T06/plan.json"
    )
    assert drift_res["drift_detected"] is True
    assert drift_res["remediation_triggered"] is False
    # Verify zero tf-apply-* Jobs were launched
    assert not any(op["phase"] == "apply" for op in DEFAULT_STORE.tf_operations.values())

    assert len(drift_res["drift_findings"]) == 1
    finding = drift_res["drift_findings"][0]
    assert finding["drift_id"] == "bc02e690-5151-57ea-b6e1-115e926402c0"
    assert finding["drift_kind"] == "out_of_band_change"
    assert finding["attribute_path"] == "/public_access_prevention"
    assert finding["expected_value"] == "enforced"
    assert finding["observed_value"] == "inherited"
    assert finding["severity"] == "critical"
    assert finding["guardrail_rule_id"] == "GCS-PAP-001"
    assert finding["status"] == "OPEN"


def test_drift_comparer_spanner_and_ignore_paths():
    # Non-autoscaled Spanner instance processing_units changed 100 -> 300 => medium severity
    diffs = compare_expected_vs_observed(
        kind="spanner_instance",
        expected_entry={
            "name": "spp-nonprod-orders",
            "config": "regional-us-east1",
            "processing_units": 100,
            "autoscaling_config": None,
            "force_destroy": False,
        },
        observed_projection={
            "exists": True,
            "name": "spp-nonprod-orders",
            "config": "regional-us-east1",
            "processing_units": 300,
            "autoscaling_config": None,
            "force_destroy": True,
        },
        drift_ignore_paths=["/force_destroy"],
    )
    assert len(diffs) == 1
    assert diffs[0] == {
        "drift_kind": "out_of_band_change",
        "attribute_path": "/processing_units",
        "expected_value": 100,
        "observed_value": 300,
        "severity": "medium",
        "guardrail_rule_id": None,
    }

    # Autoscaled Spanner instance ignores /processing_units; spanner_database ignores /ddl and /deletion_protection
    auto_diffs = compare_expected_vs_observed(
        kind="spanner_instance",
        expected_entry={
            "name": "spp-nonprod-orders",
            "processing_units": 100,
            "autoscaling_config": {"min_processing_units": 100, "max_processing_units": 500},
            "force_destroy": False,
        },
        observed_projection={
            "exists": True,
            "name": "spp-nonprod-orders",
            "processing_units": 300,
            "autoscaling_config": {"min_processing_units": 100, "max_processing_units": 500},
        },
        drift_ignore_paths=["/force_destroy"],
    )
    assert auto_diffs == []

    db_diffs = compare_expected_vs_observed(
        kind="spanner_database",
        expected_entry={
            "name": "orders",
            "ddl": ["CREATE TABLE t1 (id INT64) PRIMARY KEY(id)"],
            "deletion_protection": True,
        },
        observed_projection={
            "exists": True,
            "name": "orders",
            "ddl": ["CREATE TABLE t1 (id INT64) PRIMARY KEY(id)", "CREATE TABLE t2 (id INT64) PRIMARY KEY(id)"],
            "deletion_protection": False,
        },
        drift_ignore_paths=["/ddl", "/deletion_protection"],
    )
    assert db_diffs == []


def test_helm_values_and_rbac_configure_spp_deploy_workload_identity():
    values_path = REPO_ROOT / "deploy" / "helm" / "spp-workers" / "values.yaml"
    values = yaml.safe_load(values_path.read_text(encoding="utf-8"))

    assert values["global"]["namespace"] == "spp-platform"
    assert values["global"]["gcpRegion"] == "us-east1"
    assert values["global"]["nodeSelector"] == {"workload": "spp"}
    assert values["serviceAccounts"]["runner"]["name"] == "spp-tf-runner"
    assert (
        values["serviceAccounts"]["runner"]["gsaEmail"]
        == "spp-deploy@microdrive-dev.iam.gserviceaccount.com"
    )
    assert (
        values["serviceAccounts"]["worker"]["gsaEmail"]
        == "spp-deploy@microdrive-dev.iam.gserviceaccount.com"
    )
    assert (
        values["terraform"]["defaultKmsKeyName"]
        == "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek"
    )
    assert values["alloydb"]["host"] == "10.80.140.2"
    assert values["alloydb"]["port"] == 5432
    assert values["alloydb"]["database"] == "spp_platform"
    assert values["alloydb"]["username"] == "postgres"
    assert values["alloydb"]["gcpSecretManagerPasswordSecret"] == "spp-alloydb-postgres-password"
    assert values["temporal"]["taskQueue"] == "spp-gcs-provisioning"
    assert values["images"]["runnerRepo"] == "spp-tf-runner"
    assert values["images"]["tag"] != "latest"


def test_worker_config_defaults_and_runner_image_tag_resolution(monkeypatch: pytest.MonkeyPatch):
    from workers.config import WorkerConfig, resolve_database_url, resolve_runner_image

    monkeypatch.delenv("TEMPORAL_TASK_QUEUE", raising=False)
    monkeypatch.delenv("SPP_RUNNER_IMAGE", raising=False)
    monkeypatch.setenv("SPP_RUNNER_IMAGE_TAG", "eb6c14b")

    cfg = WorkerConfig()
    assert cfg.task_queue == "spp-gcs-provisioning"
    assert cfg.runner_image == "us-east1-docker.pkg.dev/microdrive-dev/spp/spp-tf-runner:eb6c14b"
    assert not cfg.runner_image.endswith(":latest")

    # Verify AlloyDB URL resolution from SPP_ALLOYDB_* env vars
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SPP_ALLOYDB_HOST", "10.80.140.2")
    monkeypatch.setenv("SPP_ALLOYDB_PORT", "5432")
    monkeypatch.setenv("SPP_ALLOYDB_DB", "spp_platform")
    monkeypatch.setenv("SPP_ALLOYDB_USER", "postgres")
    monkeypatch.setenv("SPP_ALLOYDB_PASSWORD", "test-secret-pw")
    db_url = resolve_database_url()
    assert db_url == "postgresql://postgres:test-secret-pw@10.80.140.2:5432/spp_platform?sslmode=require"


def test_real_gcs_object_store_if_generation_match_zero():
    from workers.db import RealGcsObjectStore

    class FakeBlob:
        def __init__(self, name: str, store: dict[str, bytes]):
            self.name = name
            self._store = store
            self.generation = 1729599999000000
            self.size = 0
            self.updated = None

        def upload_from_string(
            self,
            data: bytes,
            content_type: str | None = None,
            if_generation_match: int | None = None,
        ) -> None:
            if if_generation_match == 0 and self.name in self._store:
                class PreconditionFailed(Exception):
                    pass

                raise PreconditionFailed("412 Precondition Failed: ifGenerationMatch=0")
            self._store[self.name] = data
            self.size = len(data)

        def exists(self) -> bool:
            return self.name in self._store

        def download_as_text(self, encoding: str = "utf-8") -> str:
            return self._store[self.name].decode(encoding)

    class FakeBucket:
        def __init__(self):
            self.blobs: dict[str, bytes] = {}

        def blob(self, name: str) -> FakeBlob:
            return FakeBlob(name, self.blobs)

    class FakeStorageClient:
        def __init__(self):
            self.buckets: dict[str, FakeBucket] = {}

        def bucket(self, name: str) -> FakeBucket:
            return self.buckets.setdefault(name, FakeBucket())

    gcs = RealGcsObjectStore(project_id="microdrive-dev", client=FakeStorageClient())
    uri = "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/expected/rev-000001/terraform.tfvars.json"
    gen, sha = gcs.put_object(uri, {"gcs_buckets": {"b1": {"location": "us-east1"}}}, if_generation_match=0)
    assert gen == 1729599999000000
    assert len(sha) == 64
    assert gcs.get_json(uri) == {"gcs_buckets": {"b1": {"location": "us-east1"}}}

    with pytest.raises(GCSArtifactExistError):
        gcs.put_object(uri, {"gcs_buckets": {}}, if_generation_match=0)


@pytest.mark.asyncio
async def test_workflow_accepts_approved_spec_and_executes_real_k8s_job_path():
    """Verify GcsBucketProvisionWorkflow accepts `approved_spec` (sent by ssp-cp) and run_tf_plan/apply_job_activity
    actually invokes launch_or_reattach_k8s_job when use_k8s=True, reading plan.json from GCS.
    """
    doc = _sample_approved_gcs_doc()
    reg = DEFAULT_STORE.register_approved_resource(
        approved_doc=doc,
        state_unit_id="af42f828-3378-5ee2-8e9b-2378d2314f97",
        resource_id="9478bcfc-c2cf-583b-8bd8-096476647840",
        request_id="22120aab-3834-51d3-b631-0f7d3c43f56e",
        approval_id="761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
    )
    rendered = await render_expected_revision_activity(
        {
            "approved_spec": doc,
            "state_unit_id": reg["state_unit"]["state_unit_id"],
            "resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
            "request_id": "22120aab-3834-51d3-b631-0f7d3c43f56e",
            "approval_id": "761d8bf8-d23e-5da3-8e52-02e6eb6c2afb",
            "revision_id": "fa5d29a6-b406-5b66-81f2-8e45d24fb019",
        }
    )
    rev_id = rendered["revision_id"]

    # Pre-stage plan.json in GCS as the runner K8s Job would upload it
    plan_uri = "gs://microdrive-dev-spp-tfstate/projects/spp-nonprod-orders-svc/expected/rev-000001/plan.json"
    plan_doc = {
        "format_version": "1.2",
        "terraform_version": "1.10.5",
        "resource_changes": [
            {
                "address": 'module.gcs_bucket["spp-nonprod-orders-landing-a1b2"].google_storage_bucket.this',
                "type": "google_storage_bucket",
                "change": {
                    "actions": ["create"],
                    "after": {
                        "name": "spp-nonprod-orders-landing-a1b2",
                        "location": "us-east1",
                        "uniform_bucket_level_access": True,
                        "public_access_prevention": "enforced",
                        "force_destroy": False,
                        "encryption": [
                            {
                                "default_kms_key_name": "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek",
                            }
                        ],
                        "labels": doc["metadata"]["labels"],
                    },
                },
            }
        ],
    }
    DEFAULT_STORE.put_gcs_object(plan_uri, plan_doc)

    class FakeJobStatus:
        succeeded = 1
        failed = 0

    class FakeJobObj:
        status = FakeJobStatus()

    class FakeApiError(Exception):
        def __init__(self, status: int):
            self.status = status

    class FakeBatchApi:
        def __init__(self):
            self.created_jobs: list[str] = []

        def read_namespaced_job(self, name: str, namespace: str):
            raise FakeApiError(404)

        def create_namespaced_job(self, namespace: str, body: dict[str, Any]):
            self.created_jobs.append(body["metadata"]["name"])

        def read_namespaced_job_status(self, name: str, namespace: str):
            return FakeJobObj()

    fake_batch = FakeBatchApi()

    plan_res = await run_tf_plan_job_activity(
        {
            "revision_id": rev_id,
            "use_k8s": True,
            "k8s_batch_api": fake_batch,
        }
    )
    assert plan_res["succeeded"] is True
    assert plan_res["k8s_job_name"] == "tf-plan-eba5f9ebf4fafecad8e9"
    assert "tf-plan-eba5f9ebf4fafecad8e9" in fake_batch.created_jobs

    apply_res = await run_tf_apply_job_activity(
        {
            "revision_id": rev_id,
            "use_k8s": True,
            "k8s_batch_api": fake_batch,
        }
    )
    assert apply_res["succeeded"] is True
    assert apply_res["k8s_job_name"] == "tf-apply-b85d57b4511c03e98373"
    assert "tf-apply-b85d57b4511c03e98373" in fake_batch.created_jobs


