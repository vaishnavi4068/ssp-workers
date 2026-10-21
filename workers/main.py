"""Entrypoint for SPP Temporal Workers on namespace 'platform' in GKE cluster md-dev (WI-002, ADR-CLD-GCP-001, ADR-AUT-001)."""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import (
    check_bucket_empty_for_decommission_activity,
    check_state_unit_kill_switch_activity,
    prepare_rollback_revision_activity,
    render_expected_revision_activity,
    run_lifecycle_tasks_activity,
    run_tf_apply_job_activity,
    run_tf_plan_job_activity,
)
from .config import WorkerConfig
from .drift import run_drift_scan_activity
from .workflows import (
    ApprovalGateWorkflow,
    GcsBucketProvisionWorkflow,
    RequestWorkflow,
    StateUnitReconcilerWorkflow,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ssp-workers")

WORKFLOWS = [
    GcsBucketProvisionWorkflow,
    RequestWorkflow,
    ApprovalGateWorkflow,
    StateUnitReconcilerWorkflow,
]

ACTIVITIES = [
    check_state_unit_kill_switch_activity,
    check_bucket_empty_for_decommission_activity,
    run_lifecycle_tasks_activity,
    render_expected_revision_activity,
    run_tf_plan_job_activity,
    run_tf_apply_job_activity,
    prepare_rollback_revision_activity,
    run_drift_scan_activity,
]


async def run_worker(cfg: WorkerConfig | None = None) -> None:
    config = cfg or WorkerConfig()
    logger.info(
        "Connecting to Temporal at %s (namespace=%s, task_queue=%s, k8s_namespace=%s, runner_ksa=%s, runner_gsa=%s)...",
        config.temporal_address,
        config.temporal_namespace,
        config.task_queue,
        config.k8s_namespace,
        config.runner_ksa,
        config.runner_gsa,
    )
    client = await Client.connect(config.temporal_address, namespace=config.temporal_namespace)
    worker = Worker(
        client,
        task_queue=config.task_queue,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    )
    logger.info("SPP Temporal Worker started polling task queue '%s'.", config.task_queue)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
