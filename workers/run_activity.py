#!/usr/bin/env python3
"""One-shot activity executor for the Go Temporal worker bridge (phase 2)."""

from __future__ import annotations

import asyncio
import json
import sys

from workers.activities import (
    check_bucket_empty_for_decommission_activity,
    check_state_unit_kill_switch_activity,
    prepare_rollback_revision_activity,
    render_expected_revision_activity,
    run_lifecycle_tasks_activity,
    run_tf_apply_job_activity,
    run_tf_plan_job_activity,
)
from workers.drift import run_drift_scan_activity

_ACTIVITIES = {
    "check_state_unit_kill_switch_activity": check_state_unit_kill_switch_activity,
    "check_bucket_empty_for_decommission_activity": check_bucket_empty_for_decommission_activity,
    "prepare_rollback_revision_activity": prepare_rollback_revision_activity,
    "render_expected_revision_activity": render_expected_revision_activity,
    "run_lifecycle_tasks_activity": run_lifecycle_tasks_activity,
    "run_tf_plan_job_activity": run_tf_plan_job_activity,
    "run_tf_apply_job_activity": run_tf_apply_job_activity,
    "run_drift_scan_activity": run_drift_scan_activity,
}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m workers.run_activity <activity_name>")
    name = sys.argv[1]
    fn = _ACTIVITIES.get(name)
    if fn is None:
        raise SystemExit(f"unknown activity: {name}")
    params = json.load(sys.stdin)
    result = asyncio.run(fn(params))
    json.dump(result, sys.stdout, separators=(",", ":"), default=str)


if __name__ == "__main__":
    main()
