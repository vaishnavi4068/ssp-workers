"""Container entrypoint executed inside the deterministic Kubernetes Job tf-<phase>-<20hex> in namespace spp-platform (ADR-AUT-001, WI-002)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from .k8s_runner import evaluate_plan_with_conftest, summarize_terraform_plan

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("spp-tf-runner")


def run_terraform_job() -> int:
    operation_id = os.getenv("SPP_OPERATION_ID", "")
    phase = os.getenv("SPP_PHASE", "plan")
    project_id = os.getenv("SPP_PROJECT_ID", "microdrive-dev")
    revision_no = int(os.getenv("SPP_REVISION_NO", "1"))
    state_bucket = os.getenv("SPP_STATE_BUCKET", "microdrive-dev-spp-tfstate")
    plan_uri = os.getenv("SPP_PLAN_URI", "")
    logs_uri = os.getenv("SPP_LOGS_URI", "")
    tf_dir = os.getenv("SPP_TF_MODULE_DIR", "/app/modules/gcs-bucket")
    dry_run = os.getenv("SPP_RUNNER_DRY_RUN", "true").lower() == "true"

    logger.info(
        "Starting Terraform runner Job: operation_id=%s phase=%s project=%s rev=%d bucket=%s",
        operation_id,
        phase,
        project_id,
        revision_no,
        state_bucket,
    )

    if dry_run:
        result = {
            "operation_id": operation_id,
            "phase": phase,
            "project_id": project_id,
            "revision_no": revision_no,
            "tf_exit_code": 2 if phase in {"plan", "refresh_plan"} else 0,
            "plan_uri": plan_uri,
            "logs_uri": logs_uri,
            "mode": "dry",
        }
        print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
        return 0

    backend_prefix = f"projects/{project_id}"
    init_cmd = [
        "terraform",
        f"-chdir={tf_dir}",
        "init",
        "-input=false",
        f"-backend-config=bucket={state_bucket}",
        f"-backend-config=prefix={backend_prefix}",
    ]
    init_proc = subprocess.run(init_cmd, capture_output=True, text=True, check=False)
    if init_proc.returncode != 0:
        logger.error("terraform init failed: %s", init_proc.stderr)
        return 1

    if phase in {"plan", "refresh_plan", "destroy_plan"}:
        plan_args = [
            "terraform",
            f"-chdir={tf_dir}",
            "plan",
            "-input=false",
            "-detailed-exitcode",
            "-out=planfile",
        ]
        if phase == "refresh_plan":
            plan_args.append("-refresh-only")
        elif phase == "destroy_plan":
            plan_args.append("-destroy")

        plan_proc = subprocess.run(plan_args, capture_output=True, text=True, check=False)
        tf_exit = plan_proc.returncode
        if tf_exit not in (0, 2):
            logger.error("terraform plan failed with exit code %d: %s", tf_exit, plan_proc.stderr)
            return 1

        show_proc = subprocess.run(
            ["terraform", f"-chdir={tf_dir}", "show", "-json", "planfile"],
            capture_output=True,
            text=True,
            check=False,
        )
        if show_proc.returncode != 0:
            logger.error("terraform show -json failed: %s", show_proc.stderr)
            return 1

        plan_doc: dict[str, Any] = json.loads(show_proc.stdout)
        summary = summarize_terraform_plan(plan_doc)
        conftest_res = evaluate_plan_with_conftest(
            plan_doc,
            allow_destroy=(phase == "destroy_plan"),
        )
        result = {
            "operation_id": operation_id,
            "phase": phase,
            "project_id": project_id,
            "revision_no": revision_no,
            "tf_exit_code": tf_exit,
            "plan_summary": summary,
            "conftest": conftest_res,
            "plan_uri": plan_uri,
            "logs_uri": logs_uri,
        }
        print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
        return 0 if conftest_res["decision"] == "allow" else 1

    if phase == "apply":
        apply_proc = subprocess.run(
            ["terraform", f"-chdir={tf_dir}", "apply", "-input=false", "-auto-approve", "planfile"],
            capture_output=True,
            text=True,
            check=False,
        )
        tf_exit = apply_proc.returncode
        result = {
            "operation_id": operation_id,
            "phase": phase,
            "project_id": project_id,
            "revision_no": revision_no,
            "tf_exit_code": tf_exit,
            "logs_uri": logs_uri,
        }
        print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
        return 0 if tf_exit == 0 else 1

    logger.error("Unsupported phase: %s", phase)
    return 1


if __name__ == "__main__":
    sys.exit(run_terraform_job())
