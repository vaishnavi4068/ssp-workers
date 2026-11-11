"""Container entrypoint executed inside the deterministic Kubernetes Job tf-<phase>-<20hex> in namespace spp-platform (ADR-AUT-001, WI-002).

Execution flow inside the runner Job (`spp-tf-runner` Workload Identity -> `spp-deploy@microdrive-dev.iam.gserviceaccount.com`):
1. Prepare an isolated working directory copying `/app/modules/gcs-bucket`.
2. Download the Expected `terraform.tfvars.json` from `gs://microdrive-dev-spp-tfstate/projects/<project_id>/expected/rev-NNNNNN/terraform.tfvars.json`.
3. Read `projects/<project_id>/default.tfstate` from GCS (if present) to capture `state_serial_before` and `state_lineage`.
4. Run `terraform init -backend-config=bucket=microdrive-dev-spp-tfstate -backend-config=prefix=projects/<project_id>`.
5. For `plan` / `refresh_plan` / `destroy_plan`:
   - Run `terraform plan -out=planfile -detailed-exitcode` (`-refresh-only` for `refresh_plan`, `-destroy` for `destroy_plan`).
   - Run `terraform show -json planfile > plan.json`.
   - Upload `plan.json` (and binary `planfile.tfplan` for `plan`/`destroy_plan`) to GCS.
   - Run `conftest test` against `plan.json` with `/app/modules/gcs-bucket/policies`.
6. For `apply`:
   - Download the exact saved `planfile.tfplan` from GCS (`INV-5`).
   - Run `terraform apply -input=false -auto-approve planfile`.
   - Run `terraform show -json > show.json` and read updated `default.tfstate` (`state_serial_after`, `state_generation`).
7. Always upload combined runner execution logs to `gs://microdrive-dev-spp-tfstate/projects/<project_id>/logs/<k8s_job_name>.log`.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from .db import RealGcsObjectStore, parse_gs_uri
from .k8s_runner import evaluate_plan_with_conftest, summarize_terraform_plan
from .models import build_artifact_uri, build_state_uri, format_revision_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("spp-tf-runner")


def _read_tfstate_serial_and_lineage(
    gcs: RealGcsObjectStore,
    state_uri: str,
) -> tuple[int, str, int]:
    """Read (serial, lineage, generation) from gs://<state_bucket>/projects/<project_id>/default.tfstate if it exists."""
    try:
        meta = gcs.get_blob_metadata(state_uri)
        if not meta:
            return 0, "", 0
        state_doc = gcs.get_json(state_uri)
        serial = int(state_doc.get("serial") or 0)
        lineage = str(state_doc.get("lineage") or "")
        generation = int(meta.get("generation") or 0)
        return serial, lineage, generation
    except Exception:
        return 0, "", 0


def _download_bytes_from_gcs(gcs: RealGcsObjectStore, uri: str) -> bytes:
    bucket_name, blob_name = parse_gs_uri(uri)
    bucket = gcs.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"GCS object not found: {uri}")
    return blob.download_as_bytes()


def run_terraform_job() -> int:
    operation_id = os.getenv("SPP_OPERATION_ID", "")
    phase = os.getenv("SPP_PHASE", "plan")
    project_id = os.getenv("SPP_PROJECT_ID", "microdrive-dev")
    revision_no = int(os.getenv("SPP_REVISION_NO", "1"))
    state_bucket = os.getenv("SPP_STATE_BUCKET", "microdrive-dev-spp-tfstate")
    artifact_uri = os.getenv("SPP_ARTIFACT_URI") or build_artifact_uri(project_id, revision_no, state_bucket)
    rev_dir = format_revision_dir(revision_no)
    planfile_uri = (
        os.getenv("SPP_PLANFILE_URI")
        or f"gs://{state_bucket}/projects/{project_id}/expected/{rev_dir}/planfile.tfplan"
    )
    plan_uri = os.getenv("SPP_PLAN_URI", "")
    logs_uri = os.getenv("SPP_LOGS_URI", "")
    allow_destroy = os.getenv("SPP_ALLOW_DESTROY", "false").lower() == "true" or (phase == "destroy_plan")
    tf_source_dir = Path(os.getenv("SPP_TF_MODULE_DIR", "/app/modules/project-root"))
    policy_dir = Path(os.getenv("SPP_POLICY_DIR", str(tf_source_dir / "policies")))
    dry_run = os.getenv("SPP_RUNNER_DRY_RUN", "false").lower() == "true"

    logger.info(
        "Starting Terraform runner Job: operation_id=%s phase=%s project=%s rev=%d bucket=%s dry_run=%s",
        operation_id,
        phase,
        project_id,
        revision_no,
        state_bucket,
        dry_run,
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

    gcs = RealGcsObjectStore(project_id=os.getenv("GOOGLE_CLOUD_PROJECT", "microdrive-dev"))
    state_uri = build_state_uri(project_id, state_bucket)
    log_lines: list[str] = [
        f"[spp-tf-runner] operation_id={operation_id} phase={phase} project_id={project_id} revision_no={revision_no}"
    ]

    serial_before, lineage_before, _ = _read_tfstate_serial_and_lineage(gcs, state_uri)
    log_lines.append(f"[spp-tf-runner] state_serial_before={serial_before} state_lineage={lineage_before}")

    with tempfile.TemporaryDirectory(prefix="spp-tf-run-") as tmp_root:
        work_dir = Path(tmp_root) / "gcs-bucket"
        shutil.copytree(tf_source_dir, work_dir, dirs_exist_ok=True)

        # 1. Download Expected terraform.tfvars.json from GCS
        try:
            tfvars_text = gcs.get_text(artifact_uri)
            (work_dir / "terraform.tfvars.json").write_text(tfvars_text, encoding="utf-8")
            log_lines.append(f"[spp-tf-runner] Downloaded {artifact_uri} ({len(tfvars_text)} bytes)")
        except Exception as exc:
            msg = f"Failed to download Expected tfvars from {artifact_uri}: {exc}"
            logger.error(msg)
            log_lines.append(msg)
            _flush_logs(gcs, logs_uri, log_lines)
            return 1

        # 2. Run terraform init against gs://microdrive-dev-spp-tfstate with prefix=projects/<project_id>
        backend_prefix = f"projects/{project_id}"
        init_cmd = [
            "terraform",
            f"-chdir={work_dir}",
            "init",
            "-input=false",
            "-no-color",
            f"-backend-config=bucket={state_bucket}",
            f"-backend-config=prefix={backend_prefix}",
        ]
        init_proc = subprocess.run(init_cmd, capture_output=True, text=True, check=False)
        log_lines.append(f"$ {' '.join(init_cmd)}\n{init_proc.stdout}\n{init_proc.stderr}")
        if init_proc.returncode != 0:
            logger.error("terraform init failed: %s", init_proc.stderr)
            _flush_logs(gcs, logs_uri, log_lines)
            result = {
                "operation_id": operation_id,
                "phase": phase,
                "tf_exit_code": 1,
                "state_serial_before": serial_before,
                "state_serial_after": serial_before,
                "state_lineage": lineage_before,
                "logs_uri": logs_uri,
                "error": {"stage": "init", "stderr": init_proc.stderr[-2000:]},
            }
            print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
            return 1

        # 3. Execute phase: plan / refresh_plan / destroy_plan
        if phase in {"plan", "refresh_plan", "destroy_plan"}:
            plan_args = [
                "terraform",
                f"-chdir={work_dir}",
                "plan",
                "-input=false",
                "-no-color",
                "-detailed-exitcode",
                "-out=planfile",
            ]
            if phase == "refresh_plan":
                plan_args.append("-refresh-only")
            elif phase == "destroy_plan":
                plan_args.append("-destroy")

            plan_proc = subprocess.run(plan_args, capture_output=True, text=True, check=False)
            tf_exit = plan_proc.returncode
            log_lines.append(f"$ {' '.join(plan_args)} (exit={tf_exit})\n{plan_proc.stdout}\n{plan_proc.stderr}")

            if tf_exit not in (0, 2):
                logger.error("terraform plan failed with exit code %d: %s", tf_exit, plan_proc.stderr)
                _flush_logs(gcs, logs_uri, log_lines)
                result = {
                    "operation_id": operation_id,
                    "phase": phase,
                    "tf_exit_code": 1,
                    "state_serial_before": serial_before,
                    "state_serial_after": serial_before,
                    "state_lineage": lineage_before,
                    "logs_uri": logs_uri,
                    "error": {"stage": "plan", "stderr": plan_proc.stderr[-2000:]},
                }
                print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
                return 1

            show_cmd = ["terraform", f"-chdir={work_dir}", "show", "-json", "planfile"]
            show_proc = subprocess.run(show_cmd, capture_output=True, text=True, check=False)
            if show_proc.returncode != 0:
                logger.error("terraform show -json failed: %s", show_proc.stderr)
                log_lines.append(f"terraform show -json failed: {show_proc.stderr}")
                _flush_logs(gcs, logs_uri, log_lines)
                return 1

            plan_doc: dict[str, Any] = json.loads(show_proc.stdout)
            if plan_uri:
                gcs.put_object(plan_uri, plan_doc, content_type="application/json")
                log_lines.append(f"[spp-tf-runner] Uploaded plan JSON to {plan_uri}")

            if phase in {"plan", "destroy_plan"} and planfile_uri:
                planfile_bytes = (work_dir / "planfile").read_bytes()
                gcs.put_object(planfile_uri, planfile_bytes, content_type="application/octet-stream")
                log_lines.append(f"[spp-tf-runner] Uploaded binary planfile to {planfile_uri}")

            summary = summarize_terraform_plan(plan_doc)
            conftest_res = evaluate_plan_with_conftest(
                plan_doc,
                policy_dir=policy_dir,
                allow_destroy=allow_destroy,
            )
            log_lines.append(f"[spp-tf-runner] Conftest result: {json.dumps(conftest_res)}")
            _flush_logs(gcs, logs_uri, log_lines)

            result = {
                "operation_id": operation_id,
                "phase": phase,
                "project_id": project_id,
                "revision_no": revision_no,
                "tf_exit_code": tf_exit,
                "state_serial_before": serial_before,
                "state_serial_after": serial_before,
                "state_lineage": lineage_before,
                "plan_summary": summary,
                "conftest": conftest_res,
                "plan_uri": plan_uri,
                "planfile_uri": planfile_uri,
                "logs_uri": logs_uri,
                "mode": "real",
            }
            print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
            return 0 if conftest_res["decision"] == "allow" else 1

        # 4. Execute phase: apply
        if phase == "apply":
            planfile_path = work_dir / "planfile"
            try:
                planfile_bytes = _download_bytes_from_gcs(gcs, planfile_uri)
                planfile_path.write_bytes(planfile_bytes)
                log_lines.append(f"[spp-tf-runner] Downloaded saved planfile from {planfile_uri} ({len(planfile_bytes)} bytes)")
                apply_cmd = [
                    "terraform",
                    f"-chdir={work_dir}",
                    "apply",
                    "-input=false",
                    "-no-color",
                    "-auto-approve",
                    "planfile",
                ]
            except Exception as exc:
                log_lines.append(f"[spp-tf-runner] Saved planfile not found ({exc}); running apply with terraform.tfvars.json")
                apply_cmd = [
                    "terraform",
                    f"-chdir={work_dir}",
                    "apply",
                    "-input=false",
                    "-no-color",
                    "-auto-approve",
                ]

            apply_proc = subprocess.run(apply_cmd, capture_output=True, text=True, check=False)
            tf_exit = 0 if apply_proc.returncode == 0 else 1
            log_lines.append(f"$ {' '.join(apply_cmd)} (exit={apply_proc.returncode})\n{apply_proc.stdout}\n{apply_proc.stderr}")

            serial_after, lineage_after, state_gen = _read_tfstate_serial_and_lineage(gcs, state_uri)
            log_lines.append(
                f"[spp-tf-runner] state_serial_after={serial_after} state_generation={state_gen} state_lineage={lineage_after}"
            )
            _flush_logs(gcs, logs_uri, log_lines)

            result = {
                "operation_id": operation_id,
                "phase": phase,
                "project_id": project_id,
                "revision_no": revision_no,
                "tf_exit_code": tf_exit,
                "state_serial_before": serial_before,
                "state_serial_after": serial_after,
                "state_generation": state_gen,
                "state_lineage": lineage_after or lineage_before,
                "logs_uri": logs_uri,
                "mode": "real",
            }
            if tf_exit != 0:
                result["error"] = {"stage": "apply", "stderr": apply_proc.stderr[-2000:]}
            print(f"SPP_TF_RESULT_JSON={json.dumps(result, sort_keys=True)}")
            return 0 if tf_exit == 0 else 1

    logger.error("Unsupported phase: %s", phase)
    return 1


def _flush_logs(gcs: RealGcsObjectStore, logs_uri: str, log_lines: list[str]) -> None:
    if not logs_uri:
        return
    try:
        gcs.put_object(logs_uri, "\n".join(log_lines) + "\n", content_type="text/plain; charset=utf-8")
    except Exception as exc:
        logger.warning("Failed to upload runner logs to %s: %s", logs_uri, exc)


if __name__ == "__main__":
    sys.exit(run_terraform_job())
