# Python → Go workers migration

Branch `cursor/go-workers-b7b7` ports **Temporal workflows and activities** from `workers/*.py` to Go while keeping Terraform modules and the runner image unchanged.

## Phase 1 (this branch)

- `cmd/provisioning-worker` — polls `TEMPORAL_TASK_QUEUE` (default `spp-gcs-provisioning`)
- Workflow names match Python: `GcsBucketProvisionWorkflow`, `ApprovalGateWorkflow`, `RequestWorkflow`, `StateUnitReconcilerWorkflow`
- Activity names match Python `*_activity` strings
- **Memory mode** (default): activities succeed without K8s/GCS/AlloyDB — for unit/replay tests
- **Real mode**: set `SSP_WORKER_USE_K8s=true` (etc.) — activities return "not implemented yet" until Phase 2

## Run

```bash
go test ./...
go run ./cmd/provisioning-worker   # requires Temporal at TEMPORAL_ADDRESS
```

Python worker remains available until Phase 2 parity tests pass and Helm switches the image entrypoint.

## Phase 2 (next)

- Port `workers/db.py`, `k8s_runner.py`, `drift.py` to `internal/tfrun`, `internal/k8s`, `internal/drift`
- Wire Postgres/GCS clients; delete `workers/` Python package from the runtime image
- Replay parity with `tests/test_ssp_workers.py` scenarios
