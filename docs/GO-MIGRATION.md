# Python → Go workers migration

## Runtime (merged path)

| Component | Implementation |
|-----------|----------------|
| Temporal **workflows** | **Go** (`internal/workflows`, `cmd/provisioning-worker`) |
| **Activities** (dev/tests, `SSP_WORKER_*=false`) | **Go** in-memory (`internal/activities/memory.go`) |
| **Activities** (cluster, real K8s/GCS/DB) | **Python bridge** (`python -m workers.run_activity`) until native Go port |
| **Container CMD** | `/app/bin/provisioning-worker` (Go) |
| **Terraform runner Jobs** | Unchanged (`Dockerfile.runner`, `modules/`) |

Set `SSP_WORKER_PYTHON_ACTIVITIES=false` to force Go memory activities only (not for production).

## Commands

```bash
go test ./...
go run ./cmd/provisioning-worker
make test          # Python pytest (regression corpus)
```

## Phase 3 (next)

- Native Go `internal/store` (pgx + GCS + k8s) replacing the Python activity bridge
- Retire `python -m workers.main` and slim the image Python footprint to runner-only needs
