package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/vaishnavi4068/ssp-workers/internal/config"
)

func dispatch(ctx context.Context, name string, params Params) (Params, error) {
	if config.UsePythonActivityBridge() {
		return execPythonActivity(ctx, name, params)
	}
	return dispatchMemory(ctx, name, params)
}

func execPythonActivity(ctx context.Context, name string, params Params) (Params, error) {
	py := config.PythonBin()
	in, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, py, "-m", "workers.run_activity", name)
	cmd.Dir = config.WorkerAppRoot()
	cmd.Env = os.Environ()
	cmd.Stdin = bytes.NewReader(in)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("python activity %s: %w: %s", name, err, stderr.String())
	}
	var out Params
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("decode activity %s result: %w", name, err)
	}
	return out, nil
}

func dispatchMemory(_ context.Context, name string, params Params) (Params, error) {
	switch name {
	case "check_state_unit_kill_switch_activity":
		return checkStateUnitKillSwitchMemory(params)
	case "check_bucket_empty_for_decommission_activity":
		return checkBucketEmptyMemory(params)
	case "prepare_rollback_revision_activity":
		return prepareRollbackMemory(params)
	case "render_expected_revision_activity":
		return renderExpectedMemory(params)
	case "run_lifecycle_tasks_activity":
		return runLifecycleTasksMemory(params)
	case "run_tf_plan_job_activity":
		return runTFPlanMemory(params)
	case "run_tf_apply_job_activity":
		return runTFApplyMemory(params)
	case "run_drift_scan_activity":
		return runDriftScanMemory(params)
	default:
		return nil, fmt.Errorf("unknown activity %s", name)
	}
}

// Activities block below 2 minutes for bridge subprocess (runner jobs use activity heartbeats in Python).
const bridgeTimeout = 2 * time.Hour

func withBridge(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, bridgeTimeout)
}
