package activities

import (
	"context"
)

type Params = map[string]any

func CheckStateUnitKillSwitchActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "check_state_unit_kill_switch_activity", params)
}

func CheckBucketEmptyForDecommissionActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "check_bucket_empty_for_decommission_activity", params)
}

func PrepareRollbackRevisionActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "prepare_rollback_revision_activity", params)
}

func RenderExpectedRevisionActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "render_expected_revision_activity", params)
}

func RunLifecycleTasksActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "run_lifecycle_tasks_activity", params)
}

func RunTFPlanJobActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "run_tf_plan_job_activity", params)
}

func RunTFApplyJobActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "run_tf_apply_job_activity", params)
}

func RunDriftScanActivity(ctx context.Context, params Params) (Params, error) {
	ctx, cancel := withBridge(ctx)
	defer cancel()
	return dispatch(ctx, "run_drift_scan_activity", params)
}
