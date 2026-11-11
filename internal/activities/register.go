package activities

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// Registry can register activities (worker or test environment).
type Registry interface {
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
}

// Register wires activity names to match Python @activity.defn(name=...) strings.
func Register(w Registry) {
	opts := func(name string) activity.RegisterOptions {
		return activity.RegisterOptions{Name: name}
	}
	w.RegisterActivityWithOptions(CheckStateUnitKillSwitchActivity, opts("check_state_unit_kill_switch_activity"))
	w.RegisterActivityWithOptions(CheckBucketEmptyForDecommissionActivity, opts("check_bucket_empty_for_decommission_activity"))
	w.RegisterActivityWithOptions(PrepareRollbackRevisionActivity, opts("prepare_rollback_revision_activity"))
	w.RegisterActivityWithOptions(RenderExpectedRevisionActivity, opts("render_expected_revision_activity"))
	w.RegisterActivityWithOptions(RunLifecycleTasksActivity, opts("run_lifecycle_tasks_activity"))
	w.RegisterActivityWithOptions(RunTFPlanJobActivity, opts("run_tf_plan_job_activity"))
	w.RegisterActivityWithOptions(RunTFApplyJobActivity, opts("run_tf_apply_job_activity"))
	w.RegisterActivityWithOptions(RunDriftScanActivity, opts("run_drift_scan_activity"))
}

// RegisterWorker is an alias for the production worker entrypoint.
func RegisterWorker(w worker.Worker) { Register(w) }
