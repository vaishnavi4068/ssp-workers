package activities

import (
	"context"
	"fmt"

	"github.com/vaishnavi4068/ssp-workers/internal/config"
	"github.com/vaishnavi4068/ssp-workers/internal/models"
	"github.com/vaishnavi4068/ssp-workers/internal/store"
)

type Params = map[string]any

// CheckStateUnitKillSwitchActivity — INV-9.
func CheckStateUnitKillSwitchActivity(_ context.Context, params Params) (Params, error) {
	suID, _ := params["state_unit_id"].(string)
	projectID, _ := params["project_id"].(string)
	su, ok := store.Default.GetStateUnit(suID, projectID)
	if !ok {
		return Params{"frozen": false, "status": "active"}, nil
	}
	return Params{
		"state_unit_id": su.ID,
		"project_id":    su.ProjectID,
		"status":        su.Status,
		"frozen":        su.Status == "frozen",
	}, nil
}

func CheckBucketEmptyForDecommissionActivity(_ context.Context, params Params) (Params, error) {
	count, _ := params["object_count"].(float64)
	if int(count) > 0 {
		return Params{"allowed": false, "blocked_reason": "bucket_not_empty"}, nil
	}
	return Params{"allowed": true}, nil
}

func PrepareRollbackRevisionActivity(_ context.Context, params Params) (Params, error) {
	return Params{
		"state_unit_id":     params["state_unit_id"],
		"new_approval_id":   "rollback-" + fmt.Sprint(params["resource_id"]),
		"restored_revision": true,
	}, nil
}

func RenderExpectedRevisionActivity(_ context.Context, params Params) (Params, error) {
	if config.UseRealIntegrations() {
		return nil, fmt.Errorf("render_expected_revision: real GCS/DB not implemented in Go worker yet")
	}
	revNo := store.Default.NextRevision()
	revID := fmt.Sprintf("rev-%d", revNo)
	approvalID := str(params, "approval_id", "approval-local")
	return Params{
		"revision_id":   revID,
		"revision_no":   revNo,
		"state_unit_id": str(params, "state_unit_id", "su-local"),
		"manifest": map[string]any{
			"trigger_approval_id": approvalID,
		},
	}, nil
}

func RunLifecycleTasksActivity(_ context.Context, params Params) (Params, error) {
	if typ, _ := params["force_fail_task"].(string); typ != "" {
		return Params{
			"succeeded": false, "blocked_reason": "prerequisite_failed",
			"tasks": []any{map[string]any{"task_type": typ, "status": "FAILED"}},
		}, nil
	}
	return Params{"succeeded": true, "blocked_reason": nil, "tasks": []any{}}, nil
}

func RunTFPlanJobActivity(_ context.Context, params Params) (Params, error) {
	if config.UseRealIntegrations() {
		return nil, fmt.Errorf("run_tf_plan_job: real K8s runner not implemented in Go worker yet")
	}
	revID := str(params, "revision_id", "rev-1")
	op := revID + ":" + str(params, "run_key", "1")
	job := models.K8sJobName(op, "plan")
	return Params{
		"succeeded": true,
		"k8s_job_name": job,
		"plan_summary": map[string]any{"add": 3, "change": 0, "destroy": 0, "replace": 0},
	}, nil
}

func RunTFApplyJobActivity(_ context.Context, params Params) (Params, error) {
	if config.UseRealIntegrations() {
		return nil, fmt.Errorf("run_tf_apply_job: real K8s runner not implemented in Go worker yet")
	}
	revID := str(params, "revision_id", "rev-1")
	op := revID + ":" + str(params, "run_key", "1")
	job := models.K8sJobName(op, "apply")
	return Params{
		"succeeded": true,
		"k8s_job_name": job,
		"status":       "APPLIED",
	}, nil
}

func RunDriftScanActivity(_ context.Context, params Params) (Params, error) {
	window := str(params, "window", "scan")
	op := str(params, "project_id", "proj") + ":" + window
	return Params{
		"window":          window,
		"k8s_job_name":    models.K8sJobName(op, "refresh"),
		"drift_detected":  false,
		"drift_findings":  []any{},
	}, nil
}

func str(m Params, k, def string) string {
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return def
}
