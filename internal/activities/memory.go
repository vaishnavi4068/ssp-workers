package activities

import (
	"fmt"

	"github.com/vaishnavi4068/ssp-workers/internal/models"
	"github.com/vaishnavi4068/ssp-workers/internal/store"
)

func checkStateUnitKillSwitchMemory(params Params) (Params, error) {
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

func checkBucketEmptyMemory(params Params) (Params, error) {
	count, _ := params["object_count"].(float64)
	if int(count) > 0 {
		return Params{"allowed": false, "blocked_reason": "bucket_not_empty"}, nil
	}
	return Params{"allowed": true}, nil
}

func prepareRollbackMemory(params Params) (Params, error) {
	return Params{
		"state_unit_id":   params["state_unit_id"],
		"new_approval_id": "rollback-" + fmt.Sprint(params["resource_id"]),
	}, nil
}

func renderExpectedMemory(params Params) (Params, error) {
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

func runLifecycleTasksMemory(params Params) (Params, error) {
	if typ, _ := params["force_fail_task"].(string); typ != "" {
		return Params{"succeeded": false, "blocked_reason": "prerequisite_failed"}, nil
	}
	return Params{"succeeded": true}, nil
}

func runTFPlanMemory(params Params) (Params, error) {
	revID := str(params, "revision_id", "rev-1")
	op := revID + ":" + str(params, "run_key", "1")
	return Params{
		"succeeded":    true,
		"k8s_job_name": models.K8sJobName(op, "plan"),
		"plan_summary": map[string]any{"add": 3, "change": 0, "destroy": 0, "replace": 0},
	}, nil
}

func runTFApplyMemory(params Params) (Params, error) {
	revID := str(params, "revision_id", "rev-1")
	op := revID + ":" + str(params, "run_key", "1")
	return Params{
		"succeeded":    true,
		"k8s_job_name": models.K8sJobName(op, "apply"),
		"status":       "APPLIED",
	}, nil
}

func str(m Params, k, def string) string {
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return def
}

func runDriftScanMemory(params Params) (Params, error) {
	window := str(params, "window", "scan")
	op := str(params, "project_id", "proj") + ":" + window
	return Params{
		"window":         window,
		"k8s_job_name":   models.K8sJobName(op, "refresh"),
		"drift_detected": false,
		"drift_findings": []any{},
	}, nil
}
