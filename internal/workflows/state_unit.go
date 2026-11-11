package workflows

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/vaishnavi4068/ssp-workers/internal/activities"
)

// StateUnitReconcilerWorkflow serializes state-file ops per GCP project (su-gcp-<project_id>).
type StateUnitReconcilerWorkflow struct {
	projectID           string
	stateUnitID         string
	frozen              bool
	approvalQueue       []map[string]any
	driftQueue          []map[string]any
	stop                bool
	processedRevisions  []map[string]any
	processedDriftScans []map[string]any
}

func (w *StateUnitReconcilerWorkflow) ApprovalReady(_ workflow.Context, payload map[string]any) error {
	w.approvalQueue = append(w.approvalQueue, payload)
	return nil
}

func (w *StateUnitReconcilerWorkflow) DriftScan(_ workflow.Context, payload map[string]any) error {
	w.driftQueue = append(w.driftQueue, payload)
	return nil
}

func (w *StateUnitReconcilerWorkflow) Freeze(_ workflow.Context) error {
	w.frozen = true
	return nil
}

func (w *StateUnitReconcilerWorkflow) Unfreeze(_ workflow.Context) error {
	w.frozen = false
	return nil
}

func (w *StateUnitReconcilerWorkflow) StopReconciler(_ workflow.Context) error {
	w.stop = true
	return nil
}

func (w *StateUnitReconcilerWorkflow) GetStateQuery() (map[string]any, error) {
	return map[string]any{
		"project_id": w.projectID, "state_unit_id": w.stateUnitID, "frozen": w.frozen,
		"queued_approvals": len(w.approvalQueue), "queued_drift_scans": len(w.driftQueue),
		"processed_revisions": w.processedRevisions, "processed_drift_scans": w.processedDriftScans,
	}, nil
}

// StateUnitReconcilerWorkflowRun is the registered workflow function.
func StateUnitReconcilerWorkflowRun(ctx workflow.Context, params map[string]any) (map[string]any, error) {
	w := &StateUnitReconcilerWorkflow{}
	w.projectID = strParam(params, "project_id", "microdrive-dev")
	w.stateUnitID = strParam(params, "state_unit_id", "")
	if init, ok := params["initial_approvals"].([]any); ok {
		for _, it := range init {
			if m, ok := it.(map[string]any); ok {
				w.approvalQueue = append(w.approvalQueue, m)
			}
		}
	}
	autoExit := boolParam(params, "exit_when_drained", true)
	ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Minute}

	for {
		if w.stop {
			break
		}
		if len(w.approvalQueue) == 0 && len(w.driftQueue) == 0 {
			if autoExit {
				break
			}
			_ = workflow.Await(ctx, func() bool {
				return len(w.approvalQueue) > 0 || len(w.driftQueue) > 0 || w.stop
			})
			continue
		}
		if len(w.driftQueue) > 0 {
			scanReq := w.driftQueue[0]
			w.driftQueue = w.driftQueue[1:]
			var drift activities.Params
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunDriftScanActivity, activities.Params{
				"project_id": w.projectID, "state_unit_id": w.stateUnitID,
				"window": strParam(scanReq, "window", "2026-10-23T06"),
			}).Get(ctx, &drift); err != nil {
				return nil, err
			}
			w.processedDriftScans = append(w.processedDriftScans, map[string]any{
				"window": drift["window"], "k8s_job_name": drift["k8s_job_name"],
				"drift_detected": drift["drift_detected"],
			})
			continue
		}
		if len(w.approvalQueue) > 0 {
			if w.frozen {
				_ = workflow.Await(ctx, func() bool { return !w.frozen || w.stop })
				if w.stop {
					break
				}
			}
			item := w.approvalQueue[0]
			w.approvalQueue = w.approvalQueue[1:]
			var kill activities.Params
			_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.CheckStateUnitKillSwitchActivity, activities.Params{
				"state_unit_id": w.stateUnitID, "project_id": w.projectID,
			}).Get(ctx, &kill)
			if kill["frozen"] == true {
				w.processedRevisions = append(w.processedRevisions, map[string]any{
					"approval_id": item["approval_id"], "status": "BLOCKED", "blocked_reason": "state_unit_frozen",
				})
				continue
			}
			var rendered activities.Params
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RenderExpectedRevisionActivity, item).Get(ctx, &rendered); err != nil {
				return nil, err
			}
			revID, _ := rendered["revision_id"].(string)
			w.stateUnitID, _ = rendered["state_unit_id"].(string)
			var plan activities.Params
			_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunTFPlanJobActivity, activities.Params{
				"revision_id": revID, "run_key": strParam(item, "run_key", "1"),
			}).Get(ctx, &plan)
			if plan["succeeded"] != true {
				w.processedRevisions = append(w.processedRevisions, map[string]any{
					"revision_id": revID, "status": "BLOCKED", "blocked_reason": plan["blocked_reason"],
				})
				continue
			}
			var apply activities.Params
			_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunTFApplyJobActivity, activities.Params{
				"revision_id": revID, "run_key": strParam(item, "run_key", "1"),
			}).Get(ctx, &apply)
			w.processedRevisions = append(w.processedRevisions, map[string]any{
				"revision_id": revID, "status": apply["status"], "plan_job_name": plan["k8s_job_name"], "apply_job_name": apply["k8s_job_name"],
			})
		}
	}
	st, _ := w.GetStateQuery()
	return st, nil
}
