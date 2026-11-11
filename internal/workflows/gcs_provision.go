package workflows

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/vaishnavi4068/ssp-workers/internal/activities"
)

const activityTimeout = 30 * time.Minute

// gcsProvisionState holds workflow mutable state (queries/signals use channels in GcsBucketProvisionWorkflow).
type gcsProvisionState struct {
	status                string
	blockedReason         *string
	awaitingApplyApproval bool
	revisionID            *string
	revisionNo            *int
	planJobName           *string
	applyJobName          *string
	planSummary           map[string]any
	applyApprovalSignal   map[string]any
	rollbackRequested     map[string]any
	history               []map[string]any
}

func (w *gcsProvisionState) stateMap() map[string]any {
	return map[string]any{
		"status":                  w.status,
		"blocked_reason":          w.blockedReason,
		"awaiting_apply_approval": w.awaitingApplyApproval,
		"revision_id":             w.revisionID,
		"revision_no":             w.revisionNo,
		"plan_job_name":           w.planJobName,
		"apply_job_name":          w.applyJobName,
		"plan_summary":            w.planSummary,
		"history":                 w.history,
	}
}

// GcsBucketProvisionWorkflow orchestrates GCS provisioning (parity with workers/workflows.py).
func GcsBucketProvisionWorkflow(ctx workflow.Context, params map[string]any) (map[string]any, error) {
	w := &gcsProvisionState{status: "RUNNING"}
	approved := approvedDoc(params)
	action := strParam(params, "action", strParam(approved, "action", "create"))
	projectID := strParam(params, "project_id", metaStr(approved, "project_id", "microdrive-dev"))
	stateUnitID := strParam(params, "state_unit_id", "")
	requireApply := boolParam(params, "require_apply_signal", true)

	ao := workflow.ActivityOptions{StartToCloseTimeout: activityTimeout}

	// Step 1: kill switch
	var kill activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.CheckStateUnitKillSwitchActivity, activities.Params{
		"state_unit_id": stateUnitID, "project_id": projectID,
	}).Get(ctx, &kill); err != nil {
		return nil, err
	}
	if kill["frozen"] == true {
		w.status = "BLOCKED"
		reason := "state_unit_frozen"
		w.blockedReason = &reason
		return w.stateMap(), nil
	}

	// Step 2: decommission guard
	if action == "delete" || action == "decommission" {
		bucket := strParam(params, "bucket_name", specStr(approved, "name", ""))
		var empty activities.Params
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.CheckBucketEmptyForDecommissionActivity, activities.Params{
			"bucket_name": bucket, "object_count": params["bucket_object_count"],
		}).Get(ctx, &empty); err != nil {
			return nil, err
		}
		w.history = append(w.history, map[string]any{"step": "decommission_empty_check", "result": empty})
		if empty["allowed"] != true {
			w.status = "BLOCKED"
			r := strParam(empty, "blocked_reason", "bucket_not_empty")
			w.blockedReason = &r
			return w.stateMap(), nil
		}
	}

	renderParams := cloneMap(params)
	if action == "rollback" {
		var rb activities.Params
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.PrepareRollbackRevisionActivity, activities.Params{
			"resource_id": params["resource_id"], "target_approval_id": params["target_approval_id"],
		}).Get(ctx, &rb); err != nil {
			return nil, err
		}
		w.history = append(w.history, map[string]any{"step": "prepare_rollback", "result": rb})
		renderParams = activities.Params{
			"state_unit_id": rb["state_unit_id"],
			"approval_id":   rb["new_approval_id"],
		}
	}

	var rendered activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RenderExpectedRevisionActivity, renderParams).Get(ctx, &rendered); err != nil {
		return nil, err
	}
	if rid, ok := rendered["revision_id"].(string); ok {
		w.revisionID = &rid
	}
	if rn, ok := rendered["revision_no"].(int); ok {
		w.revisionNo = &rn
	}
	approvalID := ""
	if manifest, ok := rendered["manifest"].(map[string]any); ok {
		approvalID, _ = manifest["trigger_approval_id"].(string)
	}
	w.history = append(w.history, map[string]any{"step": "render_expected", "revision_id": w.revisionID, "revision_no": w.revisionNo})

	var pre activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunLifecycleTasksActivity, activities.Params{
		"approval_id": approvalID, "phase": "pre_apply", "force_fail_task": params["force_fail_pre_apply_task"],
	}).Get(ctx, &pre); err != nil {
		return nil, err
	}
	w.history = append(w.history, map[string]any{"step": "pre_apply", "result": pre})
	if pre["succeeded"] == false {
		w.status = "BLOCKED"
		r := strParam(pre, "blocked_reason", "prerequisite_failed")
		w.blockedReason = &r
		return w.stateMap(), nil
	}

	allowDestroy := boolParam(params, "allow_destroy", false) ||
		((action == "delete" || action == "decommission") && boolParam(params, "human_approved_delete", false))
	phase := "plan"
	if (action == "delete" || action == "decommission") && boolParam(params, "use_destroy_plan", false) {
		phase = "destroy_plan"
	}
	var plan activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunTFPlanJobActivity, activities.Params{
		"revision_id": w.revisionID, "run_key": strParam(params, "run_key", "1"), "phase": phase,
		"allow_destroy": allowDestroy, "mock_plan_json": params["mock_plan_json"],
	}).Get(ctx, &plan); err != nil {
		return nil, err
	}
	if job, ok := plan["k8s_job_name"].(string); ok {
		w.planJobName = &job
	}
	if ps, ok := plan["plan_summary"].(map[string]any); ok {
		w.planSummary = ps
	}
	w.history = append(w.history, map[string]any{"step": "tf_plan", "result": plan})
	if plan["succeeded"] != true {
		w.status = "BLOCKED"
		r := strParam(plan, "blocked_reason", "plan_failed")
		w.blockedReason = &r
		return w.stateMap(), nil
	}

	if requireApply {
		w.status = "AWAITING_APPLY_APPROVAL"
		w.awaitingApplyApproval = true
		sig, err := waitApplyApprovalSignal(ctx)
		if err != nil {
			return nil, err
		}
		w.awaitingApplyApproval = false
		decision := strParam(sig, "decision", strParam(sig, "status", "approved"))
		if decision == "deny" || decision == "denied" || decision == "reject" || decision == "rejected" {
			w.status = "DENIED"
			r := strParam(sig, "comment", "apply_denied_by_human")
			w.blockedReason = &r
			return w.stateMap(), nil
		}
		w.status = "APPLYING"
	}

	var apply activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunTFApplyJobActivity, activities.Params{
		"revision_id": w.revisionID, "run_key": strParam(params, "run_key", "1"),
	}).Get(ctx, &apply); err != nil {
		return nil, err
	}
	if job, ok := apply["k8s_job_name"].(string); ok {
		w.applyJobName = &job
	}
	w.history = append(w.history, map[string]any{"step": "tf_apply", "result": apply})
	if apply["succeeded"] != true {
		w.status = "BLOCKED"
		r := strParam(apply, "blocked_reason", "apply_failed")
		w.blockedReason = &r
		return w.stateMap(), nil
	}

	var post activities.Params
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), activities.RunLifecycleTasksActivity, activities.Params{
		"approval_id": approvalID, "phase": "post_apply",
	}).Get(ctx, &post); err != nil {
		return nil, err
	}
	w.history = append(w.history, map[string]any{"step": "post_apply", "result": post})
	w.status = "OBSERVED"
	return w.stateMap(), nil
}

func approvedDoc(params map[string]any) map[string]any {
	if d, ok := params["approved_spec"].(map[string]any); ok {
		return d
	}
	if d, ok := params["approved_doc"].(map[string]any); ok {
		return d
	}
	return map[string]any{}
}

func metaStr(doc map[string]any, key, def string) string {
	meta, _ := doc["metadata"].(map[string]any)
	return strParam(meta, key, def)
}

func specStr(doc map[string]any, key, def string) string {
	spec, _ := doc["spec"].(map[string]any)
	return strParam(spec, key, def)
}

func strParam(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func boolParam(m map[string]any, key string, def bool) bool {
	if m == nil {
		return def
	}
	if v, ok := m[key].(bool); ok {
		return v
	}
	return def
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func waitApplyApprovalSignal(ctx workflow.Context) (map[string]any, error) {
	var sig map[string]any
	approveCh := workflow.GetSignalChannel(ctx, "ApproveApply")
	humanCh := workflow.GetSignalChannel(ctx, "HumanApproval")
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(approveCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, &sig) })
	sel.AddReceive(humanCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, &sig) })
	sel.Select(ctx)
	return sig, nil
}

// GcsBucketProvisionWorkflowName is the Temporal registration name (must match ssp-cp dispatch).
const GcsBucketProvisionWorkflowName = "GcsBucketProvisionWorkflow"
