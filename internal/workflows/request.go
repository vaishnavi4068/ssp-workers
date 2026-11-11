package workflows

import (
	"go.temporal.io/sdk/workflow"
)

// RequestWorkflow drives req-<request_id> through ApprovalGateWorkflow.
type RequestWorkflow struct {
	status    string
	cancelled bool
}

func (w *RequestWorkflow) Cancel(_ workflow.Context) error {
	w.cancelled = true
	return nil
}

// RequestWorkflowRun is the registered workflow function.
func RequestWorkflowRun(ctx workflow.Context, params map[string]any) (map[string]any, error) {
	w := &RequestWorkflow{}
	if w.cancelled {
		w.status = "CANCELLED"
		return map[string]any{"status": w.status}, nil
	}
	requestID := strParam(params, "request_id", "default")
	childOpts := workflow.ChildWorkflowOptions{WorkflowID: "apr-" + requestID}
	ctx = workflow.WithChildOptions(ctx, childOpts)
	var gate map[string]any
	if err := workflow.ExecuteChildWorkflow(ctx, "ApprovalGateWorkflow", params).Get(ctx, &gate); err != nil {
		return nil, err
	}
	w.status = strParam(gate, "status", "PENDING")
	return map[string]any{
		"request_id": requestID,
		"status":     w.status,
		"decisions":  gate["decisions"],
	}, nil
}
