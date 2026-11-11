package workflows

import (
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

// ApprovalGateWorkflow — apr-<request_id> sequential human approval (skeleton; full port in progress).
type ApprovalGateWorkflow struct {
	status    string
	decisions []map[string]any
	responses []map[string]any
}

func (w *ApprovalGateWorkflow) ApprovalResponse(_ workflow.Context, payload map[string]any) error {
	w.responses = append(w.responses, payload)
	return nil
}

func (w *ApprovalGateWorkflow) GetStateQuery() (map[string]any, error) {
	return map[string]any{"status": w.status, "decisions": w.decisions}, nil
}

// ApprovalGateWorkflowRun is the registered workflow function.
func ApprovalGateWorkflowRun(ctx workflow.Context, params map[string]any) (map[string]any, error) {
	w := &ApprovalGateWorkflow{}
	w.status = "PENDING"
	requestedBy := strings.ToLower(strParam(params, "requested_by", ""))
	action := strings.ToLower(strParam(params, "action", "create"))
	approvers, _ := params["approvers"].([]any)
	if len(approvers) == 0 {
		approvers = []any{map[string]any{"key": "bucket_owner", "identity": strParam(params, "owner", "bucket.owner@example.com")}}
	}
	maxWait := time.Duration(intParam(params, "max_wait_days", 13)) * 24 * time.Hour

	for _, raw := range approvers {
		approver, _ := raw.(map[string]any)
		key := strParam(approver, "key", "")
		for {
			sigCh := workflow.GetSignalChannel(ctx, "ApprovalResponse")
			var payload map[string]any
			got := false
			sel := workflow.NewSelector(ctx)
			timer := workflow.NewTimer(ctx, maxWait)
			sel.AddReceive(sigCh, func(c workflow.ReceiveChannel, _ bool) {
				c.Receive(ctx, &payload)
				got = true
			})
			sel.AddFuture(timer, func(_ workflow.Future) { got = false })
			sel.Select(ctx)
			if !got {
				w.status = "EXPIRED"
				return map[string]any{"status": w.status, "decisions": w.decisions}, nil
			}
			resp := payload
			respStatus := strings.ToLower(strParam(resp, "status", ""))
			responder := strings.ToLower(strParam(resp, "responded_by", ""))
			if responder == requestedBy && respStatus == "approved" {
				w.status = "DENIED"
				w.decisions = append(w.decisions, map[string]any{
					"approver_key": key, "status": "rejected_self_approval",
					"responded_by": responder,
					"reason":       "self_approval = deny",
				})
				break
			}
			if action == "delete" && respStatus == "auto_approved" {
				w.status = "DENIED"
				return map[string]any{"status": w.status, "decisions": w.decisions}, nil
			}
			if respStatus == "denied" {
				w.status = "DENIED"
				return map[string]any{"status": w.status, "decisions": w.decisions}, nil
			}
			if respStatus == "approved" {
				w.decisions = append(w.decisions, map[string]any{"approver_key": key, "status": "approved", "responded_by": responder})
				break
			}
		}
	}
	w.status = "APPROVED"
	return map[string]any{"status": w.status, "decisions": w.decisions}, nil
}

func intParam(m map[string]any, key string, def int) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	if v, ok := m[key].(int); ok {
		return v
	}
	return def
}
