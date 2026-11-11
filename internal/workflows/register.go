package workflows

import (
	"go.temporal.io/sdk/workflow"
	temporalworker "go.temporal.io/sdk/worker"
)

// Register attaches all workflows with Python-compatible type names.
func Register(w temporalworker.Worker) {
	w.RegisterWorkflowWithOptions(GcsBucketProvisionWorkflow, workflow.RegisterOptions{Name: GcsBucketProvisionWorkflowName})
	w.RegisterWorkflowWithOptions(ApprovalGateWorkflowRun, workflow.RegisterOptions{Name: "ApprovalGateWorkflow"})
	w.RegisterWorkflowWithOptions(RequestWorkflowRun, workflow.RegisterOptions{Name: "RequestWorkflow"})
	w.RegisterWorkflowWithOptions(StateUnitReconcilerWorkflowRun, workflow.RegisterOptions{Name: "StateUnitReconcilerWorkflow"})
}
