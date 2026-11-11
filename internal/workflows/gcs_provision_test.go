package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/vaishnavi4068/ssp-workers/internal/activities"
	"github.com/vaishnavi4068/ssp-workers/internal/store"
	"github.com/vaishnavi4068/ssp-workers/internal/workflows"
)

func TestGcsProvisionFrozenStateUnit(t *testing.T) {
	store.Default = store.NewMemory()
	store.Default.EnsureStateUnit("af42f828-3378-5ee2-8e9b-2378d2314f97", "spp-nonprod-orders-svc", "frozen")

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.GcsBucketProvisionWorkflow, workflow.RegisterOptions{Name: workflows.GcsBucketProvisionWorkflowName})
	activities.Register(env)

	env.ExecuteWorkflow(workflows.GcsBucketProvisionWorkflowName, map[string]any{
		"approved_doc": map[string]any{
			"action": "create",
			"metadata": map[string]any{"project_id": "spp-nonprod-orders-svc"},
			"spec":     map[string]any{"name": "spp-nonprod-orders-landing-a1b2"},
		},
		"state_unit_id": "af42f828-3378-5ee2-8e9b-2378d2314f97",
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var out map[string]any
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, "BLOCKED", out["status"])
	require.Equal(t, "state_unit_frozen", out["blocked_reason"])
}

func TestGcsProvisionObservedWithoutApplySignal(t *testing.T) {
	store.Default = store.NewMemory()
	store.Default.EnsureStateUnit("su-1", "spp-nonprod-orders-svc", "active")

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.GcsBucketProvisionWorkflow, workflow.RegisterOptions{Name: workflows.GcsBucketProvisionWorkflowName})
	activities.Register(env)

	env.ExecuteWorkflow(workflows.GcsBucketProvisionWorkflowName, map[string]any{
		"approved_doc": map[string]any{
			"action": "create",
			"metadata": map[string]any{"project_id": "spp-nonprod-orders-svc"},
			"spec":     map[string]any{"name": "demo-bucket"},
		},
		"state_unit_id":        "su-1",
		"approval_id":          "apr-1",
		"require_apply_signal": false,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var out map[string]any
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, "OBSERVED", out["status"])
	require.NotNil(t, out["plan_job_name"])
	require.NotNil(t, out["apply_job_name"])
}
