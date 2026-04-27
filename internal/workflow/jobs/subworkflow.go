package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
)

// SubWorkflowJob embeds a Definition as a single node. The outer engine
// runs this job, which delegates to a (possibly different) Engine running
// the inner Definition.
//
// Why an explicit job type rather than special-casing in the engine:
// keeping sub-workflows as "just another job" preserves the rule that the
// orchestrator does not know about specific job types. This is the cleanest
// way to satisfy the brief's "Sub-workflows embeddable as a single job step."
type SubWorkflowJob struct {
	Engine *workflow.Engine
	Def    workflow.Definition
}

func (j *SubWorkflowJob) Execute(ctx context.Context, input any) (any, error) {
	if j.Engine == nil {
		return nil, errors.New("sub-workflow job: engine is nil")
	}
	res, err := j.Engine.Run(ctx, j.Def)
	if err != nil {
		return nil, fmt.Errorf("sub-workflow %s: %w", j.Def.ID, err)
	}
	if res.State != workflow.StateSucceeded {
		return res, fmt.Errorf("sub-workflow %s: %s", j.Def.ID, res.State)
	}
	return res, nil
}

// NewSubWorkflowFactory expects config["engine"] to be a *workflow.Engine
// and config["definition"] to be a workflow.Definition. We pass these
// through config rather than capturing them in a closure because a single
// factory may produce sub-workflows with different definitions.
func NewSubWorkflowFactory() workflow.JobFactory {
	return func(config map[string]any) (workflow.Job, error) {
		eng, ok := config["engine"].(*workflow.Engine)
		if !ok || eng == nil {
			return nil, errors.New("sub-workflow factory: engine missing in config")
		}
		def, ok := config["definition"].(workflow.Definition)
		if !ok {
			return nil, errors.New("sub-workflow factory: definition missing in config")
		}
		return &SubWorkflowJob{Engine: eng, Def: def}, nil
	}
}
