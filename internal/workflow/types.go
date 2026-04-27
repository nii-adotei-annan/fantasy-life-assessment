// Package workflow implements a workflow orchestrator engine.
//
// Key design rule: the orchestrator does not know about specific job types.
// Job implementations are registered at runtime via a JobFactory, and the
// orchestrator only depends on the Job interface. Adding a new job type
// requires zero changes to this package.
package workflow

import (
	"context"
	"fmt"
)

// State is a node or workflow lifecycle state.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
	StateSkipped   State = "skipped" // node skipped because its condition was false
)

// validTransitions enumerates allowed state transitions. Anything not listed
// here is rejected by transition() — this is what makes the state machine
// "explicit" rather than implicit-via-code-paths.
var validTransitions = map[State]map[State]bool{
	StatePending: {
		StateRunning:   true,
		StateCancelled: true,
		StateSkipped:   true,
	},
	StateRunning: {
		StateSucceeded: true,
		StateFailed:    true,
		StateCancelled: true,
	},
	// Terminal states have no outgoing transitions.
	StateSucceeded: {},
	StateFailed:    {},
	StateCancelled: {},
	StateSkipped:   {},
}

// transition validates and returns the new state, or an error if the
// transition is not allowed.
func transition(from, to State) (State, error) {
	allowed, ok := validTransitions[from]
	if !ok {
		return from, fmt.Errorf("workflow: unknown state %q", from)
	}
	if !allowed[to] {
		return from, fmt.Errorf("workflow: invalid transition %s -> %s", from, to)
	}
	return to, nil
}

// Job is the unit of work. Implementations are registered via JobFactory.
type Job interface {
	// Execute runs the job. Input is whatever the workflow definition
	// passes (or nil). The returned output is made available to dependent
	// nodes' conditions.
	Execute(ctx context.Context, input any) (output any, err error)
}

// JobFunc adapts a function to Job. Useful for tests and small inline jobs.
type JobFunc func(ctx context.Context, input any) (any, error)

func (f JobFunc) Execute(ctx context.Context, input any) (any, error) { return f(ctx, input) }

// JobFactory constructs a Job from a config map. The orchestrator calls
// factories during workflow build; it does not call them during Execute.
type JobFactory func(config map[string]any) (Job, error)

// Predicate decides whether a node should run, given the outputs of its
// dependencies. Returning false transitions the node to Skipped.
type Predicate func(deps map[string]NodeResult) bool

// NodeResult is what dependent nodes see about an upstream node.
type NodeResult struct {
	NodeID string
	State  State
	Output any
	Err    error
}

// RetryPolicy governs per-job retries on Execute failure.
type RetryPolicy struct {
	MaxAttempts int                                  // 1 means "no retry" (one attempt only)
	Backoff     func(attempt int) (delaySeconds int) // attempt is 1-based
}
