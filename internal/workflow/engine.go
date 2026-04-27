package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Engine builds and runs workflows. It does not know about specific job
// types; it consults the Registry.
type Engine struct {
	reg   *Registry
	bus   *EventBus
	clock func() time.Time
	sleep func(context.Context, time.Duration) error // injectable for tests
}

func NewEngine(reg *Registry, bus *EventBus) *Engine {
	if bus == nil {
		bus = NewEventBus()
	}
	return &Engine{
		reg:   reg,
		bus:   bus,
		clock: time.Now,
		sleep: ctxSleep,
	}
}

func ctxSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Result is what Run returns: per-node outcomes and the final workflow state.
type Result struct {
	WorkflowID string
	State      State
	Nodes      map[string]NodeResult
}

// nodeSlot is the per-node runtime record. It owns state machine
// transitions for that node and a done channel for dependents to wait on.
type nodeSlot struct {
	mu     sync.Mutex
	state  State
	result NodeResult
	done   chan struct{}
}

func newSlot() *nodeSlot {
	return &nodeSlot{state: StatePending, done: make(chan struct{})}
}

// transitionTo updates state if the transition is valid; returns the new
// state. On invalid transition, returns the current state and an error.
func (s *nodeSlot) transitionTo(to State) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := transition(s.state, to)
	if err != nil {
		return s.state, err
	}
	s.state = ns
	return ns, nil
}

func (s *nodeSlot) setResult(r NodeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result = r
}

func (s *nodeSlot) snapshot() (State, NodeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.result
}

// Run executes a workflow definition. It returns when all reachable nodes
// have terminated. A failed node does NOT halt the workflow: only its
// dependents are transitively skipped. Independent branches continue.
func (e *Engine) Run(ctx context.Context, def Definition) (Result, error) {
	jobs := make(map[string]Job, len(def.Nodes))
	for _, n := range def.Nodes {
		j, err := e.buildJob(n)
		if err != nil {
			return Result{}, fmt.Errorf("workflow %s: %w", def.ID, err)
		}
		jobs[n.ID] = j
	}

	slots := make(map[string]*nodeSlot, len(def.Nodes))
	for _, n := range def.Nodes {
		slots[n.ID] = newSlot()
	}

	e.bus.Publish(Event{Kind: EventWorkflowStarted, WorkflowID: def.ID, At: e.clock()})

	var wg sync.WaitGroup
	for _, n := range def.Nodes {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runNode(ctx, def.ID, n, jobs[n.ID], slots)
		}()
	}
	wg.Wait()

	res := Result{WorkflowID: def.ID, Nodes: make(map[string]NodeResult, len(def.Nodes))}
	overall := StateSucceeded
	for id, s := range slots {
		st, r := s.snapshot()
		res.Nodes[id] = r
		switch st {
		case StateFailed:
			if overall != StateCancelled {
				overall = StateFailed
			}
		case StateCancelled:
			overall = StateCancelled
		}
	}
	res.State = overall
	e.bus.Publish(Event{Kind: EventWorkflowEnded, WorkflowID: def.ID, State: overall, At: e.clock()})
	return res, nil
}

func (e *Engine) runNode(
	ctx context.Context,
	workflowID string,
	def NodeDef,
	job Job,
	slots map[string]*nodeSlot,
) {
	self := slots[def.ID]
	defer close(self.done)

	// Wait for dependencies and collect their results.
	depResults := make(map[string]NodeResult, len(def.DependsOn))
	for _, depID := range def.DependsOn {
		dep := slots[depID]
		select {
		case <-ctx.Done():
			e.markCancelled(self, def.ID, workflowID)
			return
		case <-dep.done:
		}
		depState, depRes := dep.snapshot()
		depResults[depID] = depRes
		// Transitive skip: if a dependency did not succeed, skip this node.
		// This is what gives us "a failed job must not halt unrelated branches":
		// only DEPENDENTS of the failure are skipped; siblings keep running.
		if depState != StateSucceeded {
			ns, _ := self.transitionTo(StateSkipped)
			self.setResult(NodeResult{NodeID: def.ID, State: ns})
			e.bus.Publish(Event{
				Kind: EventNodeSkipped, WorkflowID: workflowID,
				NodeID: def.ID, State: ns, At: e.clock(),
			})
			return
		}
	}

	// Conditional execution.
	if def.Condition != nil && !def.Condition(depResults) {
		ns, _ := self.transitionTo(StateSkipped)
		self.setResult(NodeResult{NodeID: def.ID, State: ns})
		e.bus.Publish(Event{
			Kind: EventNodeSkipped, WorkflowID: workflowID,
			NodeID: def.ID, State: ns, At: e.clock(),
		})
		return
	}

	if _, err := self.transitionTo(StateRunning); err != nil {
		e.markFailed(self, def.ID, workflowID, err)
		return
	}
	e.bus.Publish(Event{
		Kind: EventNodeStarted, WorkflowID: workflowID,
		NodeID: def.ID, State: StateRunning, At: e.clock(),
	})

	// Take a local copy of the retry policy so concurrent nodes never
	// mutate a shared *RetryPolicy.
	var policy RetryPolicy
	if def.Retry != nil {
		policy = *def.Retry
	} else {
		policy = RetryPolicy{MaxAttempts: 1}
	}
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}

	var (
		out      any
		execErr  error
		attempts int
	)
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		attempts = attempt
		if err := ctx.Err(); err != nil {
			e.markCancelled(self, def.ID, workflowID)
			return
		}
		out, execErr = job.Execute(ctx, def.Input)
		if execErr == nil {
			break
		}
		if attempt < policy.MaxAttempts {
			e.bus.Publish(Event{
				Kind: EventNodeRetry, WorkflowID: workflowID,
				NodeID: def.ID, Attempt: attempt, Err: execErr, At: e.clock(),
			})
			delay := time.Duration(0)
			if policy.Backoff != nil {
				delay = time.Duration(policy.Backoff(attempt)) * time.Second
			}
			if err := e.sleep(ctx, delay); err != nil {
				e.markCancelled(self, def.ID, workflowID)
				return
			}
		}
	}

	final := StateSucceeded
	if execErr != nil {
		final = StateFailed
	}
	ns, terr := self.transitionTo(final)
	if terr != nil {
		e.markFailed(self, def.ID, workflowID, terr)
		return
	}
	self.setResult(NodeResult{NodeID: def.ID, State: ns, Output: out, Err: execErr})
	e.bus.Publish(Event{
		Kind: EventNodeEnded, WorkflowID: workflowID,
		NodeID: def.ID, State: ns, Attempt: attempts, Err: execErr, At: e.clock(),
	})
}

func (e *Engine) markCancelled(self *nodeSlot, nodeID, workflowID string) {
	ns, _ := self.transitionTo(StateCancelled)
	self.setResult(NodeResult{NodeID: nodeID, State: ns, Err: context.Canceled})
	e.bus.Publish(Event{
		Kind: EventNodeEnded, WorkflowID: workflowID,
		NodeID: nodeID, State: ns, At: e.clock(),
	})
}

func (e *Engine) markFailed(self *nodeSlot, nodeID, workflowID string, err error) {
	// Defensive: force into Failed even if state machine wouldn't allow it.
	// Only called from defensive paths after a transition error.
	self.mu.Lock()
	self.state = StateFailed
	self.result = NodeResult{NodeID: nodeID, State: StateFailed, Err: err}
	self.mu.Unlock()
	e.bus.Publish(Event{
		Kind: EventNodeEnded, WorkflowID: workflowID,
		NodeID: nodeID, State: StateFailed, Err: err, At: e.clock(),
	})
}

func (e *Engine) buildJob(n NodeDef) (Job, error) {
	if n.JobType == "" {
		return nil, fmt.Errorf("node %q: job type is required", n.ID)
	}
	j, err := e.reg.build(n.JobType, n.Config)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", n.ID, err)
	}
	if j == nil {
		return nil, errors.New("registry returned nil job")
	}
	return j, nil
}
