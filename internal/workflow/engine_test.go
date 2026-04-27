package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeJob is a configurable Job for tests.
type fakeJob struct {
	name       string
	execN      atomic.Int32
	failCount  int32 // number of attempts to fail before succeeding
	failAlways bool
	output     any
	delay      time.Duration
}

func (f *fakeJob) Execute(ctx context.Context, input any) (any, error) {
	n := f.execN.Add(1)
	if f.delay > 0 {
		t := time.NewTimer(f.delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	if f.failAlways {
		return nil, errors.New("always fails")
	}
	if n <= f.failCount {
		return nil, errors.New("transient")
	}
	return f.output, nil
}

// jobRegistry helper to register a fakeJob by name.
func registerFake(t *testing.T, reg *Registry, name string, j *fakeJob) {
	t.Helper()
	if err := reg.Register(name, func(_ map[string]any) (Job, error) { return j, nil }); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

func TestEngine_LinearChain_Succeeds(t *testing.T) {
	reg := NewRegistry()
	a := &fakeJob{name: "a", output: "A"}
	b := &fakeJob{name: "b", output: "B"}
	registerFake(t, reg, "a", a)
	registerFake(t, reg, "b", b)

	def, err := NewDefinition("wf").
		AddNode(NodeDef{ID: "n1", JobType: "a"}).
		AddNode(NodeDef{ID: "n2", JobType: "b", DependsOn: []string{"n1"}}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(reg, nil)
	res, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != StateSucceeded {
		t.Fatalf("state = %s, want succeeded", res.State)
	}
	if res.Nodes["n1"].Output != "A" || res.Nodes["n2"].Output != "B" {
		t.Fatalf("outputs: %v", res.Nodes)
	}
}

func TestEngine_FailedNode_DependentsSkipped_SiblingsRun(t *testing.T) {
	reg := NewRegistry()
	registerFake(t, reg, "ok", &fakeJob{output: "ok"})
	registerFake(t, reg, "boom", &fakeJob{failAlways: true})

	// Graph:
	//   root -> bad -> child  (child should be skipped)
	//   root -> sibling       (sibling should succeed)
	def, err := NewDefinition("wf").
		AddNode(NodeDef{ID: "root", JobType: "ok"}).
		AddNode(NodeDef{ID: "bad", JobType: "boom", DependsOn: []string{"root"}}).
		AddNode(NodeDef{ID: "child", JobType: "ok", DependsOn: []string{"bad"}}).
		AddNode(NodeDef{ID: "sibling", JobType: "ok", DependsOn: []string{"root"}}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(reg, nil)
	res, _ := eng.Run(context.Background(), def)

	if res.Nodes["root"].State != StateSucceeded {
		t.Errorf("root: %s", res.Nodes["root"].State)
	}
	if res.Nodes["bad"].State != StateFailed {
		t.Errorf("bad: %s", res.Nodes["bad"].State)
	}
	if res.Nodes["child"].State != StateSkipped {
		t.Errorf("child: %s (should be skipped)", res.Nodes["child"].State)
	}
	if res.Nodes["sibling"].State != StateSucceeded {
		t.Errorf("sibling: %s (should have run)", res.Nodes["sibling"].State)
	}
	if res.State != StateFailed {
		t.Errorf("workflow state = %s, want failed", res.State)
	}
}

func TestEngine_RetrySucceedsAfterTransientFailures(t *testing.T) {
	reg := NewRegistry()
	j := &fakeJob{failCount: 2, output: "ok"} // fails attempts 1,2; succeeds on 3
	registerFake(t, reg, "flaky", j)

	def, _ := NewDefinition("wf").
		AddNode(NodeDef{
			ID: "n", JobType: "flaky",
			Retry: &RetryPolicy{MaxAttempts: 3, Backoff: func(int) int { return 0 }},
		}).Build()
	eng := NewEngine(reg, nil)
	// Make sleep instant for the test.
	eng.sleep = func(context.Context, time.Duration) error { return nil }

	res, _ := eng.Run(context.Background(), def)
	if res.Nodes["n"].State != StateSucceeded {
		t.Fatalf("state = %s", res.Nodes["n"].State)
	}
	if j.execN.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", j.execN.Load())
	}
}

func TestEngine_RetryExhausted_NodeFailed(t *testing.T) {
	reg := NewRegistry()
	j := &fakeJob{failAlways: true}
	registerFake(t, reg, "broken", j)

	def, _ := NewDefinition("wf").
		AddNode(NodeDef{
			ID: "n", JobType: "broken",
			Retry: &RetryPolicy{MaxAttempts: 3, Backoff: func(int) int { return 0 }},
		}).Build()
	eng := NewEngine(reg, nil)
	eng.sleep = func(context.Context, time.Duration) error { return nil }

	res, _ := eng.Run(context.Background(), def)
	if res.Nodes["n"].State != StateFailed {
		t.Fatalf("state = %s", res.Nodes["n"].State)
	}
	if j.execN.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", j.execN.Load())
	}
}

func TestEngine_Condition_FalseSkipsNode(t *testing.T) {
	reg := NewRegistry()
	registerFake(t, reg, "ok", &fakeJob{output: 1})
	guarded := &fakeJob{output: "should-not-run"}
	registerFake(t, reg, "guarded", guarded)

	def, _ := NewDefinition("wf").
		AddNode(NodeDef{ID: "src", JobType: "ok"}).
		AddNode(NodeDef{
			ID: "g", JobType: "guarded", DependsOn: []string{"src"},
			Condition: func(deps map[string]NodeResult) bool {
				v, ok := deps["src"].Output.(int)
				return ok && v > 100
			},
		}).Build()
	eng := NewEngine(reg, nil)
	res, _ := eng.Run(context.Background(), def)

	if res.Nodes["g"].State != StateSkipped {
		t.Fatalf("guarded state = %s, want skipped", res.Nodes["g"].State)
	}
	if guarded.execN.Load() != 0 {
		t.Fatalf("guarded ran %d times despite false condition", guarded.execN.Load())
	}
}

func TestEngine_Cancellation_StopsPendingWork(t *testing.T) {
	reg := NewRegistry()
	registerFake(t, reg, "slow", &fakeJob{delay: 100 * time.Millisecond, output: "slow"})

	def, _ := NewDefinition("wf").
		AddNode(NodeDef{ID: "a", JobType: "slow"}).
		AddNode(NodeDef{ID: "b", JobType: "slow", DependsOn: []string{"a"}}).
		Build()
	eng := NewEngine(reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res, _ := eng.Run(ctx, def)
	// At least one of a/b must be cancelled. We don't assert on which to
	// avoid race-flakes; we assert on the workflow-level state instead.
	if res.State != StateCancelled && res.State != StateFailed {
		t.Fatalf("workflow state = %s; expected cancelled or failed", res.State)
	}
}

func TestEngine_EmitsLifecycleEvents(t *testing.T) {
	reg := NewRegistry()
	registerFake(t, reg, "ok", &fakeJob{output: "ok"})

	bus := NewEventBus()
	var mu sync.Mutex
	var events []EventKind
	bus.Subscribe(SubscriberFunc(func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e.Kind)
	}))

	def, _ := NewDefinition("wf").
		AddNode(NodeDef{ID: "n", JobType: "ok"}).Build()
	eng := NewEngine(reg, bus)
	if _, err := eng.Run(context.Background(), def); err != nil {
		t.Fatal(err)
	}

	// Expect: workflow.started, node.started, node.ended, workflow.ended.
	expect := []EventKind{
		EventWorkflowStarted, EventNodeStarted, EventNodeEnded, EventWorkflowEnded,
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != len(expect) {
		t.Fatalf("events = %v, want %v", events, expect)
	}
	for i, e := range expect {
		if events[i] != e {
			t.Fatalf("event[%d] = %s, want %s", i, events[i], e)
		}
	}
}

func TestDefinition_DetectsCycles(t *testing.T) {
	_, err := NewDefinition("wf").
		AddNode(NodeDef{ID: "a", JobType: "x", DependsOn: []string{"b"}}).
		AddNode(NodeDef{ID: "b", JobType: "x", DependsOn: []string{"a"}}).
		Build()
	if err == nil {
		t.Fatal("expected cycle detection")
	}
}

func TestDefinition_RejectsUnknownDependency(t *testing.T) {
	_, err := NewDefinition("wf").
		AddNode(NodeDef{ID: "a", JobType: "x", DependsOn: []string{"missing"}}).
		Build()
	if err == nil {
		t.Fatal("expected unknown-dep error")
	}
}

func TestRegistry_RejectsDuplicateRegistration(t *testing.T) {
	reg := NewRegistry()
	f := func(_ map[string]any) (Job, error) { return &fakeJob{}, nil }
	if err := reg.Register("dup", f); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("dup", f); err == nil {
		t.Fatal("expected duplicate error")
	}
}
