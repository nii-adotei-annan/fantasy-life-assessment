// Package integration contains end-to-end tests that exercise multiple
// independent systems wired together via their public interfaces.
//
// These tests are NOT a substitute for per-package unit tests. They exist
// to demonstrate that the three independent systems can compose without
// modification — which is the only thing the demo binary is meant to prove.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mwclient "github.com/nii-adotei-annan/fantasy-life-assessment/internal/middleware/client"
	mwserver "github.com/nii-adotei-annan/fantasy-life-assessment/internal/middleware/server"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline/stages"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow/jobs"
)

// TestEndToEnd_HTTPHandler_TriggersWorkflow_UsesClientStack is the
// composability proof: a request comes in through the server middleware
// chain, the handler triggers a workflow whose job calls a fake downstream
// through the client middleware stack.
//
// Lifecycle events from the workflow are captured into a counter so we
// can assert state transitions are observable at the boundary.
func TestEndToEnd_HTTPHandler_TriggersWorkflow_UsesClientStack(t *testing.T) {
	// 1. Fake downstream the workflow's HTTP job will call.
	var downstreamHits atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		downstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	}))
	defer downstream.Close()

	// 2. Build a real client middleware stack pointing at the downstream.
	var doer mwclient.Doer = http.DefaultClient
	doer = mwclient.NewCached(doer, time.Second)
	doer = mwclient.NewRateLimited(doer, 100, 1000)
	doer = mwclient.NewRetried(doer, 2, time.Microsecond, time.Microsecond)

	// 3. Workflow registry with an HTTP job that uses our client stack.
	reg := workflow.NewRegistry()
	if err := reg.Register("http", jobs.NewHTTPFactory(&http.Client{Transport: doerRT{doer}})); err != nil {
		t.Fatal(err)
	}

	// 4. Capture lifecycle events to verify state transitions are visible.
	var (
		evtMu  sync.Mutex
		events []workflow.EventKind
	)
	bus := workflow.NewEventBus()
	bus.Subscribe(workflow.SubscriberFunc(func(e workflow.Event) {
		evtMu.Lock()
		defer evtMu.Unlock()
		events = append(events, e.Kind)
	}))
	eng := workflow.NewEngine(reg, bus)

	// 5. HTTP handler that triggers a one-node workflow.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		def, err := workflow.NewDefinition("on-request").
			AddNode(workflow.NodeDef{
				ID:      "fetch",
				JobType: "http",
				Config:  map[string]any{"url": downstream.URL},
			}).Build()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		res, err := eng.Run(r.Context(), def)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if res.State != workflow.StateSucceeded {
			http.Error(w, "workflow not ok", 502)
			return
		}
		_, _ = w.Write([]byte("done"))
	})

	// 6. Server middleware chain.
	chain := mwserver.Chain(
		mwserver.RequestID(nil),
		mwserver.PerClientRateLimit(100, time.Second, nil),
		mwserver.Auth(mwserver.AuthenticatorFunc(func(_ context.Context, tok string) (string, error) {
			if tok == "ok" {
				return "user", nil
			}
			return "", errors.New("nope")
		})),
	)
	srv := httptest.NewServer(chain(handler))
	defer srv.Close()

	// 7. Drive the request.
	req, _ := http.NewRequest("GET", srv.URL+"/run", nil)
	req.Header.Set("Authorization", "Bearer ok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if got := downstreamHits.Load(); got != 1 {
		t.Fatalf("downstream hits = %d, want 1", got)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Fatal("request id not echoed")
	}

	// 8. Verify lifecycle events were observed.
	evtMu.Lock()
	defer evtMu.Unlock()
	want := map[workflow.EventKind]bool{
		workflow.EventWorkflowStarted: true,
		workflow.EventNodeStarted:     true,
		workflow.EventNodeEnded:       true,
		workflow.EventWorkflowEnded:   true,
	}
	for _, e := range events {
		delete(want, e)
	}
	if len(want) != 0 {
		t.Fatalf("missing events: %v (saw %v)", want, events)
	}
}

// TestEndToEnd_PipelineFedByWorkflowOutput shows the pipeline and workflow
// engine cooperating through value passing only — neither imports the
// other's internal types. The workflow produces a list of records; the
// pipeline processes them.
func TestEndToEnd_PipelineFedByWorkflowOutput(t *testing.T) {
	// Workflow: a single transform job that yields three records.
	reg := workflow.NewRegistry()
	if err := reg.Register("produce", jobs.NewTransformFactory(map[string]func(any) (any, error){
		"records": func(_ any) (any, error) {
			return []pipeline.Record{
				{ID: "1", Data: map[string]any{"name": "ada", "email": "a@b.c"}},
				{ID: "2", Data: map[string]any{"name": "grace", "email": "g@h.i"}},
				{ID: "3", Data: map[string]any{"name": "missing-email"}},
			}, nil
		},
	})); err != nil {
		t.Fatal(err)
	}
	eng := workflow.NewEngine(reg, nil)
	def, _ := workflow.NewDefinition("produce-wf").
		AddNode(workflow.NodeDef{ID: "p", JobType: "produce", Config: map[string]any{"name": "records"}}).
		Build()
	res, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	recs, ok := res.Nodes["p"].Output.([]pipeline.Record)
	if !ok {
		t.Fatalf("workflow output type: %T", res.Nodes["p"].Output)
	}

	// Pipeline: validate, uppercase, dedup. Records-with-missing-email
	// must end up in the dead-letter sink.
	src := &pipeline.SliceSource{Items: recs}
	sink := &pipeline.SliceSink{}
	dead := &pipeline.SliceDeadLetterSink{}
	p, err := pipeline.NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(&stages.SchemaValidator{RequiredFields: []string{"name", "email"}}).
		WithStage(&stages.FieldTransformer{Transforms: map[string]func(any) (any, error){
			"name": func(v any) (any, error) {
				s, _ := v.(string)
				return strings.ToUpper(s), nil
			},
		}}).
		WithStage(stages.NewDeduplicator()).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Snapshot()); got != 2 {
		t.Fatalf("sink = %d, want 2", got)
	}
	if got := len(dead.Snapshot()); got != 1 {
		t.Fatalf("dead = %d, want 1", got)
	}
	if got := dead.Snapshot()[0].Stage; got != "schema-validator" {
		t.Fatalf("dead stage = %q", got)
	}
}

// TestEndToEnd_FailedWorkflowBranch_DoesNotHaltSiblings is the failure
// isolation contract from the brief, exercised end-to-end.
func TestEndToEnd_FailedWorkflowBranch_DoesNotHaltSiblings(t *testing.T) {
	reg := workflow.NewRegistry()
	if err := reg.Register("ok", func(_ map[string]any) (workflow.Job, error) {
		return workflow.JobFunc(func(_ context.Context, _ any) (any, error) { return "ok", nil }), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("fail", func(_ map[string]any) (workflow.Job, error) {
		return workflow.JobFunc(func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("boom")
		}), nil
	}); err != nil {
		t.Fatal(err)
	}

	def, _ := workflow.NewDefinition("isolation").
		AddNode(workflow.NodeDef{ID: "root", JobType: "ok"}).
		AddNode(workflow.NodeDef{ID: "bad", JobType: "fail", DependsOn: []string{"root"}}).
		AddNode(workflow.NodeDef{ID: "downstream-of-bad", JobType: "ok", DependsOn: []string{"bad"}}).
		AddNode(workflow.NodeDef{ID: "sibling", JobType: "ok", DependsOn: []string{"root"}}).
		Build()
	eng := workflow.NewEngine(reg, nil)
	res, _ := eng.Run(context.Background(), def)

	if res.Nodes["sibling"].State != workflow.StateSucceeded {
		t.Fatalf("sibling = %s (failure halted unrelated branch)", res.Nodes["sibling"].State)
	}
	if res.Nodes["downstream-of-bad"].State != workflow.StateSkipped {
		t.Fatalf("downstream-of-bad = %s (should be skipped)", res.Nodes["downstream-of-bad"].State)
	}
}

// doerRT adapts a client.Doer to http.RoundTripper so a job can use the
// same middleware stack via *http.Client. This is the kind of glue we
// want at the integration boundary, not in any internal package.
type doerRT struct{ d mwclient.Doer }

func (a doerRT) RoundTrip(req *http.Request) (*http.Response, error) { return a.d.Do(req) }
