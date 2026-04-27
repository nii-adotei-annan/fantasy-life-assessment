// Demo: wires server middleware -> handler -> client middleware -> fake
// downstream, plus a minimal pipeline run.
//
// The demo proves the three systems can compose via their public
// interfaces. It does NOT prove they are one system: each could be deleted
// and the others would still work.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/middleware/client"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/middleware/server"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline/stages"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow/jobs"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Run the pipeline standalone. No coupling to the other systems.
	logger.Println("=== pipeline ===")
	runPipeline(ctx, logger)

	// 2. Run the workflow engine standalone.
	logger.Println("=== workflow ===")
	runWorkflow(ctx, logger)

	// 3. Run a tiny HTTP demo: server middleware -> handler that uses the
	// client middleware stack to call a fake downstream.
	logger.Println("=== http server + client ===")
	runHTTPDemo(ctx, logger)
}

func runPipeline(ctx context.Context, logger *log.Logger) {
	src := &pipeline.SliceSource{Items: []pipeline.Record{
		{ID: "1", Data: map[string]any{"name": "ada", "email": "a@b.c"}},
		{ID: "2", Data: map[string]any{"name": "grace", "email": "g@h.i"}},
		{ID: "3", Data: map[string]any{"name": "missing-email"}},
		{ID: "1", Data: map[string]any{"name": "duplicate"}}, // dropped by dedup
	}}
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
		logger.Fatalf("pipeline build: %v", err)
	}
	if err := p.Run(ctx); err != nil {
		logger.Printf("pipeline run: %v", err)
	}
	logger.Printf("pipeline: %d succeeded, %d dead-lettered", len(sink.Snapshot()), len(dead.Snapshot()))
}

func runWorkflow(ctx context.Context, logger *log.Logger) {
	reg := workflow.NewRegistry()
	_ = reg.Register("transform", jobs.NewTransformFactory(map[string]func(any) (any, error){
		"upper": func(v any) (any, error) {
			s, _ := v.(string)
			return strings.ToUpper(s), nil
		},
	}))
	_ = reg.Register("email", jobs.NewEmailFactory(stdoutMailer{logger: logger}))

	bus := workflow.NewEventBus()
	bus.Subscribe(workflow.SubscriberFunc(func(e workflow.Event) {
		logger.Printf("event: %s wf=%s node=%s state=%s", e.Kind, e.WorkflowID, e.NodeID, e.State)
	}))

	def, err := workflow.NewDefinition("greet").
		AddNode(workflow.NodeDef{
			ID: "upper", JobType: "transform",
			Config: map[string]any{"name": "upper"},
			Input:  "ada lovelace",
		}).
		AddNode(workflow.NodeDef{
			ID: "send", JobType: "email", DependsOn: []string{"upper"},
			Config: map[string]any{"to": "ops@example.com", "subject": "greet"},
			Input:  map[string]any{"body": "hello"},
		}).
		Build()
	if err != nil {
		logger.Fatalf("workflow build: %v", err)
	}
	eng := workflow.NewEngine(reg, bus)
	res, err := eng.Run(ctx, def)
	if err != nil {
		logger.Fatalf("workflow run: %v", err)
	}
	logger.Printf("workflow %s state=%s", res.WorkflowID, res.State)
}

type stdoutMailer struct{ logger *log.Logger }

func (m stdoutMailer) Send(_ context.Context, to, subject, body string) error {
	m.logger.Printf("[mail] to=%s subj=%q body=%q", to, subject, body)
	return nil
}

func runHTTPDemo(ctx context.Context, logger *log.Logger) {
	// Fake downstream the client middleware will call.
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"hello": "world"})
	}))
	defer downstream.Close()

	// Build the client stack: log -> retry -> ratelimit -> cache -> default.
	var doer client.Doer = http.DefaultClient
	doer = client.NewCached(doer, time.Second)
	doer = client.NewRateLimited(doer, 10, 100)
	doer = client.NewRetried(doer, 2, 10*time.Millisecond, 100*time.Millisecond)
	doer = client.NewLogged(doer, client.LoggerFunc(func(f string, a ...any) {
		logger.Printf("[client] "+f, a...)
	}))

	// Server handler: calls the downstream via the client stack.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", downstream.URL, nil)
		resp, err := doer.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	// Server middleware chain.
	chain := server.Chain(
		server.RequestID(nil),
		server.Logging(server.LoggerFunc(func(f string, a ...any) {
			logger.Printf("[server] "+f, a...)
		})),
		server.PerClientRateLimit(100, time.Second, nil),
		server.Auth(server.AuthenticatorFunc(func(_ context.Context, tok string) (string, error) {
			if tok == "demo-token" {
				return "demo-user", nil
			}
			return "", fmt.Errorf("bad token")
		})),
	)
	srv := httptest.NewServer(chain(handler))
	defer srv.Close()

	// Drive a request through the full stack.
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/echo", nil)
	req.Header.Set("Authorization", "Bearer demo-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Printf("demo request: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	logger.Printf("demo response: status=%d body=%s", resp.StatusCode, body)
}
