package jobs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
)

func TestHTTPJob_GET_Succeeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()
	j := &HTTPJob{URL: srv.URL}
	out, err := j.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := out.(map[string]any)
	if m["status"] != 200 || !strings.Contains(m["body"].(string), "hello") {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestHTTPJob_4xx_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()
	j := &HTTPJob{URL: srv.URL}
	if _, err := j.Execute(context.Background(), nil); err == nil {
		t.Fatal("expected error on 4xx")
	}
}

func TestHTTPJob_InputOverridesURL(t *testing.T) {
	called := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = r.URL.Path
		w.WriteHeader(200)
	}))
	defer srv.Close()
	j := &HTTPJob{URL: "http://wrong/"}
	_, err := j.Execute(context.Background(), map[string]any{"url": srv.URL + "/right"})
	if err != nil {
		t.Fatal(err)
	}
	if called != "/right" {
		t.Fatalf("called = %q", called)
	}
}

type stubMailer struct {
	to, subj, body string
	err            error
}

func (m *stubMailer) Send(_ context.Context, to, s, b string) error {
	m.to, m.subj, m.body = to, s, b
	return m.err
}

func TestEmailJob_SendsViaMailer(t *testing.T) {
	m := &stubMailer{}
	j := &EmailJob{To: "a@b.c", Subject: "hi", Mailer: m}
	out, err := j.Execute(context.Background(), map[string]any{"body": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if m.to != "a@b.c" || m.body != "hello" {
		t.Fatalf("mailer not called correctly: %+v", m)
	}
	if out.(map[string]any)["sent_to"] != "a@b.c" {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestEmailJob_PropagatesMailerError(t *testing.T) {
	m := &stubMailer{err: errors.New("smtp boom")}
	j := &EmailJob{To: "a@b.c", Mailer: m}
	if _, err := j.Execute(context.Background(), nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestTransformJob_AppliesNamedTransform(t *testing.T) {
	factory := NewTransformFactory(map[string]func(any) (any, error){
		"upper": func(v any) (any, error) {
			s, _ := v.(string)
			return strings.ToUpper(s), nil
		},
	})
	job, err := factory(map[string]any{"name": "upper"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := job.Execute(context.Background(), "ada")
	if err != nil {
		t.Fatal(err)
	}
	if out != "ADA" {
		t.Fatalf("got %v", out)
	}
}

func TestTransformFactory_RejectsUnknownTransform(t *testing.T) {
	f := NewTransformFactory(nil)
	if _, err := f(map[string]any{"name": "missing"}); err == nil {
		t.Fatal("expected error")
	}
}

// Sub-workflow integration: the orchestrator does not know this is a
// sub-workflow. From its perspective, it's just another Job.
func TestSubWorkflowJob_RunsInnerDefinition(t *testing.T) {
	reg := workflow.NewRegistry()
	_ = reg.Register("inner", func(_ map[string]any) (workflow.Job, error) {
		return jobFn(func(_ context.Context, _ any) (any, error) { return "inner-ok", nil }), nil
	})
	innerEng := workflow.NewEngine(reg, nil)
	innerDef, _ := workflow.NewDefinition("inner-wf").
		AddNode(workflow.NodeDef{ID: "x", JobType: "inner"}).Build()

	sub := &SubWorkflowJob{Engine: innerEng, Def: innerDef}
	out, err := sub.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	res := out.(workflow.Result)
	if res.State != workflow.StateSucceeded {
		t.Fatalf("inner state = %s", res.State)
	}
}

// jobFn adapts a function to workflow.Job for test brevity.
type jobFn func(context.Context, any) (any, error)

func (f jobFn) Execute(ctx context.Context, input any) (any, error) { return f(ctx, input) }
