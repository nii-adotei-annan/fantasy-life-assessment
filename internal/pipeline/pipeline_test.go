package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStage is a configurable stage used across tests.
type fakeStage struct {
	name      string
	setupErr  error
	procFn    func(context.Context, Record) (Record, error)
	setupN    int
	teardownN int
	processN  int
}

func (f *fakeStage) Name() string { return f.name }
func (f *fakeStage) Setup(ctx context.Context) error {
	f.setupN++
	return f.setupErr
}
func (f *fakeStage) Teardown(ctx context.Context) error { f.teardownN++; return nil }
func (f *fakeStage) Process(ctx context.Context, r Record) (Record, error) {
	f.processN++
	if f.procFn != nil {
		return f.procFn(ctx, r)
	}
	return r, nil
}

func TestPipeline_HappyPath_AllRecordsReachSink(t *testing.T) {
	src := &SliceSource{Items: []Record{
		{ID: "1", Data: map[string]any{"v": 1}},
		{ID: "2", Data: map[string]any{"v": 2}},
	}}
	sink := &SliceSink{}
	dead := &SliceDeadLetterSink{}
	stage := &fakeStage{name: "noop"}

	p, err := NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(stage).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(sink.Snapshot()); got != 2 {
		t.Fatalf("sink got %d, want 2", got)
	}
	if got := len(dead.Snapshot()); got != 0 {
		t.Fatalf("dead got %d, want 0", got)
	}
	if stage.setupN != 1 || stage.teardownN != 1 {
		t.Fatalf("setup=%d teardown=%d, both want 1", stage.setupN, stage.teardownN)
	}
}

func TestPipeline_RecordError_GoesToDeadLetter_PipelineContinues(t *testing.T) {
	src := &SliceSource{Items: []Record{
		{ID: "1", Data: map[string]any{"v": 1}},
		{ID: "bad", Data: map[string]any{"v": 2}},
		{ID: "3", Data: map[string]any{"v": 3}},
	}}
	sink := &SliceSink{}
	dead := &SliceDeadLetterSink{}
	stage := &fakeStage{
		name: "fails-on-bad",
		procFn: func(_ context.Context, r Record) (Record, error) {
			if r.ID == "bad" {
				return Record{}, errors.New("boom")
			}
			return r, nil
		},
	}

	p, _ := NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(stage).Build()
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Snapshot()); got != 2 {
		t.Fatalf("sink got %d, want 2", got)
	}
	dls := dead.Snapshot()
	if len(dls) != 1 {
		t.Fatalf("dead got %d, want 1", len(dls))
	}
	if dls[0].Stage != "fails-on-bad" {
		t.Fatalf("dead stage = %q, want fails-on-bad", dls[0].Stage)
	}
	if dls[0].Record.ID != "bad" {
		t.Fatalf("dead record id = %q, want bad", dls[0].Record.ID)
	}
}

func TestPipeline_ZeroRecord_DropsSilently(t *testing.T) {
	src := &SliceSource{Items: []Record{
		{ID: "1", Data: map[string]any{}},
		{ID: "2", Data: map[string]any{}},
	}}
	sink := &SliceSink{}
	dead := &SliceDeadLetterSink{}
	dropAll := &fakeStage{
		name: "dropper",
		procFn: func(_ context.Context, _ Record) (Record, error) {
			return Record{}, nil
		},
	}
	p, _ := NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(dropAll).Build()
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Snapshot()); got != 0 {
		t.Fatalf("sink got %d, want 0", got)
	}
	if got := len(dead.Snapshot()); got != 0 {
		t.Fatalf("dead got %d, want 0", got)
	}
}

func TestPipeline_SetupError_AbortsRunAndTearsDown(t *testing.T) {
	src := &SliceSource{Items: []Record{{ID: "1"}}}
	sink := &SliceSink{}
	dead := &SliceDeadLetterSink{}
	good := &fakeStage{name: "good"}
	bad := &fakeStage{name: "bad", setupErr: errors.New("setup boom")}

	p, _ := NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(good).WithStage(bad).Build()
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected setup error, got nil")
	}
	if good.processN != 0 {
		t.Fatalf("good stage was processed despite setup failure: %d", good.processN)
	}
	// Good stage was set up, so it should be torn down.
	if good.teardownN != 1 {
		t.Fatalf("good teardown = %d, want 1", good.teardownN)
	}
}

func TestPipeline_ContextCancellation_StopsProcessing(t *testing.T) {
	// A source that emits forever until ctx is cancelled.
	src := &blockingSource{}
	sink := &SliceSink{}
	dead := &SliceDeadLetterSink{}
	stage := &fakeStage{name: "noop"}
	p, _ := NewBuilder().
		WithSource(src).WithSink(sink).WithDeadLetter(dead).
		WithStage(stage).Build()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if stage.teardownN != 1 {
		t.Fatalf("teardown not called on cancellation: %d", stage.teardownN)
	}
}

type blockingSource struct{}

func (b *blockingSource) Records(ctx context.Context) (<-chan Record, error) {
	out := make(chan Record)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case out <- Record{ID: "x", Data: map[string]any{}}:
			}
		}
	}()
	return out, nil
}

func TestBuilder_RequiresSourceSinkDeadAndStages(t *testing.T) {
	cases := []struct {
		name string
		mk   func() (*Pipeline, error)
	}{
		{"no source", func() (*Pipeline, error) {
			return NewBuilder().WithSink(&SliceSink{}).WithDeadLetter(&SliceDeadLetterSink{}).WithStage(&fakeStage{}).Build()
		}},
		{"no sink", func() (*Pipeline, error) {
			return NewBuilder().WithSource(&SliceSource{}).WithDeadLetter(&SliceDeadLetterSink{}).WithStage(&fakeStage{}).Build()
		}},
		{"no dead", func() (*Pipeline, error) {
			return NewBuilder().WithSource(&SliceSource{}).WithSink(&SliceSink{}).WithStage(&fakeStage{}).Build()
		}},
		{"no stages", func() (*Pipeline, error) {
			return NewBuilder().WithSource(&SliceSource{}).WithSink(&SliceSink{}).WithDeadLetter(&SliceDeadLetterSink{}).Build()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.mk(); err == nil {
				t.Fatal("expected build error, got nil")
			}
		})
	}
}
