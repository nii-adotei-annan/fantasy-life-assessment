package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Pipeline runs records from a Source through a sequence of Stages, writing
// successes to Sink and failures to DeadLetterSink.
//
// Concurrency model: serial per-record processing. We considered fan-out
// across stages but rejected it for the assessment scope; see DECISIONS.md.
type Pipeline struct {
	stages []Stage
	source Source
	sink   Sink
	dead   DeadLetterSink
	clock  func() time.Time // injectable for tests
}

// Builder constructs a Pipeline. Order of WithStage calls defines stage order.
type Builder struct {
	p *Pipeline
}

func NewBuilder() *Builder {
	return &Builder{p: &Pipeline{clock: time.Now}}
}

func (b *Builder) WithSource(s Source) *Builder             { b.p.source = s; return b }
func (b *Builder) WithSink(s Sink) *Builder                 { b.p.sink = s; return b }
func (b *Builder) WithDeadLetter(d DeadLetterSink) *Builder { b.p.dead = d; return b }
func (b *Builder) WithStage(s Stage) *Builder               { b.p.stages = append(b.p.stages, s); return b }

// withClock is for tests.
func (b *Builder) withClock(c func() time.Time) *Builder { b.p.clock = c; return b }

func (b *Builder) Build() (*Pipeline, error) {
	if b.p.source == nil {
		return nil, errors.New("pipeline: source is required")
	}
	if b.p.sink == nil {
		return nil, errors.New("pipeline: sink is required")
	}
	if b.p.dead == nil {
		return nil, errors.New("pipeline: dead letter sink is required")
	}
	if len(b.p.stages) == 0 {
		return nil, errors.New("pipeline: at least one stage is required")
	}
	return b.p, nil
}

// Run executes the pipeline. It returns when the source is exhausted, the
// context is cancelled, or a fatal error occurs (Setup failure).
//
// Per-record errors during Process are NOT fatal: they go to the dead-letter
// sink and processing continues with the next record.
func (p *Pipeline) Run(ctx context.Context) error {
	// Setup phase — fatal on error.
	for _, s := range p.stages {
		if err := s.Setup(ctx); err != nil {
			// Best-effort teardown of stages already set up.
			p.teardownAll()
			return fmt.Errorf("pipeline: setup %s: %w", s.Name(), err)
		}
	}
	// Teardown is deferred so it runs even on panic/cancellation.
	defer p.teardownAll()

	records, err := p.source.Records(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: source: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-records:
			if !ok {
				return nil // source exhausted
			}
			p.processOne(ctx, r)
		}
	}
}

// processOne runs a single record through all stages. On any stage error,
// the record is dead-lettered with the stage name and the loop ends for
// this record (subsequent stages are skipped).
//
// We deliberately do NOT propagate the error up: per-record errors must not
// halt the pipeline. The only way Run returns an error is via context
// cancellation or Setup failure.
func (p *Pipeline) processOne(ctx context.Context, r Record) {
	current := r
	for _, s := range p.stages {
		// Check cancellation between stages so we don't keep working on a
		// record after the operator has signalled shutdown.
		if err := ctx.Err(); err != nil {
			return
		}
		next, err := s.Process(ctx, current)
		if err != nil {
			_ = p.dead.WriteDead(ctx, DeadLetter{
				Record:    current,
				Stage:     s.Name(),
				Err:       err,
				Timestamp: p.clock(),
			})
			return
		}
		// A stage may legitimately drop a record by returning a zero Record
		// with no error (e.g. dedup stage). We detect this via empty ID.
		if next.ID == "" {
			return
		}
		current = next
	}
	if err := p.sink.Write(ctx, current); err != nil {
		_ = p.dead.WriteDead(ctx, DeadLetter{
			Record:    current,
			Stage:     "sink",
			Err:       err,
			Timestamp: p.clock(),
		})
	}
}

func (p *Pipeline) teardownAll() {
	// Use a fresh, non-cancelled context with a short budget so teardown
	// runs even if the parent ctx was cancelled (graceful shutdown). If
	// teardown itself takes too long, we give up.
	teardownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, s := range p.stages {
		_ = s.Teardown(teardownCtx)
	}
}
