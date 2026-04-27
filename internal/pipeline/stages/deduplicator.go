package stages

import (
	"context"
	"sync"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
)

// Deduplicator drops records whose ID has been seen before.
//
// Memory note: this keeps every seen ID in memory. For a real system we'd
// back this with a bounded LRU or a Bloom filter behind the same interface;
// the Stage abstraction makes that swap a one-line change in pipeline wiring.
type Deduplicator struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewDeduplicator() *Deduplicator {
	return &Deduplicator{seen: make(map[string]struct{})}
}

func (d *Deduplicator) Name() string { return "deduplicator" }

func (d *Deduplicator) Setup(ctx context.Context) error {
	if d.seen == nil {
		d.seen = make(map[string]struct{})
	}
	return nil
}

func (d *Deduplicator) Teardown(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = nil
	return nil
}

// Process drops duplicates by returning a zero Record with no error.
// The pipeline interprets a zero ID as "drop silently."
func (d *Deduplicator) Process(ctx context.Context, r pipeline.Record) (pipeline.Record, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[r.ID]; ok {
		return pipeline.Record{}, nil
	}
	d.seen[r.ID] = struct{}{}
	return r, nil
}
