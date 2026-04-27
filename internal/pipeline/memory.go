package pipeline

import (
	"context"
	"sync"
)

// SliceSource emits records from a fixed slice. Closes the channel when done.
// Respects ctx cancellation between emissions.
type SliceSource struct{ Items []Record }

func (s *SliceSource) Records(ctx context.Context) (<-chan Record, error) {
	out := make(chan Record)
	go func() {
		defer close(out)
		for _, r := range s.Items {
			select {
			case <-ctx.Done():
				return
			case out <- r:
			}
		}
	}()
	return out, nil
}

// SliceSink collects successful records in memory.
type SliceSink struct {
	mu   sync.Mutex
	Recs []Record
}

func (s *SliceSink) Write(ctx context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Recs = append(s.Recs, r)
	return nil
}

func (s *SliceSink) Snapshot() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.Recs))
	copy(out, s.Recs)
	return out
}

// SliceDeadLetterSink collects dead letters in memory.
type SliceDeadLetterSink struct {
	mu sync.Mutex
	DL []DeadLetter
}

func (s *SliceDeadLetterSink) WriteDead(ctx context.Context, dl DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DL = append(s.DL, dl)
	return nil
}

func (s *SliceDeadLetterSink) Snapshot() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadLetter, len(s.DL))
	copy(out, s.DL)
	return out
}
