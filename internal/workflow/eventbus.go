package workflow

import (
	"sync"
	"time"
)

// EventKind enumerates lifecycle events. Subscribers (logging, metrics,
// notifications) consume these and are decoupled from orchestration.
type EventKind string

const (
	EventWorkflowStarted EventKind = "workflow.started"
	EventWorkflowEnded   EventKind = "workflow.ended"
	EventNodeStarted     EventKind = "node.started"
	EventNodeEnded       EventKind = "node.ended"
	EventNodeRetry       EventKind = "node.retry"
	EventNodeSkipped     EventKind = "node.skipped"
)

// Event is a lifecycle notification.
type Event struct {
	Kind       EventKind
	WorkflowID string
	NodeID     string // empty for workflow-level events
	State      State  // for *Ended events
	Attempt    int    // for retry/end events
	Err        error  // for failure events
	At         time.Time
}

// Subscriber receives events. It must not block; long work should be
// handed off to a goroutine. We deliberately do not buffer per-subscriber
// because the appropriate buffering policy depends on the subscriber.
type Subscriber interface {
	OnEvent(e Event)
}

// SubscriberFunc adapts a plain function to Subscriber.
type SubscriberFunc func(Event)

func (f SubscriberFunc) OnEvent(e Event) { f(e) }

// EventBus is a synchronous fan-out. We chose synchronous over async
// channels because (a) it makes test ordering deterministic, and (b) the
// async case is easily added by a subscriber that forwards to a channel.
type EventBus struct {
	mu   sync.RWMutex
	subs []Subscriber
}

func NewEventBus() *EventBus { return &EventBus{} }

func (b *EventBus) Subscribe(s Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, s)
}

func (b *EventBus) Publish(e Event) {
	b.mu.RLock()
	subs := make([]Subscriber, len(b.subs))
	copy(subs, b.subs)
	b.mu.RUnlock()
	for _, s := range subs {
		s.OnEvent(e)
	}
}
