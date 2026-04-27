package workflow

import (
	"fmt"
	"sync"
)

// Registry maps job-type names to factories. The orchestrator looks up
// factories here when building a Workflow from a definition.
//
// We use a per-orchestrator Registry rather than a global one because
// global registries make tests interfere with each other and make it hard
// to run two engines with different job sets in the same process.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]JobFactory
}

func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]JobFactory)}
}

func (r *Registry) Register(jobType string, f JobFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[jobType]; exists {
		return fmt.Errorf("workflow: job type %q already registered", jobType)
	}
	r.factories[jobType] = f
	return nil
}

func (r *Registry) build(jobType string, config map[string]any) (Job, error) {
	r.mu.RLock()
	f, ok := r.factories[jobType]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("workflow: unknown job type %q", jobType)
	}
	return f(config)
}
