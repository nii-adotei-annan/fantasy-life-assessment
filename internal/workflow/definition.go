package workflow

import (
	"errors"
	"fmt"
)

// NodeDef is the declarative description of one node in a workflow.
type NodeDef struct {
	ID        string
	JobType   string         // looked up in Registry
	Config    map[string]any // passed to the JobFactory
	DependsOn []string       // IDs of upstream nodes
	Condition Predicate      // optional; if nil, node runs unconditionally
	Retry     *RetryPolicy   // optional; default is one attempt, no backoff
	Input     any            // static input passed to Execute
}

// Definition is a complete workflow definition.
type Definition struct {
	ID    string
	Nodes []NodeDef
}

// DefBuilder constructs a Definition with validation.
type DefBuilder struct {
	def Definition
}

func NewDefinition(id string) *DefBuilder {
	return &DefBuilder{def: Definition{ID: id}}
}

func (b *DefBuilder) AddNode(n NodeDef) *DefBuilder {
	b.def.Nodes = append(b.def.Nodes, n)
	return b
}

func (b *DefBuilder) Build() (Definition, error) {
	if b.def.ID == "" {
		return Definition{}, errors.New("workflow: definition ID is required")
	}
	ids := make(map[string]bool, len(b.def.Nodes))
	for _, n := range b.def.Nodes {
		if n.ID == "" {
			return Definition{}, errors.New("workflow: node ID is required")
		}
		if ids[n.ID] {
			return Definition{}, fmt.Errorf("workflow: duplicate node ID %q", n.ID)
		}
		ids[n.ID] = true
	}
	for _, n := range b.def.Nodes {
		for _, dep := range n.DependsOn {
			if !ids[dep] {
				return Definition{}, fmt.Errorf("workflow: node %q depends on unknown node %q", n.ID, dep)
			}
		}
	}
	if err := detectCycle(b.def); err != nil {
		return Definition{}, err
	}
	return b.def, nil
}

// detectCycle does a DFS on the dependency graph. Cycles are rejected at
// build time rather than run time because they're an authoring bug, not a
// runtime condition.
func detectCycle(def Definition) error {
	graph := make(map[string][]string, len(def.Nodes))
	for _, n := range def.Nodes {
		graph[n.ID] = n.DependsOn
	}
	const (
		white = 0 // unvisited
		gray  = 1 // in current DFS path
		black = 2 // fully explored
	)
	color := make(map[string]int, len(def.Nodes))
	var visit func(id string) error
	visit = func(id string) error {
		switch color[id] {
		case gray:
			return fmt.Errorf("workflow: cycle detected at node %q", id)
		case black:
			return nil
		}
		color[id] = gray
		for _, dep := range graph[id] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}
	for _, n := range def.Nodes {
		if err := visit(n.ID); err != nil {
			return err
		}
	}
	return nil
}
