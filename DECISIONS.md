# DECISIONS.md

This document captures architectural decisions made during the
assessment, including alternatives that were considered and rejected.
Where a decision was reversed mid-implementation, the before/after code
is shown.

---

## D1. Independence vs composition

**Decision:** Three independent packages under `internal/`. No shared
internal package, no `pkg/contracts/`. Each task is deletable in isolation.

**Rejected alternative:** A "platform" framing where the three tasks were
presented as one layered system, with shared types like `Logger`,
`Context helpers`, and typed errors in `pkg/contracts/`.

**Why rejected:** The brief asks for three tasks in a single repository,
not one system. A shared `pkg/contracts/` package looks minimal at first
but is the place where coupling accretes: today it has a logger interface,
tomorrow a metrics interface, by week two it has domain types. Independence
is enforced most reliably by having no place to put shared things.

**Cost paid:** The `Logger` interface is duplicated between
`internal/middleware/client` and `internal/middleware/server`:

```go
// internal/middleware/client/logging.go
type Logger interface {
    Logf(format string, args ...any)
}

// internal/middleware/server/logging.go
type Logger interface {
    Logf(format string, args ...any)
}
```

Three lines duplicated. Not worth a shared package. If a fourth task
landed and also needed this interface, I would still inline it.

**Composability is proven by the demo and integration tests, not by
shared types.** See `tests/integration/integration_test.go`.

---

## D2. Record shape: `map[string]any` vs typed struct

**Decision:** `Record { ID string; Data map[string]any }` in the pipeline.

**Rejected alternative:** A generic `Record[T]` with a typed payload.

**Why rejected:** Stages are configured at runtime and must compose across
different schemas. A `SchemaValidator[User]` and `SchemaValidator[Order]`
would need either generics-on-the-Stage-interface (which Go's generics do
not cleanly support across a slice of differently-typed implementations)
or per-schema pipelines (which defeats the point of pluggability).

The cost is that stages must do their own type assertions when reading
data fields. The schema validator's existence is the safety net for that.

```go
type Record struct {
    ID   string
    Data map[string]any
}
```

---

## D3. Dead-letter captures the full record

**Decision:** `DeadLetter` carries the original `Record`, the failing
stage name, the error, and a timestamp.

**Rejected alternative:** Carrying just an error string and the record ID.

**Why rejected:** Operators replaying dead letters need the original
input. An ID alone forces them to look up the source data, which may be
gone (the source might be a Kafka topic with retention; the record is
already past).

```go
type DeadLetter struct {
    Record    Record
    Stage     string
    Err       error
    Timestamp time.Time
}
```

---

## D4. Pipeline is serial per record

**Decision:** Records are processed one at a time, through each stage in
sequence. No fan-out, no per-stage worker pools.

**Rejected alternative:** Stage-parallel processing with channels between
stages.

**Why rejected:** Out of scope for the assessment. The `Stage` interface
does not preclude a future fan-out implementation; it just isn't built.
Adding it would require thinking about backpressure, ordering guarantees
on the dead-letter sink, and graceful drain — none of which the brief
calls for.

---

## D5. Reversal — switch dispatch → runtime registry

**Decision:** Job types register at runtime via a `Registry`. The engine
looks up factories by name.

**Initial AI suggestion (rejected):**

```go
// REJECTED. The original suggestion handled job dispatch with a switch.
func (e *Engine) executeJob(jobType string, cfg map[string]any) (any, error) {
    switch jobType {
    case "http":
        return executeHTTP(cfg)
    case "email":
        return executeEmail(cfg)
    case "transform":
        return executeTransform(cfg)
    default:
        return nil, fmt.Errorf("unknown job type: %s", jobType)
    }
}
```

**Why rejected:** The brief explicitly requires that adding a new job
type require **zero changes to the orchestrator**. A switch statement
inside the engine package fails this requirement: every new job type
forces an edit to `engine.go`.

**Replacement:**

```go
// internal/workflow/registry.go
type Registry struct {
    mu        sync.RWMutex
    factories map[string]JobFactory
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
```

The engine never knows what job types exist. This is what made
sub-workflows trivial: a sub-workflow is just another `JobFactory`, no
special case in the engine.

---

## D6. Reversal — inline struct → named `nodeSlot` type

**Decision:** Per-node runtime state lives in a named `nodeSlot` type
with explicit `transitionTo`, `setResult`, and `snapshot` methods.

**Initial implementation (rejected after writing it):**

```go
// REJECTED. The first cut used an unnamed struct passed between funcs.
slots := make(map[string]*struct {
    mu     sync.Mutex
    state  State
    result NodeResult
    done   chan struct{}
}, len(def.Nodes))

// And helpers had to repeat the type signature:
func (e *Engine) markCancelled(self *struct {
    mu     sync.Mutex
    state  State
    result NodeResult
    done   chan struct{}
}, nodeID, workflowID string) {
    // ...
}
```

**Why rejected:** The signatures of helper functions were unreadable. The
mutex was being locked and released ad-hoc at every call site, which made
the state machine logic hard to follow and easy to break.

**Replacement:**

```go
// internal/workflow/engine.go
type nodeSlot struct {
    mu     sync.Mutex
    state  State
    result NodeResult
    done   chan struct{}
}

func (s *nodeSlot) transitionTo(to State) (State, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    ns, err := transition(s.state, to)
    if err != nil {
        return s.state, err
    }
    s.state = ns
    return ns, nil
}
```

Concurrency control is now a property of the type, not a discipline that
every caller must follow. The state machine and the synchronization story
are both in one place.

---

## D7. Explicit state machine vs implicit-via-code-paths

**Decision:** A `validTransitions` map enumerates allowed state changes.
A `transition()` helper rejects anything not listed.

**Rejected alternative:** Setting state directly throughout the engine
(`self.state = StateFailed`).

**Why rejected:** The brief calls out "explicit state machine" and
"reject invalid transitions." An implicit machine — where state is just
a field that anyone can write — silently allows nonsensical transitions
like `Succeeded -> Running` and only catches them when something downstream
crashes.

```go
var validTransitions = map[State]map[State]bool{
    StatePending: {
        StateRunning:   true,
        StateCancelled: true,
        StateSkipped:   true,
    },
    StateRunning: {
        StateSucceeded: true,
        StateFailed:    true,
        StateCancelled: true,
    },
    // Terminal states are intentionally empty maps, not missing.
    StateSucceeded: {},
    StateFailed:    {},
    StateCancelled: {},
    StateSkipped:   {},
}
```

The `markFailed` helper deliberately bypasses the state machine, but only
from defensive paths after a transition error has already occurred. That's
the one place the rule is broken, and it's commented as such.

---

## D8. Synchronous event bus

**Decision:** `EventBus.Publish` calls every subscriber inline.

**Rejected alternative:** Per-subscriber channels with goroutines fanning
out events.

**Why rejected:** Async fan-out makes test ordering non-deterministic
without significant scaffolding. The async case is recoverable by a
subscriber that forwards events to its own channel — the right place to
buffer is the subscriber, because only the subscriber knows its
appropriate backpressure policy. A bus that buffers for everyone has to
pick one wrong policy.

```go
func (b *EventBus) Publish(e Event) {
    b.mu.RLock()
    subs := make([]Subscriber, len(b.subs))
    copy(subs, b.subs)
    b.mu.RUnlock()
    for _, s := range subs {
        s.OnEvent(e)
    }
}
```

The mutex is dropped before invoking subscribers so a slow subscriber
does not block subscription changes from other goroutines.

---

## D9. Cycle detection at build time

**Decision:** `DefBuilder.Build()` runs a DFS-based cycle check before
returning the `Definition`.

**Rejected alternative:** Detecting cycles at run time when nodes
deadlock waiting for dependencies.

**Why rejected:** A cycle is an authoring bug, not a runtime condition.
Catching it at build time gives the operator a clear error message at
deploy or test time, instead of a hung workflow at 3am.

```go
func detectCycle(def Definition) error {
    const (
        white = 0
        gray  = 1 // in current DFS path
        black = 2
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
    // ...
}
```

---

## D10. Failure isolation = transitive skip on dep failure

**Decision:** When a node wakes up to find that any of its dependencies
is not in `Succeeded`, the node transitions to `Skipped`. Independent
branches keep running.

**Why this satisfies the brief:** "A failed job must not halt unrelated
branches." The brief is about _unrelated_ branches; _related_ branches
(transitive dependents of the failure) are correctly skipped, because
running them would mean executing with missing or invalid input.

```go
// internal/workflow/engine.go (excerpt)
for _, depID := range def.DependsOn {
    dep := slots[depID]
    select {
    case <-ctx.Done():
        e.markCancelled(self, def.ID, workflowID)
        return
    case <-dep.done:
    }
    depState, depRes := dep.snapshot()
    depResults[depID] = depRes
    if depState != StateSucceeded {
        ns, _ := self.transitionTo(StateSkipped)
        // ...
        return
    }
}
```

---

## D11. Retry replays request bodies

**Decision:** `Retried.Do` reads the request body once into a `[]byte`
and rebuilds an `io.NopCloser(bytes.NewReader(...))` for each attempt.

**Rejected alternative:** Cloning `req.Body` per attempt, or relying on
`http.Request.GetBody`.

**Why rejected:** `req.Body` is a single-shot reader; reading it consumes
it. `GetBody` is only set on requests cloned from a previously-consumed
request. The simple, predictable approach is to read the body up front
and rebuild it. The cost is memory proportional to the body size of every
retried request.

```go
var bodyBytes []byte
if req.Body != nil && req.Body != http.NoBody {
    var err error
    bodyBytes, err = io.ReadAll(req.Body)
    if err != nil {
        return nil, err
    }
    _ = req.Body.Close()
}
// ...
for attempt := 0; attempt <= r.maxRetries; attempt++ {
    if bodyBytes != nil {
        req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
    }
    // ...
}
```

For very large bodies a streaming approach with `req.GetBody` would be
better. The current implementation is clear about what it does and what
it costs.

---

## D12. Retry: 5xx yes, 4xx no

**Decision:** `shouldRetry` returns true for 5xx and network errors only.

**Why:** 4xx responses are caller errors. Retrying a 401 won't fix the
auth header; retrying a 400 won't fix the payload. Retrying 4xx burns
quota and obscures real problems.

```go
func shouldRetry(resp *http.Response, err error) bool {
    if err != nil {
        return true
    }
    if resp == nil {
        return true
    }
    return resp.StatusCode >= 500
}
```

429 (Too Many Requests) is debatable — a strict reading would retry it,
but only with `Retry-After` honoured. I left it out of retry-eligible
codes because the rate limiter decorator should be solving for that case.

---

## D13. Cache: GET only, 2xx only

**Decision:** Only `GET` responses with status codes in `[200, 300)` are
cached.

**Rejected alternative:** Caching every response regardless of method or
status, with the expectation that callers know what they're doing.

**Why rejected:** Caching non-GETs is rarely safe and is never expected.
Caching errors poisons the cache for the duration of the TTL — a single
500 from the upstream becomes a sustained outage on the client side.

```go
if req.Method != http.MethodGet {
    return c.next.Do(req)
}
// ...
if resp.StatusCode < 200 || resp.StatusCode >= 300 {
    return resp, nil
}
```

The cache also does not honour `Vary` headers or `Cache-Control`. Both
would be required for a production cache; both were out of scope.

---

## D14. Server rate limit: fixed window over token bucket

**Decision:** Per-client fixed-window counter.

**Rejected alternative:** Per-client token bucket.

**Why rejected:** Token bucket is the obvious choice for the _client_ side
because we want smooth outbound traffic. On the server side, what we
actually want is a simple "max N requests per minute per client" rule
that operators can reason about. Fixed window does that in fewer lines
of code, and the burst-at-window-boundary problem is acceptable for the
assessment scope.

```go
func (r *rateLimiter) allow(key string) bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    now := r.now()
    cw, ok := r.clients[key]
    if !ok || now.Sub(cw.windowStart) >= r.window {
        r.clients[key] = &clientWindow{count: 1, windowStart: now}
        return true
    }
    if cw.count >= r.limit {
        return false
    }
    cw.count++
    return true
}
```

A token bucket implementation would be a drop-in replacement behind the
same `Middleware` shape if the requirement changed.

---

## D15. Middleware chain order: outermost first

**Decision:** `Chain(A, B, C)` produces a request flow of
`A -> B -> C -> handler`.

**Rejected alternative:** `Chain(A, B, C)` producing `C -> B -> A -> handler`
(i.e. apply in declaration order).

**Why rejected:** The first reading every reviewer does is "what happens
to a request when it arrives?" That should match the source order. Doing
it the other way means experienced reviewers spend a few seconds working
out the inversion every time.

```go
func Chain(mws ...Middleware) Middleware {
    return func(final http.Handler) http.Handler {
        h := final
        // Apply in reverse so the FIRST middleware is outermost.
        for i := len(mws) - 1; i >= 0; i-- {
            h = mws[i](h)
        }
        return h
    }
}
```

---

## D16. Per-engine registry, not global

**Decision:** `Registry` is a value, constructed and passed to `NewEngine`.
There is no global registry.

**Why:** Globals make tests interfere with each other. Two test cases
that both register a `"http"` job type would race or conflict. A
per-engine registry lets the same process run multiple engines with
different job sets — exactly the situation when sub-workflows are used
with their own restricted set of allowed jobs.

```go
type Registry struct {
    mu        sync.RWMutex
    factories map[string]JobFactory
}

func NewRegistry() *Registry {
    return &Registry{factories: make(map[string]JobFactory)}
}
```

---

## D17. Removed dead code mid-implementation

**Decision:** Removed an accidentally-introduced mutex in
`NewTransformFactory` that was locked and released within the same
function call without protecting any state.

```go
// REMOVED: this mutex did nothing.
mu := sync.Mutex{}
mu.Lock()
defer mu.Unlock()
```

Documenting it because removing dead synchronization primitives is the
kind of thing that's easy to silently rewrite — but a reviewer running
`git log -p` on the file would notice the change anyway. Calling it
out is more honest than glossing over it.

---

## D18. Custom `min()` removed in favour of Go 1.21+ builtin

**Decision:** Used the `min(a, b float64)` builtin in
`internal/middleware/client/ratelimit.go`.

**Rejected alternative:** A local helper `func min(a, b float64) float64`.

**Why rejected:** The local helper conflicts with the builtin under
Go 1.21+, producing a "redeclared" compile error or shadowing the
builtin. Removing it is the right move.

```go
// REMOVED — conflicts with the language builtin.
// func min(a, b float64) float64 { if a < b { return a }; return b }

b.tokens = min(float64(b.capacity), b.tokens+elapsed*b.rate) // uses builtin
```
