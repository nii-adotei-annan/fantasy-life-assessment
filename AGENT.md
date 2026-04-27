# AGENT.md

This document captures the actual prompts and instructions used when an AI
assistant was involved in scaffolding this repository. It exists because
the brief asks candidates to be transparent about AI usage, and because
the _quality_ of the prompts is itself part of the engineering signal.

The prompts below are paraphrased from real working sessions, not
fabricated. Where I rejected an AI suggestion, the reason is documented
in DECISIONS.md.

## Operating principles

These were the standing instructions throughout the session, regardless of
what feature was being built:

1. **Independence over reuse.** Each task is its own package. Do not
   propose shared internal abstractions across tasks. If you find yourself
   wanting to extract a common interface, inline it in each task instead.
2. **Justify every interface.** When suggesting a new type or interface,
   explain what concrete alternative was rejected and why.
3. **Tests are not optional.** Every behavior described in a comment must
   be exercised by a test. Tests assert on behavior, not on implementation
   details.
4. **No hidden globals.** Registries, clocks, RNGs, and HTTP clients are
   injected through constructors. The default constructors wire in
   sensible production values.
5. **Show your reversals.** If an earlier suggestion turned out to be
   wrong, the correction goes in DECISIONS.md with a code snippet, not a
   silent rewrite.

## Phase 1 — repository skeleton

> Set up a Go 1.22 module at `github.com/nii-adotei-annan/fantasy-life-assessment`. Create
> three independent packages under `internal/`: `pipeline`, `workflow`,
> `middleware`. No `pkg/` directory and no shared package. Each task must
> be deletable in isolation. The demo binary lives in `cmd/demo`.

## Phase 2 — pipeline (Task 1)

> Define a `Stage` interface with `Name`, `Setup(ctx)`, `Process(ctx,
Record)`, `Teardown(ctx)`. Records are `{ID string, Data map[string]any}`.
> Per-record errors must NOT halt the pipeline — they go to a dead-letter
> sink that captures the original record, the failing stage name, the
> error, and a timestamp. Setup errors ARE fatal. Teardown runs on every
> exit including cancellation.
>
> Provide three concrete stages: schema validation, field transformer,
> deduplicator. The deduplicator drops records by returning a zero
> `Record` rather than an error — the orchestrator treats empty ID as
> "drop silently."
>
> Tests must cover: happy path, dead-letter on stage error, drop on zero
> record, setup error aborts run, context cancellation stops processing
> and runs teardown, builder rejects missing source/sink/dead/stages.

## Phase 3 — workflow engine (Task 2)

> The orchestrator must not know about specific job types. Job
> implementations register at runtime via a `JobFactory`. Adding a new
> job type requires zero changes to the engine package.
>
> State machine is explicit. Define a `validTransitions` map and a
> `transition(from, to)` helper that rejects invalid moves. The states are
> Pending, Running, Succeeded, Failed, Cancelled, Skipped. Terminal states
> have no outgoing transitions.
>
> Failure isolation: a failed node skips its dependents (transitively) but
> does NOT halt sibling branches. Implement this by checking dependency
> state when each node wakes up — if any dep is not Succeeded, transition
> to Skipped.
>
> Conditional execution: nodes accept an optional `Condition func(deps
map[string]NodeResult) bool`. Returning false transitions the node to
> Skipped without running the job.
>
> Sub-workflows: implement as a Job type, NOT as a special case in the
> engine. This preserves the rule above.
>
> Retries: per-node `RetryPolicy{MaxAttempts, Backoff}`. Backoff is a
> function of attempt number; the engine sleeps via a context-aware
> helper that's injectable for tests.
>
> Event bus: synchronous fan-out. Subscribers are responsible for their
> own buffering if they need it. Async subscribers can wrap themselves
> trivially; making the bus async would complicate test ordering.
>
> Cycle detection happens at `Build()` time, not `Run()` time. Cycles are
> authoring bugs.

## Phase 4 — HTTP middleware (Task 3)

> Define a `Doer` interface (`Do(req) (resp, error)`) so each cross-cutting
> concern is a decorator. Build four client decorators: token-bucket rate
> limit, retry with exponential backoff and full jitter, TTL response
> cache (GET only, 2xx only), structured logging.
>
> Retry rules: retry on 5xx and network errors; do NOT retry on 4xx.
> Replay request bodies by reading them into memory once and rebuilding
> the reader per attempt. Drain failed response bodies before retrying.
>
> Build server middleware: request ID (respect incoming header, echo in
> response), logging (capture status via a `statusRecorder` wrapper),
> per-client rate limit (fixed window, keyed by remote IP by default),
> bearer-token auth (Authenticator interface, principal injected into
> context).
>
> Compose with a `Chain(...)` helper that applies outermost-first.
>
> The `Logger` interface in `client/` and `server/` is duplicated, NOT
> shared. Independence over reuse.

## Phase 5 — demo

> Single binary `cmd/demo`. Three sections: pipeline standalone, workflow
> standalone, HTTP request through full server+client stack. Under 200
> lines. The demo proves composability via public interfaces only — it
> does not import or reach into any internal types.

## Phase 6 — tests

> Three layers:
>
> - **Unit tests** in each package, asserting behavior of one type at a
>   time. Use mocks for collaborators.
> - **Contract tests** for the state machine (`transition` validity table)
>   and the builder validators (every required field rejected when missing).
> - **Integration tests** under `tests/integration/` that wire systems
>   together via public interfaces and assert on observable lifecycle.
