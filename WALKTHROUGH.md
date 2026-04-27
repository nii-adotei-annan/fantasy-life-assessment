# WALKTHROUGH.md

> "I treated this as three independent systems with consistent design
> principles, and I added a small demo to prove they compose. I did not
> build a unified platform — that was tempting, but it would have
> introduced coupling that worked against the testability requirement.
>
> The three systems are the pipeline, the workflow engine, and the HTTP
> middleware. None imports another. The demo wires them through public
> interfaces only. If you delete any one of them, the other two still
> compile and pass their tests.

## Deep-dive 1 — Workflow engine: `runNode`

**File:** `internal/workflow/engine.go`, function `runNode`.
**Why this one:** densest piece of the codebase. Demonstrates state
machine, concurrency, failure isolation, retry, conditional execution,
and event emission in one function.

"This is the per-node executor. One goroutine per node. The function
does five things in order: wait for dependencies, evaluate the
condition, transition to running, run the job with retries, transition
to a terminal state."

1. **The dependency wait** — "We block on each dependency's `done`
   channel. As soon as a dependency finishes, we look at its state. If
   it didn't succeed, we transition this node to `Skipped`. That is
   where failure isolation actually happens — failed branches skip
   their dependents, but sibling branches that don't depend on the
   failure keep running."

2. **The condition check** — "Optional predicate over dependency
   results. Returning false skips the node. The dependency results are
   passed in as a map so the predicate can inspect any upstream
   output."

3. **The state transition to running** — "Every state change goes
   through `transitionTo`, which consults the `validTransitions` map.
   Direct field assignment isn't allowed except in one defensive path
   that I'll point out."

4. **The retry loop** — "1-based attempts. Backoff is a function of
   attempt number. Sleep is injectable so tests don't take real wall
   time. We check `ctx.Err()` at the top of each iteration so a
   cancellation between attempts short-circuits cleanly."

5. **The terminal transition** — "Either Succeeded or Failed. The state
   machine catches anything inconsistent."

**"Why one goroutine per node instead of a worker pool?"**

> The brief is for an orchestrator, not a scheduler. Workflows in the
> assessment scope are small. A worker pool would add a queue, a
> bounded-concurrency story, and a fairness policy — none of which the
> brief calls for. The current shape is correct for the scope and
> straightforward to swap out: the per-node logic doesn't change, only
> who calls it.

**"What happens if two dependencies finish at the same time?"**

> Each dependency has its own `done` channel that is closed exactly
> once. We range over the dependency list serially, so the order is
> deterministic per-node. Concurrent close of independent channels is
> safe. The interesting race is on the dependency's _state_, which we
> read under its mutex via `snapshot`.

**"Why is `markFailed` allowed to bypass the state machine?"**

> It's only called from defensive paths after a transition has already
> failed for some reason — meaning we've already detected an
> inconsistency and need to land the node somewhere. Forcing it into
> Failed is the safest choice. It's commented in the source as an
> intentional rule break.

**"What if a dependency is cancelled? Is its dependent skipped or
cancelled?"**

> Skipped. Transitive skip applies to anything that isn't
> `Succeeded` — Failed, Cancelled, or Skipped. The reasoning is the
> same in all three cases: the dependency did not produce the output
> the dependent expected, so running the dependent would mean executing
> with missing or invalid input.

### What I would change

> If I were doing this for production, I would track per-attempt
> timing as part of `NodeResult` and emit it on the event bus. Right
> now, attempt count is in the final `EventNodeEnded` but per-attempt
> latency is gone.

## Deep-dive 2 — Retry decorator

**File:** `internal/middleware/client/retry.go`, function `Do`.
**Why this one:** small enough to read aloud, every line is a decision,
and the body-replay logic is a real bug-magnet that I got right.

"This decorator wraps a `Doer` and retries on transient failures. The
two interesting decisions are which errors to retry and how to replay
the request body across attempts."

1. **The body read at the top** — "We read the request body once into a
   `[]byte` before the loop, then rebuild a fresh `io.NopCloser(bytes.
NewReader(...))` for every attempt. Without this, retrying a POST
   silently sends an empty body on the second attempt because
   `req.Body` is a single-shot reader."

2. **`shouldRetry`** — "Retries on 5xx and network errors. Does NOT
   retry on 4xx. 4xx is a caller error — retrying a 401 won't fix the
   auth header, retrying a 400 won't fix the payload."

3. **The drain-and-close on intermediate failures** — "Failed responses
   are drained before the next attempt to free the underlying
   connection. Forgetting this leaks connections under sustained
   failure."

4. **The skip-drain on the final attempt** — "On the last attempt, we
   return the response as-is even if it's a 5xx. The caller might want
   to inspect the body. If we drained it, we'd hand them an empty
   reader."

5. **Full jitter backoff** — "Random in `[0, base * 2^attempt)`, capped
   at max. Full jitter is the AWS-recommended pattern — it avoids
   thundering-herd retry storms when many clients fail simultaneously."

**"What about request body memory cost?"**

> Real cost. Bodies are buffered in memory for the duration of all
> retries. For very large payloads, the right approach is `req.GetBody`
> if the caller provides it, with a fallback to in-memory replay
> otherwise. I went with the simpler shape because the assessment
> doesn't specify body sizes, and predictable behaviour is more
> important than peak efficiency at this scope.

**"Why not retry 429?"**

> 429 with a `Retry-After` header is debatable — a strict reading would
> retry it, honouring the header. I left it out because the rate
> limiter decorator should be solving for the case that produces 429s
> on our side. If 429s are coming from the upstream, that's a
> configuration problem and silent retry hides it.

**"What if the context is cancelled mid-backoff?"**

> `sleepCtx` returns the context error and we return immediately. No
> retry happens after cancellation.

**"How does this compose with the rate limiter and the cache?"**

> Order matters. In the demo I have it as
> `log -> retry -> ratelimit -> cache -> http.DefaultClient`. Cache
> nearest the wire so retries hit the wire and produce real
> observations. Rate limiter inside retry so a retry storm doesn't
> bypass the limit. Logging outermost so it sees the final outcome.

### What I would change

> The retry policy is hard-coded into the decorator constructor. For a
> real client, retry policy belongs on the request — different
> endpoints want different policies. I'd pass a `RetryPolicy` via the
> request context and let the decorator consult it.

## Deep-dive 3 — Dead-letter design

**File:** `internal/pipeline/types.go` and `pipeline.go`.
**Why this one:** smallest of the three, but the design choice is
where most pipelines I've reviewed get it wrong.

"Per-record errors don't halt the pipeline. They go to a dead-letter
sink that captures the original record, the failing stage name, the
error, and a timestamp. Setup errors are the only fatal kind."

1. **The `DeadLetter` struct** — "Full record, not just an ID. The
   record might be gone from the source by the time someone
   investigates — Kafka retention, in-memory queues, whatever. If we
   only kept the ID, the dead letter would be unreplayable."

2. **The `processOne` method** — "Records flow through stages
   serially. On any stage error, we write to the dead-letter sink and
   return. We do _not_ propagate the error up. The only way `Run`
   returns an error is via context cancellation or setup failure."

3. **The zero-Record drop** — "A stage can drop a record by returning
   a zero `Record` with no error. The deduplicator uses this. It's
   semantically different from an error: 'this was expected, don't
   dead-letter it.'"

4. **The teardown context** — "Teardown runs with a fresh context
   that has its own timeout, not the parent context. The parent might
   be cancelled — that's why we're tearing down — but cleanup itself
   needs to be allowed to run. Five-second budget, hard cap."

**"What if the dead-letter sink itself fails?"**

> Currently we ignore the error from `WriteDead`. That's a real gap.
> In production, a failing dead-letter sink should be alarming
> behaviour — it means we're losing records silently. The fix is
> either a fallback sink (file-on-disk) or surfacing it as a metric.
> I'd lean toward the metric-plus-alarm approach because the right
> escalation path depends on the operator.

**"Why serial per-record? Why not fan out across stages?"**

> Out of scope for the assessment. The `Stage` interface doesn't
> preclude a fan-out implementation later — it just isn't built. Adding
> it would require thinking about backpressure, ordering guarantees on
> the dead-letter sink, and graceful drain. None of that is in the
> brief.

**"What's the point of `Setup` and `Teardown` if stages are stateless
in practice?"**

> Real stages aren't stateless. A schema validator might compile a
> regex once. A transformer might open a database connection. The
> lifecycle hooks are where that resource management lives. The three
> stages I shipped happen not to need them, because the assessment
> rewards minimal stages — but the interface is the right shape for
> stages that do.

### What I would change

> The slice-backed source/sink/dead-letter implementations live in the
> production package because the demo binary uses them. A reviewer
> might reasonably ask why they're not in `_test.go` files. The honest
> answer is that putting them there would force the demo to declare its
> own duplicates, and I judged that worse than the cosmetic cost.
> A different person might judge it differently.

## "what would you change?"

1. **Replace `[]byte` body buffering in retry with `req.GetBody`-aware
   replay.** Memory cost matters at production scale. The current
   shape is right for predictability; a real client wants the
   streaming option.

1. **Track per-attempt timing in `NodeResult`.** Right now, attempt
   count is reported but per-attempt latency is lost. That's a real
   observability gap for debugging flaky upstream services.

1. **Add a fallback path or alarm signal for dead-letter sink
   failures.** Currently the error from `WriteDead` is ignored. In
   production that's a record-loss bug.
