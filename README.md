# Fantasy-Life-assessment

Three independent Go systems in one repository:

- `internal/pipeline` — Task 1: pluggable data processing pipeline
- `internal/workflow` — Task 2: workflow orchestrator engine
- `internal/middleware` — Task 3: layered HTTP client and server middleware

## Layout

```
cmd/demo/                  Integration demo wiring the three systems together
internal/pipeline/         Task 1
internal/workflow/         Task 2
internal/middleware/       Task 3
tests/integration/         End-to-end tests across the demo binary
DECISIONS.md               Architectural decisions and rejected alternatives
AGENT.md                   Prompts/instructions used for AI scaffolding
```

## Independence

Each `internal/<task>` package compiles and tests on its own. No task imports
another. The demo binary wires them through their public interfaces only.

## Running

```
go test ./...
go run ./cmd/demo
```

## Why three packages, not one platform

See DECISIONS.md, "Independence vs composition tradeoff." The brief asks for
three tasks; treating them as one system would have created coupling that
worked against the testability requirement. The demo proves they _can_
compose without requiring that they do.
