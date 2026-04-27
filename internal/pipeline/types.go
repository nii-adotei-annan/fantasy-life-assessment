// Package pipeline implements a pluggable data processing pipeline.
//
// A Pipeline runs Records through a sequence of Stages. Each Stage has a
// Setup/Process/Teardown lifecycle. Per-record errors are isolated into a
// dead-letter sink rather than halting the pipeline. Cancellation propagates
// through context.
package pipeline

import (
	"context"
	"time"
)

// Record is the unit of data flowing through the pipeline.
//
// We use map[string]any rather than a typed struct because stages must be
// composable across different schemas. A schema-validation stage can enforce
// shape; downstream stages should not need recompilation when the shape
// changes. See DECISIONS.md, "Record shape: map vs generic."
type Record struct {
	ID   string
	Data map[string]any
}

// Stage is one step in the pipeline. Implementations must be safe for use
// from a single goroutine; the Pipeline does not call Process concurrently
// on the same Stage instance.
type Stage interface {
	// Name identifies the stage in dead-letter records and logs.
	Name() string

	// Setup is called once before any Process call. Use it to initialize
	// resources (DB connections, caches, compiled regexes). If Setup returns
	// an error, the pipeline aborts before processing any record.
	Setup(ctx context.Context) error

	// Process handles one record. Returning an error sends the record to the
	// dead-letter sink with this stage's name; the pipeline continues with
	// the next record. Returning (nil, nil) drops the record silently.
	Process(ctx context.Context, r Record) (Record, error)

	// Teardown is called once after the last Process call, regardless of
	// whether Process errored. Errors are logged but do not fail the run.
	Teardown(ctx context.Context) error
}

// DeadLetter captures a failed record with full context for auditability.
//
// The original record is preserved (not just an error string) so operators
// can replay or inspect the input that triggered the failure.
type DeadLetter struct {
	Record    Record
	Stage     string
	Err       error
	Timestamp time.Time
}

// Source produces records. Implementations should respect ctx cancellation
// and close the returned channel when the source is exhausted.
type Source interface {
	Records(ctx context.Context) (<-chan Record, error)
}

// Sink consumes successfully-processed records.
type Sink interface {
	Write(ctx context.Context, r Record) error
}

// DeadLetterSink consumes failed records.
type DeadLetterSink interface {
	WriteDead(ctx context.Context, dl DeadLetter) error
}
