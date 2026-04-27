package stages

import (
	"context"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
)

// FieldTransformer applies a map of field-name -> transform to each record.
//
// Why a function map and not a struct of named transforms: callers configure
// transforms at construction, so the orchestrator never knows which fields
// or transforms exist. Adding a new transform requires zero changes here.
type FieldTransformer struct {
	Transforms map[string]func(any) (any, error)
}

func (t *FieldTransformer) Name() string                       { return "field-transformer" }
func (t *FieldTransformer) Setup(ctx context.Context) error    { return nil }
func (t *FieldTransformer) Teardown(ctx context.Context) error { return nil }

func (t *FieldTransformer) Process(ctx context.Context, r pipeline.Record) (pipeline.Record, error) {
	// Copy data to avoid mutating the input record. This matters when the
	// pipeline is later run concurrently or when the source caches records.
	out := pipeline.Record{ID: r.ID, Data: make(map[string]any, len(r.Data))}
	for k, v := range r.Data {
		out.Data[k] = v
	}
	for field, fn := range t.Transforms {
		if v, ok := out.Data[field]; ok {
			nv, err := fn(v)
			if err != nil {
				return pipeline.Record{}, err
			}
			out.Data[field] = nv
		}
	}
	return out, nil
}
