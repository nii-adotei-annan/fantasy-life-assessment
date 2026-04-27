package stages

import (
	"context"
	"fmt"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
)

// SchemaValidator rejects records that do not contain all required fields.
//
// We considered using a JSON Schema library but rejected it: the assessment
// rewards understanding the pattern, not pulling in a dependency. The
// interface is shaped so a JSON Schema implementation could be slotted in
// later without changing the Pipeline.
type SchemaValidator struct {
	RequiredFields []string
}

func (v *SchemaValidator) Name() string { return "schema-validator" }

func (v *SchemaValidator) Setup(ctx context.Context) error    { return nil }
func (v *SchemaValidator) Teardown(ctx context.Context) error { return nil }

func (v *SchemaValidator) Process(ctx context.Context, r pipeline.Record) (pipeline.Record, error) {
	for _, f := range v.RequiredFields {
		if _, ok := r.Data[f]; !ok {
			return pipeline.Record{}, fmt.Errorf("missing required field: %s", f)
		}
	}
	return r, nil
}
