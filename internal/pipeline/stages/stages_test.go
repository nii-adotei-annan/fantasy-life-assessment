package stages

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/pipeline"
)

func TestSchemaValidator_PassesWhenAllFieldsPresent(t *testing.T) {
	v := &SchemaValidator{RequiredFields: []string{"a", "b"}}
	r := pipeline.Record{ID: "1", Data: map[string]any{"a": 1, "b": 2}}
	out, err := v.Process(context.Background(), r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.ID != "1" {
		t.Fatalf("id changed: %q", out.ID)
	}
}

func TestSchemaValidator_FailsWhenFieldMissing(t *testing.T) {
	v := &SchemaValidator{RequiredFields: []string{"a", "b"}}
	r := pipeline.Record{ID: "1", Data: map[string]any{"a": 1}}
	_, err := v.Process(context.Background(), r)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "b") {
		t.Fatalf("error should name missing field, got: %v", err)
	}
}

func TestFieldTransformer_AppliesAndDoesNotMutateInput(t *testing.T) {
	tr := &FieldTransformer{Transforms: map[string]func(any) (any, error){
		"name": func(v any) (any, error) {
			s, ok := v.(string)
			if !ok {
				return nil, errors.New("not a string")
			}
			return strings.ToUpper(s), nil
		},
	}}
	in := pipeline.Record{ID: "1", Data: map[string]any{"name": "ada"}}
	out, err := tr.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.Data["name"] != "ADA" {
		t.Fatalf("transform did not apply: %v", out.Data["name"])
	}
	// Critical: the input must not be mutated. Without a defensive copy,
	// upstream sources caching records would see corrupted state.
	if in.Data["name"] != "ada" {
		t.Fatalf("input mutated: %v", in.Data["name"])
	}
}

func TestFieldTransformer_ReturnsErrorWhenTransformFails(t *testing.T) {
	tr := &FieldTransformer{Transforms: map[string]func(any) (any, error){
		"x": func(v any) (any, error) { return nil, errors.New("nope") },
	}}
	_, err := tr.Process(context.Background(), pipeline.Record{ID: "1", Data: map[string]any{"x": 1}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeduplicator_DropsDuplicateIDs(t *testing.T) {
	d := NewDeduplicator()
	_ = d.Setup(context.Background())
	r1, err := d.Process(context.Background(), pipeline.Record{ID: "x"})
	if err != nil || r1.ID != "x" {
		t.Fatalf("first: r=%v err=%v", r1, err)
	}
	r2, err := d.Process(context.Background(), pipeline.Record{ID: "x"})
	if err != nil {
		t.Fatalf("dup err: %v", err)
	}
	if r2.ID != "" {
		t.Fatalf("dup not dropped: %v", r2)
	}
}
