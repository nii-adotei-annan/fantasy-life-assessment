package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
)

// Mailer is the dependency boundary for sending mail. We define our own
// minimal interface rather than importing a mail library: this keeps the
// jobs package free of vendor lock-in and makes tests trivial.
type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

// EmailJob sends an email. Recipient can be set in config or input.
type EmailJob struct {
	To      string
	Subject string
	Mailer  Mailer
}

func (j *EmailJob) Execute(ctx context.Context, input any) (any, error) {
	if j.Mailer == nil {
		return nil, errors.New("email job: mailer is nil")
	}
	body := ""
	to := j.To
	subj := j.Subject
	if m, ok := input.(map[string]any); ok {
		if v, ok := m["body"].(string); ok {
			body = v
		}
		if v, ok := m["to"].(string); ok && v != "" {
			to = v
		}
	}
	if to == "" {
		return nil, errors.New("email job: recipient is required")
	}
	if err := j.Mailer.Send(ctx, to, subj, body); err != nil {
		return nil, err
	}
	return map[string]any{"sent_to": to}, nil
}

func NewEmailFactory(m Mailer) workflow.JobFactory {
	return func(config map[string]any) (workflow.Job, error) {
		to, _ := config["to"].(string)
		subj, _ := config["subject"].(string)
		return &EmailJob{To: to, Subject: subj, Mailer: m}, nil
	}
}

// TransformJob applies a named transform to the input. Transforms are
// registered once at factory construction; nodes pick one by name. This
// is the equivalent of the pipeline's FieldTransformer.
type TransformJob struct {
	Name      string
	Transform func(any) (any, error)
}

func (j *TransformJob) Execute(ctx context.Context, input any) (any, error) {
	if j.Transform == nil {
		return nil, fmt.Errorf("transform job %q: transform is nil", j.Name)
	}
	return j.Transform(input)
}

// NewTransformFactory binds a set of named transforms. The factory selects
// one based on config["name"].
func NewTransformFactory(transforms map[string]func(any) (any, error)) workflow.JobFactory {
	// Defensive copy of the map so callers can't mutate it later. This is
	// the kind of small thing that prevents subtle test pollution.
	local := make(map[string]func(any) (any, error), len(transforms))
	for k, v := range transforms {
		local[k] = v
	}
	return func(config map[string]any) (workflow.Job, error) {
		name, _ := config["name"].(string)
		fn, ok := local[name]
		if !ok {
			return nil, fmt.Errorf("transform job: unknown transform %q", name)
		}
		return &TransformJob{Name: name, Transform: fn}, nil
	}
}
