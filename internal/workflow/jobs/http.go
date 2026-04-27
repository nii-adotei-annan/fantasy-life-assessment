package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/nii-adotei-annan/fantasy-life-assessment/internal/workflow"
)

// HTTPJob performs an HTTP GET. Real implementations would also handle
// methods/bodies/headers — kept narrow for the assessment.
//
// Why a struct field for the HTTP client rather than http.DefaultClient:
// tests inject a fake transport. A dependency-injected client also makes
// it trivial to plug in the Task 3 client middleware stack.
type HTTPJob struct {
	URL    string
	Client *http.Client
}

func (j *HTTPJob) Execute(ctx context.Context, input any) (any, error) {
	url := j.URL
	// Allow runtime override via input map. This is what makes one job
	// type usable for many nodes.
	if m, ok := input.(map[string]any); ok {
		if u, ok := m["url"].(string); ok && u != "" {
			url = u
		}
	}
	if url == "" {
		return nil, errors.New("http job: url is required")
	}
	client := j.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http job: status %d", resp.StatusCode)
	}
	return map[string]any{
		"status": resp.StatusCode,
		"body":   string(body),
	}, nil
}

// NewHTTPFactory returns a factory bound to a specific *http.Client. The
// client is shared across all HTTPJob instances built by this factory,
// which is what we want: connection pooling, shared rate limiter, etc.
func NewHTTPFactory(client *http.Client) workflow.JobFactory {
	return func(config map[string]any) (workflow.Job, error) {
		url, _ := config["url"].(string)
		return &HTTPJob{URL: url, Client: client}, nil
	}
}
