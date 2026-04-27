package workflow

import (
	"context"
	"fmt"
)

func dispatch(ctx context.Context, jobType string, config map[string]any, input any) (any, error) {
	switch jobType {
	case "http":
		return executeHTTP(ctx, config, input)
	case "email":
		return executeEmail(ctx, config, input)
	case "transform":
		return executeTransform(ctx, config, input)
	default:
		return nil, fmt.Errorf("workflow: unknown job type %q", jobType)
	}
}

func executeHTTP(ctx context.Context, config map[string]any, input any) (any, error) {
	return nil, nil
}

func executeEmail(ctx context.Context, config map[string]any, input any) (any, error) {
	return nil, nil
}

func executeTransform(ctx context.Context, config map[string]any, input any) (any, error) {
	return nil, nil
}
