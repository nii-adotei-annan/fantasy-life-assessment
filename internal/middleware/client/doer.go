// Package client provides a composable HTTP client.
//
// Each cross-cutting concern (rate limiting, retry, caching, logging) is
// implemented as a decorator around the Doer interface. They compose by
// wrapping; each layer is independently testable against a mock Doer.
package client

import "net/http"

// Doer is the minimal HTTP doer interface. Both *http.Client and any of
// our decorators implement it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// DoerFunc adapts a function to Doer.
type DoerFunc func(*http.Request) (*http.Response, error)

func (f DoerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }
