// Package server provides composable HTTP server middleware.
//
// Each middleware is a function that wraps an http.Handler. The Chain
// helper composes them in order: Chain(A, B, C)(h) == A(B(C(h))).
// This avoids the deeply nested wrapping ("h := A(B(C(handler)))") that
// becomes unreadable past two layers.
package server

import "net/http"

// Middleware is the standard "wraps an http.Handler" shape.
type Middleware func(http.Handler) http.Handler

// Chain composes middlewares left-to-right: outermost first.
//
// Chain(Logging, RateLimit, Auth)(h) processes a request as:
//
//	Logging  -> RateLimit -> Auth -> h
//
// We chose this order over right-to-left because it reads as the request
// flow ("first log, then rate limit, then authenticate, then handle"),
// which matches how reviewers will reason about it.
func Chain(mws ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		// Apply in reverse so the first middleware is outermost.
		h := final
		for i := len(mws) - 1; i >= 0; i-- {
			h = mws[i](h)
		}
		return h
	}
}
