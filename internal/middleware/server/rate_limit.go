package server

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// PerClientRateLimit limits each client (keyed by remote IP, or by a
// caller-supplied key function) to N requests per window.
//
// We use a fixed-window counter rather than a true token bucket because
// the implementation is simpler and the assessment scope doesn't justify
// per-client bucket state. Both shapes are valid; the trade-off is that
// fixed windows allow up to 2*N traffic at the boundary between windows.
// See DECISIONS.md.
type clientWindow struct {
	count       int
	windowStart time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	clients map[string]*clientWindow
	limit   int
	window  time.Duration
	keyFunc func(*http.Request) string
	now     func() time.Time
}

func PerClientRateLimit(limit int, window time.Duration, keyFunc func(*http.Request) string) Middleware {
	if keyFunc == nil {
		keyFunc = clientIPKey
	}
	rl := &rateLimiter{
		clients: make(map[string]*clientWindow),
		limit:   limit,
		window:  window,
		keyFunc: keyFunc,
		now:     time.Now,
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !rl.allow(rl.keyFunc(r)) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (r *rateLimiter) allow(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	cw, ok := r.clients[key]
	if !ok || now.Sub(cw.windowStart) >= r.window {
		r.clients[key] = &clientWindow{count: 1, windowStart: now}
		return true
	}
	if cw.count >= r.limit {
		return false
	}
	cw.count++
	return true
}

func clientIPKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
