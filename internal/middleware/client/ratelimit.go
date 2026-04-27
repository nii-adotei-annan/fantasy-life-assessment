package client

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// tokenBucket is a simple token-bucket limiter. A request takes one token;
// if none is available, Wait blocks (respecting ctx) until one is.
//
// We considered using golang.org/x/time/rate but rolled our own to avoid
// the dependency for the assessment scope. The interface is the same
// shape so it can be swapped without changing call sites.
type tokenBucket struct {
	mu       sync.Mutex
	capacity int
	tokens   float64
	rate     float64 // tokens per second
	last     time.Time
	now      func() time.Time
}

func newTokenBucket(capacity int, ratePerSec float64, now func() time.Time) *tokenBucket {
	if now == nil {
		now = time.Now
	}
	return &tokenBucket{
		capacity: capacity,
		tokens:   float64(capacity),
		rate:     ratePerSec,
		last:     now(),
		now:      now,
	}
}

// reserve returns the duration the caller must wait before consuming a token.
// If a token is immediately available, returns 0 and consumes it.
func (b *tokenBucket) reserve() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(float64(b.capacity), b.tokens+elapsed*b.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return 0
	}
	missing := 1 - b.tokens
	wait := time.Duration(missing / b.rate * float64(time.Second))
	b.tokens = 0
	return wait
}

// RateLimited wraps a Doer with a token-bucket rate limit. Each Do call
// consumes one token; if none is available, Do blocks (respecting the
// request's context) until one is.
type RateLimited struct {
	next   Doer
	bucket *tokenBucket
}

func NewRateLimited(next Doer, capacity int, ratePerSec float64) *RateLimited {
	return &RateLimited{next: next, bucket: newTokenBucket(capacity, ratePerSec, nil)}
}

// withClock is for tests to inject a deterministic clock.
func (r *RateLimited) withClock(now func() time.Time) *RateLimited {
	r.bucket.now = now
	r.bucket.last = now()
	return r
}

func (r *RateLimited) Do(req *http.Request) (*http.Response, error) {
	wait := r.bucket.reserve()
	if wait > 0 {
		if err := sleepCtx(req.Context(), wait); err != nil {
			return nil, err
		}
	}
	return r.next.Do(req)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if ctx == nil {
		time.Sleep(d)
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
