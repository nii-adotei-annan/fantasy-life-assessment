package client

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"
)

// Retried wraps a Doer with retry on transient failures.
//
// Retried errors include network errors and 5xx responses. 4xx is NOT
// retried — those are caller errors and retrying won't help.
//
// Backoff is exponential with jitter. The jitter is "full jitter" per
// AWS's published guidance, which avoids thundering-herd patterns when
// many clients retry simultaneously.
type Retried struct {
	next       Doer
	maxRetries int
	base       time.Duration
	max        time.Duration
	rng        *rand.Rand
	now        func() time.Time
}

func NewRetried(next Doer, maxRetries int, base, max time.Duration) *Retried {
	return &Retried{
		next:       next,
		maxRetries: maxRetries,
		base:       base,
		max:        max,
		// Seeded RNG: deterministic in tests via withRng, real-world via time.
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		now: time.Now,
	}
}

func (r *Retried) withRng(rng *rand.Rand) *Retried { r.rng = rng; return r }

func (r *Retried) Do(req *http.Request) (*http.Response, error) {
	// We must be able to replay the request body across attempts. Read it
	// once into memory; on each attempt, reset Body to a fresh reader.
	// This is a trade-off: it costs memory for retried requests, but
	// without it, retries silently send empty bodies on the second attempt.
	var bodyBytes []byte
	if req.Body != nil && req.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
	}

	var (
		resp    *http.Response
		lastErr error
	)
	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
		resp, lastErr = r.next.Do(req)
		if !shouldRetry(resp, lastErr) {
			return resp, lastErr
		}
		// On the final attempt, return the failed response as-is so the
		// caller can inspect the body. Don't drain.
		if attempt == r.maxRetries {
			break
		}
		// Drain and close the failed response body to free the connection
		// before retrying. Forgetting this leaks connections.
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		delay := r.computeDelay(attempt)
		if err := sleepCtx(req.Context(), delay); err != nil {
			return nil, err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return resp, nil
}

func shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return true
	}
	return resp.StatusCode >= 500
}

// computeDelay returns base * 2^attempt with full jitter, capped at max.
// attempt is 0-based: attempt 0 is the second attempt overall (first retry).
func (r *Retried) computeDelay(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt))
	d := time.Duration(float64(r.base) * exp)
	if d > r.max {
		d = r.max
	}
	if d <= 0 {
		return 0
	}
	// Full jitter: random in [0, d).
	return time.Duration(r.rng.Int63n(int64(d)))
}

// ErrRetryExhausted is returned when retries run out without success.
// (Currently unused — we return the last error directly. Reserved for
// future use if callers need to distinguish exhaustion from other errors.)
var ErrRetryExhausted = errors.New("client: retries exhausted")
