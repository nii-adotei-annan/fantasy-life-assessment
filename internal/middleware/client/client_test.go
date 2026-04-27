package client

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newContextWithImmediateCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// mockDoer is a controllable Doer for layer-isolation tests.
type mockDoer struct {
	mu     sync.Mutex
	calls  int
	respFn func(int, *http.Request) (*http.Response, error) // attempt is 1-based
}

func (m *mockDoer) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.calls++
	n := m.calls
	fn := m.respFn
	m.mu.Unlock()
	return fn(n, req)
}

func okResp(body string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func statusResp(code int) *http.Response {
	return &http.Response{
		StatusCode: code, Status: http.StatusText(code),
		Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")),
	}
}

// --- Rate limiter ---

func TestRateLimit_AllowsBurstUpToCapacity(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return okResp(""), nil
	}}
	// 3 capacity, 0 refill: should allow 3 immediate calls, then block.
	rl := NewRateLimited(mock, 3, 0).withClock(func() time.Time { return time.Unix(0, 0) })

	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest("GET", "http://x/", nil)
		if _, err := rl.Do(req); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if mock.calls != 3 {
		t.Fatalf("calls = %d, want 3", mock.calls)
	}
}

func TestRateLimit_ContextCancelDuringWait(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return okResp(""), nil
	}}
	// Capacity 1, very slow refill: second call must wait, but ctx is cancelled.
	rl := NewRateLimited(mock, 1, 0.0001)
	req1, _ := http.NewRequest("GET", "http://x/", nil)
	if _, err := rl.Do(req1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := newContextWithImmediateCancel()
	cancel()
	req2, _ := http.NewRequestWithContext(ctx, "GET", "http://x/", nil)
	_, err := rl.Do(req2)
	if err == nil {
		t.Fatal("expected ctx error")
	}
}

// --- Retry ---

func TestRetry_RetriesOn5xxAndSucceeds(t *testing.T) {
	mock := &mockDoer{respFn: func(n int, _ *http.Request) (*http.Response, error) {
		if n < 3 {
			return statusResp(503), nil
		}
		return okResp("ok"), nil
	}}
	r := NewRetried(mock, 3, time.Microsecond, time.Microsecond).
		withRng(rand.New(rand.NewSource(1)))
	req, _ := http.NewRequest("GET", "http://x/", nil)
	resp, err := r.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if mock.calls != 3 {
		t.Fatalf("calls = %d, want 3", mock.calls)
	}
}

func TestRetry_DoesNotRetryOn4xx(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return statusResp(404), nil
	}}
	r := NewRetried(mock, 3, time.Microsecond, time.Microsecond).
		withRng(rand.New(rand.NewSource(1)))
	req, _ := http.NewRequest("GET", "http://x/", nil)
	resp, err := r.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if mock.calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on 4xx)", mock.calls)
	}
}

func TestRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	}}
	r := NewRetried(mock, 2, time.Microsecond, time.Microsecond).
		withRng(rand.New(rand.NewSource(1)))
	req, _ := http.NewRequest("GET", "http://x/", nil)
	_, err := r.Do(req)
	if err == nil {
		t.Fatal("expected error")
	}
	if mock.calls != 3 { // 1 original + 2 retries
		t.Fatalf("calls = %d, want 3", mock.calls)
	}
}

// --- Cache ---

func TestCache_HitWithinTTL(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return okResp("payload"), nil
	}}
	now := time.Unix(0, 0)
	c := NewCached(mock, time.Minute).withClock(func() time.Time { return now })

	req, _ := http.NewRequest("GET", "http://x/y", nil)
	resp1, _ := c.Do(req)
	b1, _ := io.ReadAll(resp1.Body)
	req2, _ := http.NewRequest("GET", "http://x/y", nil)
	resp2, _ := c.Do(req2)
	b2, _ := io.ReadAll(resp2.Body)

	if string(b1) != "payload" || string(b2) != "payload" {
		t.Fatalf("bodies: %q %q", b1, b2)
	}
	if mock.calls != 1 {
		t.Fatalf("calls = %d, want 1 (second should be cached)", mock.calls)
	}
}

func TestCache_DoesNotCacheNonGET(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return okResp("x"), nil
	}}
	c := NewCached(mock, time.Minute)
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", "http://x/", nil)
		_, _ = c.Do(req)
	}
	if mock.calls != 2 {
		t.Fatalf("calls = %d, want 2", mock.calls)
	}
}

func TestCache_DoesNotCache5xx(t *testing.T) {
	mock := &mockDoer{respFn: func(_ int, _ *http.Request) (*http.Response, error) {
		return statusResp(500), nil
	}}
	c := NewCached(mock, time.Minute)
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("GET", "http://x/", nil)
		_, _ = c.Do(req)
	}
	if mock.calls != 2 {
		t.Fatalf("calls = %d, want 2 (5xx should not be cached)", mock.calls)
	}
}

// --- Logging ---

func TestLogging_LogsBothSuccessAndFailure(t *testing.T) {
	var lines []string
	var mu sync.Mutex
	logger := LoggerFunc(func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, format)
	})
	mock := &mockDoer{respFn: func(n int, _ *http.Request) (*http.Response, error) {
		if n == 1 {
			return okResp(""), nil
		}
		return nil, errors.New("boom")
	}}
	l := NewLogged(mock, logger)
	req, _ := http.NewRequest("GET", "http://x/", nil)
	_, _ = l.Do(req)
	_, _ = l.Do(req)
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
}

// --- Composition ---

func TestComposed_LogRetryRateLimitCacheBase(t *testing.T) {
	// End-to-end: real httptest server, full stack on top.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := hits.Add(1)
		if n < 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	logged := []string{}
	var mu sync.Mutex
	log := LoggerFunc(func(f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		logged = append(logged, f)
	})

	// Layered: log -> retry -> ratelimit -> cache -> http.DefaultClient.
	// Order matters: cache nearest to the wire so retries hit the wire.
	var base Doer = http.DefaultClient
	base = NewCached(base, time.Minute)
	base = NewRateLimited(base, 10, 100)
	base = NewRetried(base, 3, time.Microsecond, time.Microsecond).
		withRng(rand.New(rand.NewSource(1)))
	base = NewLogged(base, log)

	req, _ := http.NewRequest("GET", srv.URL, nil)
	resp, err := base.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("body = %q", body)
	}
	if hits.Load() < 2 {
		t.Fatalf("hits = %d (should have retried 503)", hits.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logged) == 0 {
		t.Fatal("logger never called")
	}
}
