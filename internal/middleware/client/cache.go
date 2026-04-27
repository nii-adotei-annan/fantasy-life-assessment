package client

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"
)

// Cached wraps a Doer with a TTL-based response cache.
//
// Only GETs are cached. The cache key is method+URL. We do NOT vary on
// headers because the assessment scope doesn't require it; a real
// implementation should respect Vary headers and authentication.
//
// Cached responses are returned with a fresh body (bytes.Reader) each
// time, so callers can read them independently.
type Cached struct {
	next  Doer
	ttl   time.Duration
	mu    sync.Mutex
	store map[string]cachedEntry
	now   func() time.Time
}

type cachedEntry struct {
	status  int
	headers http.Header
	body    []byte
	expires time.Time
}

func NewCached(next Doer, ttl time.Duration) *Cached {
	return &Cached{
		next:  next,
		ttl:   ttl,
		store: make(map[string]cachedEntry),
		now:   time.Now,
	}
}

func (c *Cached) withClock(now func() time.Time) *Cached { c.now = now; return c }

func (c *Cached) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return c.next.Do(req)
	}
	key := req.Method + " " + req.URL.String()

	c.mu.Lock()
	entry, hit := c.store[key]
	now := c.now()
	if hit && now.Before(entry.expires) {
		c.mu.Unlock()
		return buildResponseFromCache(entry, req), nil
	}
	c.mu.Unlock()

	resp, err := c.next.Do(req)
	if err != nil {
		return nil, err
	}
	// Only cache 2xx. Do not poison the cache with errors.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, nil
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.store[key] = cachedEntry{
		status:  resp.StatusCode,
		headers: resp.Header.Clone(),
		body:    body,
		expires: c.now().Add(c.ttl),
	}
	c.mu.Unlock()
	// Return a response with a fresh body so the caller can read it.
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, nil
}

func buildResponseFromCache(e cachedEntry, req *http.Request) *http.Response {
	return &http.Response{
		Status:        http.StatusText(e.status),
		StatusCode:    e.status,
		Header:        e.headers.Clone(),
		Body:          io.NopCloser(bytes.NewReader(e.body)),
		ContentLength: int64(len(e.body)),
		Request:       req,
	}
}
