package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
}

// --- Chain ---

func TestChain_OrderIsOuterToInner(t *testing.T) {
	var trace []string
	makeMW := func(label string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				trace = append(trace, "before-"+label)
				next.ServeHTTP(w, r)
				trace = append(trace, "after-"+label)
			})
		}
	}
	h := Chain(makeMW("A"), makeMW("B"), makeMW("C"))(okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(w, r)
	want := []string{"before-A", "before-B", "before-C", "after-C", "after-B", "after-A"}
	if strings.Join(trace, ",") != strings.Join(want, ",") {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
}

// --- RequestID ---

func TestRequestID_GeneratesIDWhenAbsent(t *testing.T) {
	var seen string
	h := RequestID(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if seen == "" {
		t.Fatal("no request ID in context")
	}
	if w.Header().Get("X-Request-ID") != seen {
		t.Fatalf("header mismatch: %q vs %q", w.Header().Get("X-Request-ID"), seen)
	}
}

func TestRequestID_RespectsIncomingHeader(t *testing.T) {
	var seen string
	h := RequestID(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-ID", "fixed-id")
	h.ServeHTTP(w, r)
	if seen != "fixed-id" {
		t.Fatalf("got %q, want fixed-id", seen)
	}
}

// --- Logging ---

func TestLogging_RecordsStatusAndID(t *testing.T) {
	var formats []string
	var args [][]any
	var mu sync.Mutex
	log := LoggerFunc(func(f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		formats = append(formats, f)
		args = append(args, a)
	})
	// Compose RequestID + Logging so the log line includes the ID.
	h := Chain(RequestID(nil), Logging(log))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(418)
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	mu.Lock()
	defer mu.Unlock()
	if len(formats) != 1 {
		t.Fatalf("lines = %d", len(formats))
	}
	if !strings.Contains(formats[0], "status=") || !strings.Contains(formats[0], "id=") {
		t.Fatalf("missing fields in format: %q", formats[0])
	}
	// Args should include the status (418) somewhere.
	found := false
	for _, a := range args[0] {
		if i, ok := a.(int); ok && i == 418 {
			found = true
		}
	}
	if !found {
		t.Fatalf("status 418 not in args: %v", args[0])
	}
}

// --- Rate limit ---

func TestRateLimit_BlocksAfterLimit(t *testing.T) {
	mw := PerClientRateLimit(2, time.Minute, func(_ *http.Request) string { return "ip" })
	h := mw(okHandler())
	hits := 0
	for i := 0; i < 3; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
		if w.Code == 200 {
			hits++
		}
	}
	if hits != 2 {
		t.Fatalf("hits = %d, want 2", hits)
	}
}

func TestRateLimit_DifferentKeysIndependent(t *testing.T) {
	keyHeader := "X-Test-Key"
	mw := PerClientRateLimit(1, time.Minute, func(r *http.Request) string {
		return r.Header.Get(keyHeader)
	})
	h := mw(okHandler())
	for _, k := range []string{"a", "b"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(keyHeader, k)
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("key %s: code = %d", k, w.Code)
		}
	}
}

// --- Auth ---

func TestAuth_RejectsMissingBearer(t *testing.T) {
	h := Auth(AuthenticatorFunc(func(_ context.Context, _ string) (string, error) {
		return "user", nil
	}))(okHandler())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", w.Code)
	}
}

func TestAuth_RejectsInvalidToken(t *testing.T) {
	h := Auth(AuthenticatorFunc(func(_ context.Context, _ string) (string, error) {
		return "", errors.New("nope")
	}))(okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer x")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", w.Code)
	}
}

func TestAuth_AcceptsValidTokenAndInjectsPrincipal(t *testing.T) {
	var principal string
	h := Auth(AuthenticatorFunc(func(_ context.Context, tok string) (string, error) {
		if tok == "secret" {
			return "alice", nil
		}
		return "", errors.New("no")
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal = PrincipalFromContext(r.Context())
		w.WriteHeader(200)
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(w, r)
	if w.Code != 200 || principal != "alice" {
		t.Fatalf("code=%d principal=%q", w.Code, principal)
	}
}
