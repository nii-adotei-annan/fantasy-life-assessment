package server

import (
	"net/http"
	"time"
)

// Logger is the same minimal interface used by the client package — but
// duplicated here, NOT shared. Each task is independent. See DECISIONS.md.
type Logger interface {
	Logf(format string, args ...any)
}

// LoggerFunc adapts a function to Logger.
type LoggerFunc func(format string, args ...any)

func (f LoggerFunc) Logf(format string, args ...any) { f(format, args...) }

// statusRecorder captures the status code so the logging middleware can
// report it. We chose this over a Hijacker/Flusher-aware wrapper because
// the assessment doesn't require streaming — keep it minimal.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Write implicitly calls WriteHeader(200) if no status was set.
func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.status = http.StatusOK
		r.wrote = true
	}
	return r.ResponseWriter.Write(b)
}

// Logging emits one structured line per request with method, path, status,
// duration, and request ID (if available).
func Logging(log Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: 200}
			next.ServeHTTP(rec, r)
			log.Logf("server: id=%s method=%s path=%s status=%d dur=%s",
				RequestIDFromContext(r.Context()), r.Method, r.URL.Path, rec.status, time.Since(start))
		})
	}
}
