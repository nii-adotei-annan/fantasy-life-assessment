package client

import (
	"net/http"
	"time"
)

// Logger is the minimal logging interface. Defining it here (rather than
// importing one) keeps the client package free of logging dependencies.
// Callers wire in their preferred backend by implementing Logger.
type Logger interface {
	Logf(format string, args ...any)
}

// LoggerFunc adapts a function to Logger.
type LoggerFunc func(format string, args ...any)

func (f LoggerFunc) Logf(format string, args ...any) { f(format, args...) }

// Logged wraps a Doer to log request and response metadata.
type Logged struct {
	next Doer
	log  Logger
	now  func() time.Time
}

func NewLogged(next Doer, log Logger) *Logged {
	return &Logged{next: next, log: log, now: time.Now}
}

func (l *Logged) Do(req *http.Request) (*http.Response, error) {
	start := l.now()
	resp, err := l.next.Do(req)
	dur := l.now().Sub(start)
	if err != nil {
		l.log.Logf("client: %s %s err=%v dur=%s", req.Method, req.URL, err, dur)
		return resp, err
	}
	l.log.Logf("client: %s %s status=%d dur=%s", req.Method, req.URL, resp.StatusCode, dur)
	return resp, nil
}
