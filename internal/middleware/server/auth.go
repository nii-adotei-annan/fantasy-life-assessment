package server

import (
	"context"
	"net/http"
	"strings"
)

const authPrincipalKey ctxKey = "auth-principal"

// Authenticator validates a token and returns a principal (e.g. user ID)
// or an error. Callers wire in their preferred backend (JWT, opaque
// token lookup, etc.) by implementing this interface.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (principal string, err error)
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(ctx context.Context, token string) (string, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, t string) (string, error) {
	return f(ctx, t)
}

// Auth middleware extracts a bearer token, validates it, and injects the
// principal into the request context. On failure it returns 401.
func Auth(a Authenticator) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if !strings.HasPrefix(h, "Bearer ") {
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(h, "Bearer ")
			principal, err := a.Authenticate(r.Context(), token)
			if err != nil || principal == "" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), authPrincipalKey, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// PrincipalFromContext returns the authenticated principal, or "" if none.
func PrincipalFromContext(ctx context.Context) string {
	v, _ := ctx.Value(authPrincipalKey).(string)
	return v
}
