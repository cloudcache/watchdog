package watchdog

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const RequestIDHeader = "X-Request-ID"

type requestIDContextKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get(RequestIDHeader))
		if !validRequestID(requestID) {
			requestID = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, requestID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, requestID)))
	})
}

func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("-_.:", r) {
			continue
		}
		return false
	}
	return true
}
