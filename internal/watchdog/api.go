package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

type APIErrorCode string

const (
	APIErrorUnauthorized       APIErrorCode = "unauthorized"
	APIErrorPermissionDenied   APIErrorCode = "permission_denied"
	APIErrorNotFound           APIErrorCode = "not_found"
	APIErrorInvalidRequest     APIErrorCode = "invalid_request"
	APIErrorServiceUnavailable APIErrorCode = "service_unavailable"
)

type APIError struct {
	Code    APIErrorCode   `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type apiErrorResponse struct {
	Error APIError `json:"error"`
}

type AuthContext struct {
	TenantID ID   `json:"tenant_id"`
	UserID   ID   `json:"user_id"`
	RoleIDs  []ID `json:"role_ids"`
	Grants   []Permission
	IsAdmin  bool `json:"is_admin"`
}

type authContextKey struct{}

func ContextWithAuth(ctx context.Context, auth AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey{}, auth)
}

func AuthFromContext(ctx context.Context) (AuthContext, bool) {
	auth, ok := ctx.Value(authContextKey{}).(AuthContext)
	return auth, ok
}

type AuthContextAdapter func(*http.Request) (AuthContext, error)

func AuthMiddleware(adapter AuthContextAdapter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth, err := adapter(r)
			if err != nil {
				WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Unauthorized", nil)
				return
			}
			next.ServeHTTP(w, r.WithContext(ContextWithAuth(r.Context(), auth)))
		})
	}
}

type ResourceResolver func(*http.Request, AuthContext) (ResourceRef, error)

func RequirePermission(action Action, resolve ResourceResolver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth, ok := AuthFromContext(r.Context())
			if !ok {
				WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Unauthorized", nil)
				return
			}
			resource, err := resolve(r, auth)
			if err != nil {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
				return
			}
			if auth.IsAdmin {
				next.ServeHTTP(w, r)
				return
			}
			if !HasPermission(AccessRequest{
				TenantID: auth.TenantID,
				UserID:   auth.UserID,
				RoleIDs:  auth.RoleIDs,
				Action:   action,
				Resource: resource,
			}, auth.Grants) {
				WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func WriteAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func WriteAPIError(w http.ResponseWriter, status int, code APIErrorCode, message string, details map[string]any) {
	WriteAPIJSON(w, status, apiErrorResponse{Error: APIError{
		Code:    code,
		Message: message,
		Details: details,
	}})
}

func TenantResource(_ *http.Request, auth AuthContext) (ResourceRef, error) {
	if auth.TenantID == "" {
		return ResourceRef{}, errors.New("tenant id is required")
	}
	return ResourceRef{Type: ResourceTenant, ID: auth.TenantID}, nil
}
