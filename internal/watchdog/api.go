package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	Code      APIErrorCode   `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

type apiErrorResponse struct {
	Error APIError `json:"error"`
}

type AuthContext struct {
	TenantID                ID           `json:"tenant_id"`
	UserID                  ID           `json:"user_id"`
	RoleIDs                 []ID         `json:"role_ids"`
	Grants                  []Permission `json:"grants"`
	IsAdmin                 bool         `json:"is_admin"`
	CanManageAddressLibrary bool         `json:"can_manage_address_library"`
	ExternalSubject         string       `json:"-"`
	AvailableTenants        []Tenant     `json:"-"`
}

type AuthAdapterError struct {
	Status  int
	Code    APIErrorCode
	Message string
}

func (e *AuthAdapterError) Error() string {
	return e.Message
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

// addressLibraryAuthAdapters expose one platform-global AddressSnap. Every
// authenticated user may read it; only a global administrator may mutate or
// publish it. ownerScope is retained only while the legacy repositories still
// require a non-empty scope key and is not an authorization boundary.
func addressLibraryAuthAdapters(auth AuthContextAdapter, ownerTenantID ID) (AuthContextAdapter, AuthContextAdapter) {
	if auth == nil || ownerTenantID == "" {
		return auth, auth
	}
	view := func(r *http.Request) (AuthContext, error) {
		selected, err := auth(r)
		if err != nil {
			return AuthContext{}, err
		}
		selected.TenantID = ownerTenantID
		selected.IsAdmin = false
		selected.Grants = append(append([]Permission(nil), selected.Grants...), Permission{
			TenantID: ownerTenantID, SubjectType: SubjectUser, SubjectID: selected.UserID,
			ResourceType: ResourceTenant, ResourceID: ownerTenantID, Actions: []Action{ActionView},
		})
		return selected, nil
	}
	admin := func(r *http.Request) (AuthContext, error) {
		selected, err := auth(r)
		if err != nil {
			return AuthContext{}, err
		}
		if !selected.IsAdmin {
			return AuthContext{}, &AuthAdapterError{
				Status:  http.StatusForbidden,
				Code:    APIErrorPermissionDenied,
				Message: "Platform administrator required",
			}
		}
		selected.TenantID = ownerTenantID
		return selected, nil
	}
	return view, admin
}

func AuthMiddleware(adapter AuthContextAdapter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if adapter == nil {
				WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Authentication is not configured", nil)
				return
			}
			auth, err := adapter(r)
			if err != nil {
				var authErr *AuthAdapterError
				if errors.As(err, &authErr) {
					WriteAPIError(w, authErr.Status, authErr.Code, authErr.Message, nil)
					return
				}
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

// WriteAPIJSONRaw writes an already-encoded JSON document as the response body.
// It avoids a decode+re-encode when a downstream layer (e.g. a query provider)
// has already produced the exact JSON to return. The caller must ensure data is
// valid JSON.
func WriteAPIJSONRaw(w http.ResponseWriter, status int, data json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// ensureDashboardJSONEOF is the historical name of the shared one-document
// request-body guard used by the remaining legacy handlers. It is not owned by
// the retired dashboard API.
func ensureDashboardJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return err
	}
	return nil
}

func WriteAPIError(w http.ResponseWriter, status int, code APIErrorCode, message string, details map[string]any) {
	WriteAPIJSON(w, status, apiErrorResponse{Error: APIError{
		Code:    code,
		Message: message,
		// Transient conditions a client may retry verbatim; conflicts and
		// validation failures need a changed request first (platform §15.3).
		Retryable: status == http.StatusTooManyRequests || status >= 500,
		Details:   details,
	}})
}

func TenantResource(_ *http.Request, auth AuthContext) (ResourceRef, error) {
	if auth.TenantID == "" {
		return ResourceRef{}, errors.New("tenant id is required")
	}
	return ResourceRef{Type: ResourceTenant, ID: auth.TenantID}, nil
}
