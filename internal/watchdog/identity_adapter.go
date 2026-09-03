package watchdog

import (
	"errors"
	"net/http"
	"strings"
)

const TenantHeader = "X-Watchdog-Tenant-ID"

type ExternalIdentity struct {
	Provider string
	Subject  string
}

type ExternalIdentityAuthenticator func(*http.Request) (ExternalIdentity, error)

func NewIdentityAuthContextAdapter(authenticate ExternalIdentityAuthenticator, repo IdentityProjectionRepository) AuthContextAdapter {
	return func(r *http.Request) (AuthContext, error) {
		identity, projections, active, err := loadActiveIdentityProjections(r, authenticate, repo)
		if err != nil {
			return AuthContext{}, err
		}

		selectedTenant := ID(strings.TrimSpace(r.Header.Get(TenantHeader)))
		if selectedTenant == "" && len(active) > 1 {
			return AuthContext{}, authAdapterError(http.StatusBadRequest, APIErrorInvalidRequest, TenantHeader+" is required for a multi-tenant identity")
		}
		selected := active[0]
		if selectedTenant != "" {
			found := false
			for _, projection := range projections {
				if projection.Tenant.ID == selectedTenant {
					selected = projection
					found = true
					break
				}
			}
			if !found {
				return AuthContext{}, authAdapterError(http.StatusForbidden, APIErrorPermissionDenied, "Tenant access denied")
			}
		}
		if !strings.EqualFold(selected.User.Status, "active") || !strings.EqualFold(selected.Tenant.Status, "active") {
			return AuthContext{}, authAdapterError(http.StatusForbidden, APIErrorPermissionDenied, "Identity or tenant is disabled")
		}

		roleIDs, err := repo.ListUserRoleIDs(r.Context(), selected.Tenant.ID, selected.User.ID)
		if err != nil {
			return AuthContext{}, authAdapterError(http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Authorization data is unavailable")
		}
		grants, err := repo.ListPermissionsForUser(r.Context(), selected.Tenant.ID, selected.User.ID)
		if err != nil {
			return AuthContext{}, authAdapterError(http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Authorization data is unavailable")
		}
		isAdmin, err := repo.IsUserTenantAdmin(r.Context(), selected.Tenant.ID, selected.User.ID)
		if err != nil {
			return AuthContext{}, authAdapterError(http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Authorization data is unavailable")
		}
		available := make([]Tenant, 0, len(active))
		for _, projection := range active {
			available = append(available, projection.Tenant)
		}
		return AuthContext{
			TenantID:         selected.Tenant.ID,
			UserID:           selected.User.ID,
			RoleIDs:          roleIDs,
			Grants:           grants,
			IsAdmin:          isAdmin,
			ExternalSubject:  identity.Subject,
			AvailableTenants: available,
		}, nil
	}
}

// NewIdentityTenantDiscoveryAdapter authenticates the external subject and
// returns only its active tenant memberships. It intentionally does not select
// a tenant or load grants, so a multi-tenant user can bootstrap the tenant
// selector without weakening authorization on any tenant-scoped endpoint.
func NewIdentityTenantDiscoveryAdapter(authenticate ExternalIdentityAuthenticator, repo IdentityProjectionRepository) AuthContextAdapter {
	return func(r *http.Request) (AuthContext, error) {
		identity, _, active, err := loadActiveIdentityProjections(r, authenticate, repo)
		if err != nil {
			return AuthContext{}, err
		}
		available := make([]Tenant, 0, len(active))
		for _, projection := range active {
			available = append(available, projection.Tenant)
		}
		return AuthContext{
			ExternalSubject:  identity.Subject,
			AvailableTenants: available,
		}, nil
	}
}

func loadActiveIdentityProjections(r *http.Request, authenticate ExternalIdentityAuthenticator, repo IdentityProjectionRepository) (ExternalIdentity, []IdentityProjection, []IdentityProjection, error) {
	if authenticate == nil || repo == nil {
		return ExternalIdentity{}, nil, nil, authAdapterError(http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Authentication is not configured")
	}
	identity, err := authenticate(r)
	if err != nil || strings.TrimSpace(identity.Provider) == "" || strings.TrimSpace(identity.Subject) == "" {
		return ExternalIdentity{}, nil, nil, authAdapterError(http.StatusUnauthorized, APIErrorUnauthorized, "Unauthorized")
	}
	projections, err := repo.ListIdentityProjections(r.Context(), identity.Provider, identity.Subject)
	if err != nil {
		return ExternalIdentity{}, nil, nil, authAdapterError(http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Identity projection is unavailable")
	}
	if len(projections) == 0 {
		return ExternalIdentity{}, nil, nil, authAdapterError(http.StatusForbidden, APIErrorPermissionDenied, "Identity is not provisioned")
	}
	active := make([]IdentityProjection, 0, len(projections))
	for _, projection := range projections {
		if strings.EqualFold(projection.User.Status, "active") && strings.EqualFold(projection.Tenant.Status, "active") {
			active = append(active, projection)
		}
	}
	if len(active) == 0 {
		return ExternalIdentity{}, nil, nil, authAdapterError(http.StatusForbidden, APIErrorPermissionDenied, "Identity or tenant is disabled")
	}
	return identity, projections, active, nil
}

func authAdapterError(status int, code APIErrorCode, message string) error {
	return &AuthAdapterError{Status: status, Code: code, Message: message}
}

func IsAuthAdapterError(err error, status int, code APIErrorCode) bool {
	var authErr *AuthAdapterError
	return errors.As(err, &authErr) && authErr.Status == status && authErr.Code == code
}
