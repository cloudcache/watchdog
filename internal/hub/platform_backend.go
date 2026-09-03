package hub

import (
	"errors"
	"net/http"
	"strings"

	platform "github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	pbrouter "github.com/pocketbase/pocketbase/tools/router"
)

func (h *Hub) registerPlatformRoutes(se *core.ServeEvent) error {
	if h.backend == nil {
		return nil
	}
	authenticate := NewPocketBaseIdentityAuthenticator(h.App)
	auth := platform.NewIdentityAuthContextAdapter(authenticate, h.backend.Store)
	tenantDiscovery := platform.NewIdentityTenantDiscoveryAdapter(authenticate, h.backend.Store)
	handler := h.backend.Router(auth, tenantDiscovery)
	mountPlatformHandler(se.Router, handler)
	return nil
}

func mountPlatformHandler(router *pbrouter.Router[*core.RequestEvent], handler http.Handler) {
	wrapped := apis.WrapStdHandler(handler)
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		router.Route(method, "/api/v1", wrapped)
		router.Route(method, "/api/v1/{path...}", wrapped)
	}
}

func NewPocketBaseIdentityAuthenticator(app core.App) platform.ExternalIdentityAuthenticator {
	return func(r *http.Request) (platform.ExternalIdentity, error) {
		token := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(token) > 7 && strings.EqualFold(token[:7], "Bearer ") {
			token = strings.TrimSpace(token[7:])
		}
		if token == "" {
			return platform.ExternalIdentity{}, errors.New("missing authorization token")
		}
		record, err := app.FindAuthRecordByToken(token, core.TokenTypeAuth)
		if err != nil || record == nil {
			return platform.ExternalIdentity{}, errors.New("invalid authorization token")
		}
		if record.Collection().Name != "users" {
			return platform.ExternalIdentity{}, errors.New("unsupported auth collection")
		}
		return platform.ExternalIdentity{Provider: "pocketbase", Subject: record.Id}, nil
	}
}
