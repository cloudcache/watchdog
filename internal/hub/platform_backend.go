package hub

import (
	"context"
	"errors"
	"net/http"
	"strings"

	alerts "github.com/cloudcache/watchdog/internal/alerts"
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
	if metrics := h.backend.MetricsScrapeHandler(); metrics != nil {
		mountPlatformMetricsHandler(se.Router, metrics)
	}
	return nil
}

func mountPlatformHandler(router *pbrouter.Router[*core.RequestEvent], handler http.Handler) {
	wrapped := apis.WrapStdHandler(handler)
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		router.Route(method, "/api/v1", wrapped)
		router.Route(method, "/api/v1/{path...}", wrapped)
	}
}

func mountPlatformMetricsHandler(router *pbrouter.Router[*core.RequestEvent], handler http.Handler) {
	wrapped := apis.WrapStdHandler(handler)
	router.Route(http.MethodGet, "/metrics", wrapped)
	router.Route(http.MethodHead, "/metrics", wrapped)
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

// notificationChannelReader adapts the MySQL store to the alert delivery
// path's NotificationChannelReader (PLAT P0 alert subsystem).
type notificationChannelReader struct {
	store *platform.MySQLStore
}

func (r notificationChannelReader) ChannelsForExternalSubject(ctx context.Context, provider, externalSubject string) ([]string, []string, error) {
	channels, err := r.store.NotificationChannelsForExternalSubject(ctx, provider, externalSubject)
	if err != nil {
		return nil, nil, err
	}
	return channels.Emails, channels.Webhooks, nil
}

// quietHoursReader adapts the MySQL store to the alert silencing path's
// QuietHoursReader (PLAT P0 alert subsystem).
type quietHoursReader struct {
	store *platform.MySQLStore
}

func (r quietHoursReader) QuietHoursForExternalSubject(ctx context.Context, provider, externalSubject, systemID string) ([]alerts.QuietHourWindow, error) {
	windows, err := r.store.QuietHoursForExternalSubject(ctx, provider, externalSubject, systemID)
	if err != nil {
		return nil, err
	}
	out := make([]alerts.QuietHourWindow, 0, len(windows))
	for _, window := range windows {
		out = append(out, alerts.QuietHourWindow{Type: window.Type, Start: window.Start, End: window.End})
	}
	return out, nil
}
