package watchdog

import (
	"context"
	"net/http"
	"strconv"
	"strings"
)

// Audit log read API. Every management surface writes audit_logs; this is the
// tenant-admin view over them with keyset pagination and field filters.

type AuditLogReader interface {
	ListAuditLogs(ctx context.Context, tenantID ID, filter AuditLogFilter) ([]AuditLog, string, error)
}

type auditLogAPI struct {
	reader AuditLogReader
}

func registerAuditLogRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, reader AuditLogReader) {
	api := auditLogAPI{reader: reader}
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/audit-logs", auth(admin(http.HandlerFunc(api.list))))
}

func (api auditLogAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	filter := AuditLogFilter{
		ResourceType: ResourceType(strings.TrimSpace(query.Get("resource_type"))),
		ResourceID:   ID(strings.TrimSpace(query.Get("resource_id"))),
		ActorID:      ID(strings.TrimSpace(query.Get("actor_id"))),
		Action:       strings.TrimSpace(query.Get("action")),
		Cursor:       strings.TrimSpace(query.Get("cursor")),
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	logs, nextCursor, err := api.reader.ListAuditLogs(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	response := map[string]any{"items": logs}
	if nextCursor != "" {
		response["next_cursor"] = nextCursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}
