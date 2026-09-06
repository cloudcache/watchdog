package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Dashboard is a named, owned, versioned layout that arranges references to
// aggregate graphs in a grid. The layout is opaque JSON to the store; its shape
// is validated at the API boundary (see validateDashboardLayout).
type Dashboard struct {
	ID          ID              `json:"id"`
	TenantID    ID              `json:"tenant_id"`
	OwnerID     ID              `json:"owner_id,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Layout      json.RawMessage `json:"layout"`
	Version     uint32          `json:"version"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// DashboardListFilter is the server-side VTable contract. Offset pagination is
// intentional here: dashboards are management records with a bounded page
// size, while users need arbitrary sorting by the visible columns.
type DashboardListFilter struct {
	Search  string
	OwnerID ID
	Sort    string
	Desc    bool
	Limit   int
	Offset  int
}

// DashboardGraphReference is the resolved, tenant-scoped dependency behind a
// dashboard panel. Missing references stay in the response so drafts remain
// editable and preview can explain exactly what cannot be rendered.
type DashboardGraphReference struct {
	GraphID ID                   `json:"graph_id"`
	Exists  bool                 `json:"exists"`
	Graph   *AggregateGraph      `json:"graph,omitempty"`
	Series  []AggregateGraphItem `json:"series"`
}

type DashboardPreview struct {
	Dashboard       Dashboard                 `json:"dashboard"`
	References      []DashboardGraphReference `json:"references"`
	MissingGraphIDs []ID                      `json:"missing_graph_ids"`
}

// DashboardRepository is the persistence surface for dashboards. Every method
// is tenant-scoped; a dashboard is never addressable outside its tenant.
type DashboardRepository interface {
	ListDashboards(ctx context.Context, tenantID ID, filter DashboardListFilter) ([]Dashboard, int64, error)
	GetDashboard(ctx context.Context, tenantID, id ID) (Dashboard, error)
	CreateDashboard(ctx context.Context, dashboard Dashboard) (Dashboard, error)
	UpdateDashboard(ctx context.Context, dashboard Dashboard, expectedVersion uint32) (Dashboard, error)
	DeleteDashboard(ctx context.Context, tenantID, id ID, expectedVersion uint32) error
	ResolveDashboardGraphReferences(ctx context.Context, tenantID ID, graphIDs []ID) ([]DashboardGraphReference, error)
}

// ErrDashboardNameConflict is returned when a tenant already has a dashboard
// with the requested name.
var ErrDashboardNameConflict = errors.New("a dashboard with this name already exists")

// ErrDashboardVersionConflict fences stale editors at the SQL write boundary;
// the API's read-before-write ETag check alone cannot prevent a TOCTOU update.
var ErrDashboardVersionConflict = errors.New("dashboard changed since it was read")

const (
	dashboardNameMaxLen     = 190
	dashboardMaxPanels      = 200
	dashboardLayoutMax      = 256 * 1024 // 256 KiB of layout JSON is plenty for a grid
	dashboardDescMaxLen     = 4096
	dashboardDefaultVersion = 1
)

// dashboardLayout is the validated shape of layout_json. Panels reference a
// graph and carry a grid position; unknown fields are tolerated so the frontend
// can evolve the panel model without a backend change.
type dashboardLayout struct {
	Panels []dashboardPanel `json:"panels"`
}

type dashboardPanel struct {
	GraphID string `json:"graph_id"`
}

// validateDashboardLayout checks that the layout is a JSON object within size
// bounds and that every panel references a non-empty graph. It does not verify
// that the referenced graphs exist — that is a render/preview concern, deferred
// so a dashboard can be drafted before its graphs are wired.
func validateDashboardLayout(raw json.RawMessage) error {
	if len(raw) == 0 {
		return errors.New("dashboard layout is required")
	}
	if len(raw) > dashboardLayoutMax {
		return fmt.Errorf("dashboard layout exceeds %d bytes", dashboardLayoutMax)
	}
	trimmed := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(trimmed, "{") {
		return errors.New("dashboard layout must be a JSON object")
	}
	var layout dashboardLayout
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	if err := decoder.Decode(&layout); err != nil {
		return fmt.Errorf("dashboard layout is not valid JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("dashboard layout must contain one JSON object")
		}
		return fmt.Errorf("dashboard layout has trailing data: %w", err)
	}
	if len(layout.Panels) > dashboardMaxPanels {
		return fmt.Errorf("dashboard has %d panels, at most %d allowed", len(layout.Panels), dashboardMaxPanels)
	}
	for i, panel := range layout.Panels {
		if strings.TrimSpace(panel.GraphID) == "" {
			return fmt.Errorf("dashboard panel %d is missing graph_id", i)
		}
	}
	return nil
}

func dashboardGraphIDs(raw json.RawMessage) ([]ID, error) {
	if err := validateDashboardLayout(raw); err != nil {
		return nil, err
	}
	var layout dashboardLayout
	if err := json.Unmarshal(raw, &layout); err != nil {
		return nil, err
	}
	ids := make([]ID, 0, len(layout.Panels))
	seen := make(map[ID]struct{}, len(layout.Panels))
	for _, panel := range layout.Panels {
		id := ID(strings.TrimSpace(panel.GraphID))
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func normalizeDashboardListFilter(filter DashboardListFilter) (DashboardListFilter, error) {
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Sort = strings.ToLower(strings.TrimSpace(filter.Sort))
	if len(filter.Search) > 255 {
		return filter, errors.New("dashboard search exceeds 255 characters")
	}
	if filter.Offset < 0 {
		return filter, errors.New("dashboard offset must not be negative")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}
	if filter.Sort == "" {
		filter.Sort = "name"
	}
	switch filter.Sort {
	case "name", "owner_id", "version", "created_at", "updated_at":
	default:
		return filter, errors.New("unsupported dashboard sort column")
	}
	return filter, nil
}

// normalizeDashboard trims and validates the user-supplied fields shared by
// create and update.
func normalizeDashboard(d Dashboard) (Dashboard, error) {
	d.Name = strings.TrimSpace(d.Name)
	if d.Name == "" {
		return Dashboard{}, errors.New("dashboard name is required")
	}
	if len(d.Name) > dashboardNameMaxLen {
		return Dashboard{}, fmt.Errorf("dashboard name exceeds %d characters", dashboardNameMaxLen)
	}
	if len(d.Description) > dashboardDescMaxLen {
		return Dashboard{}, fmt.Errorf("dashboard description exceeds %d characters", dashboardDescMaxLen)
	}
	if err := validateDashboardLayout(d.Layout); err != nil {
		return Dashboard{}, err
	}
	return d, nil
}
