package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const (
	FlowSavedFilterPrivate = "private"
	FlowSavedFilterTenant  = "tenant"

	flowSavedFilterSchemaVersion  = uint16(1)
	flowSavedFilterNameMax        = 190
	flowSavedFilterDescriptionMax = 1024
)

var (
	ErrFlowSavedFilterInvalid         = errors.New("saved Flow filter is invalid")
	ErrFlowSavedFilterVersionConflict = errors.New("saved Flow filter changed since it was read")
)

// FlowSavedFilter is a reusable canonical typed-filter AST. It intentionally
// excludes query time, dimensions and resource selectors: those remain
// explicit on each query and are authorized at execution time.
type FlowSavedFilter struct {
	ID                  ID                         `json:"id"`
	TenantID            ID                         `json:"tenant_id"`
	OwnerUserID         ID                         `json:"owner_user_id,omitempty"`
	OwnerName           string                     `json:"owner_name,omitempty"`
	Name                string                     `json:"name"`
	Description         string                     `json:"description"`
	ShareScope          string                     `json:"share_scope"`
	FilterSchemaVersion uint16                     `json:"filter_schema_version"`
	Filter              flowquery.FilterExpression `json:"filter"`
	RowVersion          uint64                     `json:"row_version"`
	CanEdit             bool                       `json:"can_edit"`
	CreatedAt           time.Time                  `json:"created_at"`
	UpdatedAt           time.Time                  `json:"updated_at"`
}

type FlowSavedFilterListQuery struct {
	ViewerUserID ID
	Search       string
	ShareScope   string
	OwnerUserID  ID
	SortBy       string
	Descending   bool
	Limit        int
	Offset       int
}

type FlowSavedFilterOwnerFacet struct {
	OwnerUserID ID     `json:"owner_user_id"`
	OwnerName   string `json:"owner_name"`
	Count       int64  `json:"count"`
}

type FlowSavedFilterRepository interface {
	ListFlowSavedFilters(context.Context, ID, FlowSavedFilterListQuery) ([]FlowSavedFilter, int64, error)
	ListFlowSavedFilterOwners(context.Context, ID, ID, string, int) ([]FlowSavedFilterOwnerFacet, error)
	GetFlowSavedFilter(context.Context, ID, ID, ID) (FlowSavedFilter, error)
	CreateFlowSavedFilter(context.Context, FlowSavedFilter) (FlowSavedFilter, error)
	UpdateFlowSavedFilter(context.Context, FlowSavedFilter, uint64) (FlowSavedFilter, error)
	DeleteFlowSavedFilter(context.Context, ID, ID, uint64) error
}

func normalizeFlowSavedFilter(item FlowSavedFilter) (FlowSavedFilter, error) {
	item.Name = strings.TrimSpace(item.Name)
	item.Description = strings.TrimSpace(item.Description)
	item.ShareScope = strings.ToLower(strings.TrimSpace(item.ShareScope))
	if item.Name == "" || len(item.Name) > flowSavedFilterNameMax {
		return FlowSavedFilter{}, fmt.Errorf("%w: name must contain 1..%d characters", ErrFlowSavedFilterInvalid, flowSavedFilterNameMax)
	}
	if len(item.Description) > flowSavedFilterDescriptionMax {
		return FlowSavedFilter{}, fmt.Errorf("%w: description exceeds %d characters", ErrFlowSavedFilterInvalid, flowSavedFilterDescriptionMax)
	}
	if item.ShareScope == "" {
		item.ShareScope = FlowSavedFilterPrivate
	}
	if item.ShareScope != FlowSavedFilterPrivate && item.ShareScope != FlowSavedFilterTenant {
		return FlowSavedFilter{}, fmt.Errorf("%w: share_scope must be private or tenant", ErrFlowSavedFilterInvalid)
	}
	canonical, err := flowquery.CanonicalFilter(item.Filter)
	if err != nil {
		return FlowSavedFilter{}, fmt.Errorf("%w: %v", ErrFlowSavedFilterInvalid, err)
	}
	fields, err := flowquery.FilterFields(canonical)
	if err != nil {
		return FlowSavedFilter{}, fmt.Errorf("%w: %v", ErrFlowSavedFilterInvalid, err)
	}
	for _, field := range fields {
		switch field {
		case "target", "device", "exporter":
			return FlowSavedFilter{}, fmt.Errorf("%w: resource field %q must use the authorized query selector and cannot be saved", ErrFlowSavedFilterInvalid, field)
		}
	}
	item.Filter = canonical
	item.FilterSchemaVersion = flowSavedFilterSchemaVersion
	return item, nil
}

func normalizeFlowSavedFilterListQuery(query FlowSavedFilterListQuery) (FlowSavedFilterListQuery, error) {
	query.Search = strings.TrimSpace(query.Search)
	query.ShareScope = strings.ToLower(strings.TrimSpace(query.ShareScope))
	query.SortBy = strings.ToLower(strings.TrimSpace(query.SortBy))
	if query.ViewerUserID == "" {
		return query, fmt.Errorf("%w: viewer user is required", ErrFlowSavedFilterInvalid)
	}
	if len(query.Search) > 255 {
		return query, fmt.Errorf("%w: search exceeds 255 characters", ErrFlowSavedFilterInvalid)
	}
	if query.ShareScope != "" && query.ShareScope != FlowSavedFilterPrivate && query.ShareScope != FlowSavedFilterTenant {
		return query, fmt.Errorf("%w: scope must be private or tenant", ErrFlowSavedFilterInvalid)
	}
	if query.SortBy == "" {
		query.SortBy = "updated_at"
		query.Descending = true
	}
	switch query.SortBy {
	case "name", "share_scope", "owner_user_id", "created_at", "updated_at":
	default:
		return query, fmt.Errorf("%w: unsupported sort column", ErrFlowSavedFilterInvalid)
	}
	if query.Limit == 0 {
		query.Limit = 25
	}
	if query.Limit < 1 || query.Limit > 200 {
		return query, fmt.Errorf("%w: limit must be between 1 and 200", ErrFlowSavedFilterInvalid)
	}
	if query.Offset < 0 || query.Offset > 100_000 {
		return query, fmt.Errorf("%w: offset must be between 0 and 100000", ErrFlowSavedFilterInvalid)
	}
	return query, nil
}
