package watchdog

import (
	"context"
	"time"
)

type SNMPCollectorRepository interface {
	ListSNMPOSDefinitions(ctx context.Context) ([]SNMPCollectorOSDefinition, error)
	UpsertSNMPOSDefinition(ctx context.Context, definition SNMPCollectorOSDefinition) (SNMPCollectorOSDefinition, error)
	ListSNMPModuleDefinitions(ctx context.Context, moduleType SNMPCollectorModuleType) ([]SNMPCollectorModuleDefinition, error)
	UpsertSNMPModuleDefinition(ctx context.Context, definition SNMPCollectorModuleDefinition) (SNMPCollectorModuleDefinition, error)
	UpsertSNMPDeviceModule(ctx context.Context, module SNMPCollectorDeviceModule) (SNMPCollectorDeviceModule, error)
	UpsertSNMPStateTranslation(ctx context.Context, translation SNMPStateTranslation) (SNMPStateTranslation, error)
	UpsertSNMPCollectionRecipes(ctx context.Context, recipes []SNMPCollectionRecipe) error
	ListDueSNMPCollectionRecipes(ctx context.Context, tenantID ID, limit int, now time.Time) ([]SNMPCollectionRecipe, error)
	ListDueSNMPDevices(ctx context.Context, tenantID ID, limit int, now time.Time) ([]ID, error)
	ListSNMPCollectionRecipesByDevice(ctx context.Context, tenantID, deviceID ID) ([]SNMPCollectionRecipe, error)
	GetSNMPDeviceLastPolledAt(ctx context.Context, tenantID, deviceID ID) (time.Time, error)
	MarkSNMPRecipePollResult(ctx context.Context, recipeID ID, polledAt time.Time, lastError string) error
	ListSNMPTrapHandlers(ctx context.Context) ([]SNMPTrapHandlerDefinition, error)
	UpsertSNMPTrapHandler(ctx context.Context, handler SNMPTrapHandlerDefinition) (SNMPTrapHandlerDefinition, error)
	PruneSNMPCollectionRecipes(ctx context.Context, tenantID, deviceID ID, keep []ID) (int64, error)
	CreateSNMPEvent(ctx context.Context, event SNMPEvent) error
	ListSNMPEvents(ctx context.Context, tenantID, deviceID ID, limit int) ([]SNMPEvent, error)
	ListSNMPEventsPaged(ctx context.Context, tenantID, deviceID ID, filter SNMPEventFilter) ([]SNMPEvent, string, error)
}

// SNMPEventTableRepository is the optional management-query surface used by
// the Events and Alerts VTables. It is separate from the collector write
// contract so collectors and discovery runners do not acquire UI concerns.
type SNMPEventTableRepository interface {
	ListSNMPEventTable(ctx context.Context, tenantID, deviceID ID, query SNMPEventTableQuery) ([]SNMPEvent, int64, error)
	ListSNMPEventFacets(ctx context.Context, tenantID, deviceID ID, query SNMPEventFacetQuery) ([]SNMPEventFacet, error)
}

type SNMPEventTableQuery struct {
	Search        string
	Severities    []string
	EventTypes    []string
	Sources       []string
	SortBy        string
	SortDirection string
	Limit         int
	Offset        int
}

type SNMPEventFacetQuery struct {
	SNMPEventTableQuery
	Field       string
	FacetSearch string
}

type SNMPEventFacet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}
