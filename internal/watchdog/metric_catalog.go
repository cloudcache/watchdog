package watchdog

import "github.com/cloudcache/watchdog/internal/metricdomain"

// Deprecated: the live server reads the catalog from internal/metricdomain.
type MetricScope = metricdomain.Scope

const (
	MetricScopeTarget = metricdomain.ScopeTarget
	MetricScopeDevice = metricdomain.ScopeDevice
	MetricScopePort   = metricdomain.ScopePort
	MetricScopeBGP    = metricdomain.ScopeBGP
)

type MetricDefinition = metricdomain.Definition

var MetricCatalog = metricdomain.Catalog

func IsKnownMetric(name string) bool { return metricdomain.IsKnown(name) }
