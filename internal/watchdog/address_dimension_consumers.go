package watchdog

import (
	"context"
	"time"
)

const (
	AddressDimensionConsumerReady      = "ready"
	AddressDimensionConsumerDownloaded = "downloaded"
	AddressDimensionConsumerFailed     = "failed"
	AddressDimensionConsumerUnreported = "unreported"

	AddressDimensionDriftCurrent     = "current"
	AddressDimensionDriftBehind      = "behind"
	AddressDimensionDriftAhead       = "ahead"
	AddressDimensionDriftUninstalled = "uninstalled"

	AddressDimensionQueryabilityUnknown     = "unknown"
	AddressDimensionQueryabilityReady       = "ready_observed"
	AddressDimensionQueryabilityPartial     = "partial_observed"
	AddressDimensionQueryabilityUnavailable = "unavailable_observed"
	AddressDimensionConsumerScopeObserved   = "observed_only"
)

// AddressDimensionConsumerStatus separates target-version readiness from
// version drift. A worker may retain the target index while also installing a
// newer version, so LatestInstalledVersion does not replace TargetState.
type AddressDimensionConsumerStatus struct {
	WorkerID                string     `json:"worker_id"`
	BootID                  string     `json:"boot_id"`
	SoftwareVersion         string     `json:"software_version"`
	TargetState             string     `json:"target_state"`
	TargetAttemptedAt       *time.Time `json:"target_attempted_at,omitempty"`
	TargetInstalledAt       *time.Time `json:"target_installed_at,omitempty"`
	TargetErrorCode         string     `json:"target_error_code,omitempty"`
	TargetErrorMessage      string     `json:"target_error_message,omitempty"`
	LatestInstalledSnapshot ID         `json:"latest_installed_snapshot_id,omitempty"`
	LatestInstalledVersion  uint64     `json:"latest_installed_version,omitempty"`
	LatestInstalledAt       *time.Time `json:"latest_installed_at,omitempty"`
	Drift                   string     `json:"drift"`
}

type AddressDimensionConsumerSummary struct {
	SnapshotID   ID     `json:"snapshot_id"`
	Version      uint64 `json:"version"`
	Scope        string `json:"scope"`
	Queryability string `json:"queryability"`
	Observed     uint64 `json:"observed"`
	Ready        uint64 `json:"ready"`
	Downloaded   uint64 `json:"downloaded"`
	Failed       uint64 `json:"failed"`
	Unreported   uint64 `json:"unreported"`
	Current      uint64 `json:"current"`
	Behind       uint64 `json:"behind"`
	Ahead        uint64 `json:"ahead"`
	Uninstalled  uint64 `json:"uninstalled"`
}

type AddressDimensionConsumerFilter struct {
	Query  string
	State  string
	Drift  string
	Limit  int
	Cursor string
}

type AddressDimensionConsumerStatusReader interface {
	GetAddressDimensionConsumerSummary(context.Context, ID, ID) (AddressDimensionConsumerSummary, error)
	ListAddressDimensionConsumers(context.Context, ID, ID, AddressDimensionConsumerFilter) ([]AddressDimensionConsumerStatus, string, error)
}

type AddressDimensionRuntimeStatus struct {
	At         time.Time                       `json:"at"`
	Activation AddressDimensionActivation      `json:"activation"`
	Snapshot   AddressDimensionSnapshot        `json:"snapshot"`
	Consumers  AddressDimensionConsumerSummary `json:"consumers"`
}
