package watchdog

import (
	"context"
	"encoding/json"
	"time"
)

type TenantRepository interface {
	GetTenant(ctx context.Context, tenantID ID) (Tenant, error)
	ListTenantsForUser(ctx context.Context, userID ID) ([]Tenant, error)
}

type PermissionRepository interface {
	ListPermissions(ctx context.Context, tenantID ID) ([]Permission, error)
	ListPermissionsForUser(ctx context.Context, tenantID, userID ID) ([]Permission, error)
	ReplacePermission(ctx context.Context, grant Permission) error
	DeletePermission(ctx context.Context, tenantID ID, subjectType SubjectType, subjectID ID, resourceType ResourceType, resourceID ID) error
}

// TargetPageFilter requests an opt-in keyset page of targets ordered by
// (name, id). Limit is clamped by the repository; Cursor is a next_cursor from
// a prior page (""for the first). ExcludeKind, when set, omits targets of that
// kind (the Hosts view excludes network targets, which live on the device page).
type TargetPageFilter struct {
	Limit       int
	Cursor      string
	ExcludeKind string
}

// TargetTableQuery is the offset-paged, server-driven Hosts table contract.
// It is separate from TargetPageFilter so existing keyset dropdown callers do
// not inherit mutable search/sort semantics.
type TargetTableQuery struct {
	Search      string
	Kind        string
	ExcludeKind string
	Status      string
	Sort        string
	Desc        bool
	Limit       int
	Offset      int
}

type TargetRepository interface {
	ListTargets(ctx context.Context, tenantID ID) ([]Target, error)
	// ListTargetsPage returns a keyset page. When all is true the whole tenant
	// is visible (admin); otherwise only targets whose id is in allowedIDs are
	// returned (per-target view grants pushed into SQL). Returns the page and a
	// next_cursor ("" on the last page).
	ListTargetsPage(ctx context.Context, tenantID ID, all bool, allowedIDs []ID, filter TargetPageFilter) ([]Target, string, error)
	ListTargetsTablePage(ctx context.Context, tenantID ID, all bool, allowedIDs []ID, query TargetTableQuery) ([]Target, int, error)
	GetTarget(ctx context.Context, tenantID, targetID ID) (Target, error)
	// GetTargetsByIDs batch-loads targets by id (for a page of device summaries).
	GetTargetsByIDs(ctx context.Context, tenantID ID, ids []ID) (map[ID]Target, error)
	CreateTarget(ctx context.Context, target Target) (Target, error)
	UpdateTarget(ctx context.Context, target Target) (Target, error)
	DeleteTarget(ctx context.Context, tenantID, targetID ID) error
}

// NetworkTargetProvisioner creates the management target and its SNMP device
// projection as one unit. The MySQL implementation uses one transaction so a
// failed device insert cannot leave a target that cannot be polled.
type NetworkTargetProvisioner interface {
	CreateNetworkTarget(ctx context.Context, target Target, device NetworkDevice) (Target, NetworkDevice, error)
}

type AgentRepository interface {
	GetAgent(ctx context.Context, agentID ID) (SNMPAgentConfig, error)
	ListAgents(ctx context.Context, tenantID ID) ([]SNMPAgentConfig, error)
	ListAgentsPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, filter AgentPageFilter) ([]SNMPAgentConfig, int, error)
	UpsertAgent(ctx context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error)
	DeleteAgent(ctx context.Context, tenantID, agentID ID) error
	MarkAgentSeen(ctx context.Context, agentID ID) error
	RecordAgentRun(ctx context.Context, report AgentRunReport) error
	ListAgentRuns(ctx context.Context, tenantID, agentID ID, limit int) ([]AgentRunHistory, error)
	// ListAgentRunsPage returns a filtered server page, optional compatibility
	// keyset cursor, and the filtered total for the Agent Runs table.
	ListAgentRunsPage(ctx context.Context, tenantID, agentID ID, filter AgentRunPageFilter) ([]AgentRunHistory, string, int, error)
}

// AgentPageFilter drives the server-side Agent Registry table. Offset paging
// is intentional here: the UI needs a stable total and direct previous/next
// navigation while the sortable result set is bounded by tenant and grants.
type AgentPageFilter struct {
	Search    string
	AgentType AgentType
	Status    string
	Sort      string
	Desc      bool
	Limit     int
	Offset    int
}

type CollectorPlanRepository interface {
	CreateCollectorPlanRevision(ctx context.Context, plan CollectorPlanRevision) (CollectorPlanRevision, error)
	CreateNextCollectorPlanRevision(ctx context.Context, request CollectorPlanCreateRequest, signer CollectorPlanSigner) (CollectorPlanRevision, error)
	ListCollectorPlanRevisions(ctx context.Context, tenantID, collectorID ID, filter CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error)
	GetCollectorPlanRevision(ctx context.Context, tenantID, collectorID ID, configVersion uint64) (CollectorPlanRevision, error)
	ActivateCollectorPlanRevision(ctx context.Context, activation CollectorPlanActivation) (CollectorPlanRevision, error)
	AcknowledgeCollectorPlan(ctx context.Context, acknowledgement CollectorPlanAcknowledgement) error
}

type CollectorOwnershipRepository interface {
	CreateCollectorServicePrincipal(ctx context.Context, grant CollectorServicePrincipalGrant) error
	RevokeCollectorServicePrincipal(ctx context.Context, revocation CollectorPrincipalRevocation) error
	CreateCollectorOwnershipTransfer(ctx context.Context, transfer CollectorOwnershipTransfer) error
	RecordCollectorDrain(ctx context.Context, receipt CollectorDrainReceipt) error
}

type CollectorPrincipalOperationRepository interface {
	CreateCollectorServicePrincipal(ctx context.Context, grant CollectorServicePrincipalGrant) error
	RevokeCollectorServicePrincipal(ctx context.Context, revocation CollectorPrincipalRevocation) error
	GetCollectorServicePrincipal(ctx context.Context, tenantID, collectorID, principalID ID) (CollectorServicePrincipal, error)
	GetCollectorServicePrincipalByGrantOperation(ctx context.Context, tenantID ID, operationKey string) (CollectorServicePrincipal, error)
}

// NetworkDevicePageFilter requests an opt-in keyset page of network devices
// ordered by (sys_name, id). Limit is clamped by the repository; Cursor is a
// next_cursor from a prior page ("" for the first).
type NetworkDevicePageFilter struct {
	Limit  int
	Cursor string
}

type NetworkRepository interface {
	ListDevices(ctx context.Context, tenantID ID) ([]NetworkDevice, error)
	// ListDevicesPage returns a keyset page. When all is true the whole tenant is
	// visible (admin or a tenant-scoped grant); otherwise only devices whose
	// target_id is in allowedTargetIDs are returned (per-target view grants
	// pushed into SQL). Returns the page and a next_cursor ("" on the last page).
	ListDevicesPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, filter NetworkDevicePageFilter) ([]NetworkDevice, string, error)
	// ListDeviceSummaryDevicesPage returns one offset page of devices for the
	// server-driven summary table (search/status/sort applied via a join to
	// targets); the caller enriches only this page.
	ListDeviceSummaryDevicesPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, q DeviceSummaryQuery) ([]NetworkDevice, error)
	// CountDeviceStatuses returns the grant-scoped status totals for the badges.
	CountDeviceStatuses(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, search string) (DeviceStatusCounts, error)
	GetDevice(ctx context.Context, tenantID, deviceID ID) (NetworkDevice, error)
	GetDeviceByTarget(ctx context.Context, tenantID, targetID ID) (NetworkDevice, error)
	UpsertDevice(ctx context.Context, device NetworkDevice) (NetworkDevice, error)
	DeleteDevice(ctx context.Context, tenantID, deviceID ID) error
	ListPorts(ctx context.Context, tenantID, deviceID ID) ([]NetworkPort, error)
	ListDevicePortsPage(ctx context.Context, tenantID, deviceID ID, query NetworkPortQuery) ([]NetworkPort, int, error)
	CountDevicePorts(ctx context.Context, tenantID, deviceID ID) (NetworkPortCounts, error)
	ListInterfaceAddresses(ctx context.Context, tenantID, deviceID ID) ([]NetworkInterfaceAddress, error)
	ListInterfaceAddressesByPorts(ctx context.Context, tenantID ID, portIDs []ID) ([]NetworkInterfaceAddress, error)
	GetPort(ctx context.Context, tenantID, portID ID) (NetworkPort, error)
	UpsertPorts(ctx context.Context, ports []NetworkPort) error
	ReplaceInterfaceAddresses(ctx context.Context, tenantID, deviceID ID, addresses []NetworkInterfaceAddress) error
	DeletePort(ctx context.Context, tenantID, portID ID) error
	UpsertPortTransceiver(ctx context.Context, transceiver NetworkPortTransceiver) (NetworkPortTransceiver, error)
	GetPortTransceiver(ctx context.Context, tenantID, portID ID) (NetworkPortTransceiver, error)
	ListDeviceSensors(ctx context.Context, tenantID, deviceID ID) ([]NetworkDeviceSensor, error)
	ListDeviceSensorsPage(ctx context.Context, tenantID, deviceID ID, query DeviceSensorQuery) ([]NetworkDeviceSensor, int, error)
	CountDeviceSensors(ctx context.Context, tenantID, deviceID ID) (DeviceSensorCounts, error)
	UpsertDeviceSensors(ctx context.Context, sensors []NetworkDeviceSensor) error
	ListDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID) ([]PhysicalEntity, error)
	ListDevicePhysicalEntitiesPage(ctx context.Context, tenantID, deviceID ID, query PhysicalEntityQuery) ([]PhysicalEntity, int, error)
	UpsertDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID, entities []PhysicalEntity) error
	ListDeviceVLANs(ctx context.Context, tenantID, deviceID ID) ([]DeviceVLAN, error)
	ListDeviceVLANsPage(ctx context.Context, tenantID, deviceID ID, query DeviceVLANQuery) ([]DeviceVLAN, int, error)
	UpsertDeviceVLANs(ctx context.Context, tenantID, deviceID ID, vlans []DeviceVLAN) error
	ListDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID) ([]DeviceLAGGroup, error)
	ListDeviceLAGGroupsPage(ctx context.Context, tenantID, deviceID ID, query DeviceLAGQuery) ([]DeviceLAGGroup, int, error)
	UpsertDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID, groups []DeviceLAGGroup) error
	ListBGPSessions(ctx context.Context, tenantID, deviceID ID) ([]BGPSession, error)
	ListDeviceBGPSessionsPage(ctx context.Context, tenantID, deviceID ID, q BGPSessionQuery) ([]BGPSession, int, error)
	CountDeviceBGPSessions(ctx context.Context, tenantID, deviceID ID) (BGPSessionCounts, error)
	ListAllBGPSessions(ctx context.Context, tenantID ID) ([]BGPSession, error)
	GetBGPSession(ctx context.Context, tenantID, sessionID ID) (BGPSession, error)
	// ListAllBGPSessionsPage returns one offset page for the server-driven Core
	// (BGP) table (search/state/sort via a join to the owning devices).
	ListAllBGPSessionsPage(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, q BGPSessionQuery) ([]BGPSession, int, error)
	// CountBGPSessions returns the grant-scoped total/established counts.
	CountBGPSessions(ctx context.Context, tenantID ID, all bool, allowedTargetIDs []ID, search string) (BGPSessionCounts, error)
	UpsertBGPSessions(ctx context.Context, sessions []BGPSession) error
	ReplaceBGPSessions(ctx context.Context, tenantID, deviceID ID, sessions []BGPSession) error
	GetPortPolicy(ctx context.Context, tenantID, portID ID) (PortPolicy, error)
	UpsertPortPolicy(ctx context.Context, policy PortPolicy) (PortPolicy, error)
	GetTrafficPolicyDefaults(ctx context.Context, tenantID ID) (TrafficPolicyDefaults, error)
	UpsertTrafficPolicyDefault(ctx context.Context, policyDefault TrafficPolicyDefault) (TrafficPolicyDefault, error)
}

// PhysicalEntityQuery drives the device Inventory tab over ENTITY-MIB rows.
// FRU is nil for all rows and otherwise selects entPhysicalIsFRU.
type PhysicalEntityQuery struct {
	Search string
	Class  string
	FRU    *bool
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

type DeviceVLANQuery struct {
	Search string
	Status string
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

type DeviceLAGQuery struct {
	Search string
	Mode   string
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

type DeviceSensorQuery struct {
	Search string
	Class  string
	Status string
	Health string
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

type DeviceSensorCounts struct {
	Total    int `json:"total"`
	Problems int `json:"problems"`
}

type NetworkPortQuery struct {
	Search        string
	AdminStatus   string
	OperStatus    string
	AddressFamily string
	Sort          string
	Desc          bool
	Limit         int
	Offset        int
}

type NetworkPortCounts struct {
	Total int `json:"total"`
	Up    int `json:"up"`
	Down  int `json:"down"`
}

type SNMPRepository interface {
	ListSNMPProfiles(ctx context.Context, tenantID ID) ([]SNMPProfile, error)
	GetSNMPProfile(ctx context.Context, tenantID, profileID ID) (SNMPProfile, error)
	UpsertSNMPProfile(ctx context.Context, profile SNMPProfile) (SNMPProfile, error)
	DeleteSNMPProfile(ctx context.Context, tenantID, profileID ID) error
	ListMIBModules(ctx context.Context) ([]MIBModule, error)
	UpsertMIBModule(ctx context.Context, module MIBModule) (MIBModule, error)
	DeleteMIBModule(ctx context.Context, moduleID ID) error
}

type ExportRepository interface {
	CreateExportTask(ctx context.Context, task ExportTask) (ExportTask, error)
	GetExportTask(ctx context.Context, tenantID, taskID ID) (ExportTask, error)
	ListExportTasks(ctx context.Context, tenantID ID, createdBy ID) ([]ExportTask, error)
	RetryExportTask(ctx context.Context, tenantID, taskID ID) error
	MarkExportRunning(ctx context.Context, tenantID, taskID ID) error
	MarkExportComplete(ctx context.Context, tenantID, taskID ID, artifact ExportArtifact, expiresAt time.Time) error
	MarkExportFailed(ctx context.Context, tenantID, taskID ID, message string) error
}

type ExportTaskListFilter struct {
	CreatedBy     ID
	Search        string
	Status        ExportStatus
	ValueLayer    QueryValueLayer
	Format        ExportFormat
	SortBy        string
	SortDirection string
	Limit         int
	Offset        int
}

type ExportPageRepository interface {
	ListExportTasksPage(ctx context.Context, tenantID ID, filter ExportTaskListFilter) ([]ExportTask, int64, error)
}

type ExportDeletionRepository interface {
	DeleteExportTask(ctx context.Context, tenantID, taskID ID) error
}

type AuditRepository interface {
	CreateAuditLog(ctx context.Context, log AuditLog) error
}

type BillingRepository interface {
	CreateBillingAccount(ctx context.Context, account BillingAccount) (BillingAccount, error)
	GetBillingAccount(ctx context.Context, tenantID, accountID ID) (BillingAccount, error)
	ListBillingAccounts(ctx context.Context, tenantID ID) ([]BillingAccount, error)
	UpdateBillingAccount(ctx context.Context, account BillingAccount) (BillingAccount, error)
	ListBillingAccountPorts(ctx context.Context, tenantID, accountID ID) ([]BillingAccountPort, error)
	ReplaceBillingAccountPorts(ctx context.Context, tenantID, accountID ID, ports []BillingAccountPort) error
	CreateBillingPeriod(ctx context.Context, period BillingPeriod) (BillingPeriod, error)
	GetBillingPeriod(ctx context.Context, tenantID, periodID ID) (BillingPeriod, error)
	MarkBillingPeriodComputed(ctx context.Context, tenantID, periodID ID, computedValue float64, totalBytes uint64) error
	UpdateBillingPeriodStatus(ctx context.Context, tenantID, periodID ID, status BillingPeriodStatus) error
}

type ExportStatus string

const (
	ExportStatusPending  ExportStatus = "pending"
	ExportStatusRunning  ExportStatus = "running"
	ExportStatusComplete ExportStatus = "complete"
	ExportStatusFailed   ExportStatus = "failed"
	ExportStatusCanceled ExportStatus = "canceled"
)

type PeriodType string

const (
	PeriodDay    PeriodType = "day"
	PeriodMonth  PeriodType = "month"
	PeriodFixed  PeriodType = "fixed"
	PeriodCustom PeriodType = "custom"
)

type ExportValueMode string

const (
	ExportValueCorrected ExportValueMode = "corrected"
	ExportValueRaw       ExportValueMode = "raw"
	ExportValueBoth      ExportValueMode = "both"
)

type ExportFormat string

const (
	ExportFormatCSV     ExportFormat = "csv"
	ExportFormatParquet ExportFormat = "parquet"
)

type ExportTask struct {
	ID                    ID
	TenantID              ID
	CreatedBy             ID
	ContractVersion       uint16
	DatasetKey            string
	QueryJSON             json.RawMessage
	QueryHash             string
	ValueLayer            QueryValueLayer
	VersionsJSON          json.RawMessage
	AuthorizationJSON     json.RawMessage
	OperationJobID        ID
	RetentionSeconds      uint32
	ArtifactSchemaVersion uint16
	ContentType           string
	RowCount              uint64
	TargetID              ID
	PortID                ID
	PeriodType            PeriodType
	RangeStart            time.Time
	RangeEnd              time.Time
	Step                  time.Duration
	Aggregation           Aggregation
	ValueMode             ExportValueMode
	Format                ExportFormat
	Status                ExportStatus
	FileRef               string
	Checksum              string
	SizeBytes             int64
	ExpiresAt             time.Time
	ErrorMessage          string
	RowVersion            uint64
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// ExportArtifact is the produced file plus its integrity metadata, returned by
// an export writer so the worker can persist the checksum and size.
type ExportArtifact struct {
	FileRef       string
	Checksum      string // sha256 hex of the file bytes
	SizeBytes     int64
	SchemaVersion uint16
	ContentType   string
	RowCount      uint64
}

type BillingDirection string

const (
	BillingDirectionIn  BillingDirection = "in"
	BillingDirectionOut BillingDirection = "out"
	BillingDirectionSum BillingDirection = "sum"
	BillingDirectionMax BillingDirection = "max"
)

type BillingStatus string

const (
	BillingStatusActive BillingStatus = "active"
	BillingStatusPaused BillingStatus = "paused"
)

type BillingAccount struct {
	ID          ID              `json:"id"`
	TenantID    ID              `json:"tenant_id"`
	Name        string          `json:"name"`
	Status      BillingStatus   `json:"status"`
	BillingDay  uint8           `json:"billing_day"`
	Aggregation Aggregation     `json:"aggregation"`
	ValueMode   ExportValueMode `json:"value_mode"`
	QuotaBytes  uint64          `json:"quota_bytes"`
	Notes       string          `json:"notes"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type BillingAccountPort struct {
	BillingAccountID ID               `json:"billing_account_id"`
	TenantID         ID               `json:"tenant_id"`
	PortID           ID               `json:"port_id"`
	Direction        BillingDirection `json:"direction"`
	CreatedAt        time.Time        `json:"created_at"`
}

type BillingPeriodStatus string

const (
	BillingPeriodOpen     BillingPeriodStatus = "open"
	BillingPeriodComputed BillingPeriodStatus = "computed"
	BillingPeriodApproved BillingPeriodStatus = "approved"
	BillingPeriodVoid     BillingPeriodStatus = "void"
)

type BillingPeriod struct {
	ID               ID                  `json:"id"`
	TenantID         ID                  `json:"tenant_id"`
	BillingAccountID ID                  `json:"billing_account_id"`
	RangeStart       time.Time           `json:"range_start"`
	RangeEnd         time.Time           `json:"range_end"`
	Status           BillingPeriodStatus `json:"status"`
	ComputedValue    float64             `json:"computed_value"`
	TotalBytes       uint64              `json:"total_bytes"`
	ComputedAt       time.Time           `json:"computed_at"`
	CreatedAt        time.Time           `json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
}

type AuditLog struct {
	ID           ID
	TenantID     ID
	ActorID      ID
	Action       string
	ResourceType ResourceType
	ResourceID   ID
	Detail       map[string]any
	CreatedAt    time.Time
}
