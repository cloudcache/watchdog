package watchdog

import (
	"context"
	"time"
)

type TenantRepository interface {
	GetTenant(ctx context.Context, tenantID ID) (Tenant, error)
	ListTenantsForUser(ctx context.Context, userID ID) ([]Tenant, error)
}

type IdentityRepository interface {
	GetUser(ctx context.Context, userID ID) (User, error)
	ListUserRoleIDs(ctx context.Context, tenantID, userID ID) ([]ID, error)
}

type IdentityProjection struct {
	User   User
	Tenant Tenant
}

type IdentityProjectionRepository interface {
	ListIdentityProjections(ctx context.Context, provider, externalSubject string) ([]IdentityProjection, error)
	ListUserRoleIDs(ctx context.Context, tenantID, userID ID) ([]ID, error)
	ListPermissionsForUser(ctx context.Context, tenantID, userID ID) ([]Permission, error)
	IsUserTenantAdmin(ctx context.Context, tenantID, userID ID) (bool, error)
}

// IdentityAdminRepository backs the tenant-scoped user and role management
// APIs (PLAT-01). Users are authorization projections: credentials live in the
// external identity provider, so there is no password material here and
// "deleting" a user disables the projection instead of dropping history.
type IdentityAdminRepository interface {
	ListTenantUsers(ctx context.Context, tenantID ID) ([]User, error)
	GetTenantUser(ctx context.Context, tenantID, userID ID) (User, error)
	CreateUser(ctx context.Context, user User) (User, error)
	UpdateUser(ctx context.Context, user User) (User, error)
	DisableUser(ctx context.Context, tenantID, userID ID) error
	ListRoles(ctx context.Context, tenantID ID) ([]Role, error)
	GetTenantRole(ctx context.Context, tenantID, roleID ID) (Role, error)
	CreateRole(ctx context.Context, role Role) (Role, error)
	UpdateRole(ctx context.Context, role Role) (Role, error)
	DeleteRole(ctx context.Context, tenantID, roleID ID) error
	ReplaceUserRoles(ctx context.Context, tenantID, userID ID, roleIDs []ID) error
	ListUserRoleIDs(ctx context.Context, tenantID, userID ID) ([]ID, error)
}

type PermissionRepository interface {
	ListPermissions(ctx context.Context, tenantID ID) ([]Permission, error)
	ListPermissionsForUser(ctx context.Context, tenantID, userID ID) ([]Permission, error)
	ReplacePermission(ctx context.Context, grant Permission) error
	DeletePermission(ctx context.Context, tenantID ID, subjectType SubjectType, subjectID ID, resourceType ResourceType, resourceID ID) error
}

type TargetRepository interface {
	ListTargets(ctx context.Context, tenantID ID) ([]Target, error)
	GetTarget(ctx context.Context, tenantID, targetID ID) (Target, error)
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
	UpsertAgent(ctx context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error)
	DeleteAgent(ctx context.Context, tenantID, agentID ID) error
	MarkAgentSeen(ctx context.Context, agentID ID) error
	RecordAgentRun(ctx context.Context, report AgentRunReport) error
	ListAgentRuns(ctx context.Context, tenantID, agentID ID, limit int) ([]AgentRunHistory, error)
}

type CollectorPlanRepository interface {
	CreateCollectorPlanRevision(ctx context.Context, plan CollectorPlanRevision) (CollectorPlanRevision, error)
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

type NetworkRepository interface {
	ListDevices(ctx context.Context, tenantID ID) ([]NetworkDevice, error)
	GetDevice(ctx context.Context, tenantID, deviceID ID) (NetworkDevice, error)
	GetDeviceByTarget(ctx context.Context, tenantID, targetID ID) (NetworkDevice, error)
	UpsertDevice(ctx context.Context, device NetworkDevice) (NetworkDevice, error)
	DeleteDevice(ctx context.Context, tenantID, deviceID ID) error
	ListPorts(ctx context.Context, tenantID, deviceID ID) ([]NetworkPort, error)
	ListInterfaceAddresses(ctx context.Context, tenantID, deviceID ID) ([]NetworkInterfaceAddress, error)
	GetPort(ctx context.Context, tenantID, portID ID) (NetworkPort, error)
	UpsertPorts(ctx context.Context, ports []NetworkPort) error
	ReplaceInterfaceAddresses(ctx context.Context, tenantID, deviceID ID, addresses []NetworkInterfaceAddress) error
	DeletePort(ctx context.Context, tenantID, portID ID) error
	UpsertPortTransceiver(ctx context.Context, transceiver NetworkPortTransceiver) (NetworkPortTransceiver, error)
	GetPortTransceiver(ctx context.Context, tenantID, portID ID) (NetworkPortTransceiver, error)
	ListDeviceSensors(ctx context.Context, tenantID, deviceID ID) ([]NetworkDeviceSensor, error)
	UpsertDeviceSensors(ctx context.Context, sensors []NetworkDeviceSensor) error
	ListDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID) ([]PhysicalEntity, error)
	UpsertDevicePhysicalEntities(ctx context.Context, tenantID, deviceID ID, entities []PhysicalEntity) error
	ListDeviceVLANs(ctx context.Context, tenantID, deviceID ID) ([]DeviceVLAN, error)
	UpsertDeviceVLANs(ctx context.Context, tenantID, deviceID ID, vlans []DeviceVLAN) error
	ListDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID) ([]DeviceLAGGroup, error)
	UpsertDeviceLAGGroups(ctx context.Context, tenantID, deviceID ID, groups []DeviceLAGGroup) error
	ListBGPSessions(ctx context.Context, tenantID, deviceID ID) ([]BGPSession, error)
	ListAllBGPSessions(ctx context.Context, tenantID ID) ([]BGPSession, error)
	GetBGPSession(ctx context.Context, tenantID, sessionID ID) (BGPSession, error)
	UpsertBGPSessions(ctx context.Context, sessions []BGPSession) error
	ReplaceBGPSessions(ctx context.Context, tenantID, deviceID ID, sessions []BGPSession) error
	GetPortPolicy(ctx context.Context, tenantID, portID ID) (PortPolicy, error)
	UpsertPortPolicy(ctx context.Context, policy PortPolicy) (PortPolicy, error)
	GetTrafficPolicyDefaults(ctx context.Context, tenantID ID) (TrafficPolicyDefaults, error)
	UpsertTrafficPolicyDefault(ctx context.Context, policyDefault TrafficPolicyDefault) (TrafficPolicyDefault, error)
}

type RetentionRepository interface {
	ListRetentionPolicies(ctx context.Context, tenantID ID) ([]MetricRetentionPolicy, error)
	UpsertRetentionPolicy(ctx context.Context, policy MetricRetentionPolicy) (MetricRetentionPolicy, error)
	DeleteRetentionPolicy(ctx context.Context, tenantID, policyID ID) error
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
	MarkExportComplete(ctx context.Context, tenantID, taskID ID, fileRef string) error
	MarkExportFailed(ctx context.Context, tenantID, taskID ID, message string) error
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

type AggregateGraphRepository interface {
	ListAggregateGraphs(ctx context.Context, tenantID ID) ([]AggregateGraph, error)
	GetAggregateGraph(ctx context.Context, tenantID, graphID ID) (AggregateGraph, error)
	CreateAggregateGraph(ctx context.Context, graph AggregateGraph) (AggregateGraph, error)
	UpdateAggregateGraph(ctx context.Context, graph AggregateGraph) (AggregateGraph, error)
	DeleteAggregateGraph(ctx context.Context, tenantID, graphID ID) error
	ListAggregateGraphItems(ctx context.Context, tenantID, graphID ID) ([]AggregateGraphItem, error)
	ReplaceAggregateGraphItems(ctx context.Context, tenantID, graphID ID, items []AggregateGraphItem) error
	ListAggregateGraphPorts(ctx context.Context, tenantID, graphID ID) ([]AggregateGraphPort, error)
	ReplaceAggregateGraphPorts(ctx context.Context, tenantID, graphID ID, ports []AggregateGraphPort) error
	AppendAggregateGraphData(ctx context.Context, point AggregateGraphDataPoint) error
	ListAggregateGraphData(ctx context.Context, tenantID, graphID ID, start, end time.Time) ([]AggregateGraphDataPoint, error)
	ListAllAggregateGraphs(ctx context.Context) ([]AggregateGraph, error)
}

type ExportStatus string

const (
	ExportStatusPending  ExportStatus = "pending"
	ExportStatusRunning  ExportStatus = "running"
	ExportStatusComplete ExportStatus = "complete"
	ExportStatusFailed   ExportStatus = "failed"
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
	ExportFormatCSV ExportFormat = "csv"
)

type ExportTask struct {
	ID           ID
	TenantID     ID
	CreatedBy    ID
	TargetID     ID
	PortID       ID
	PeriodType   PeriodType
	RangeStart   time.Time
	RangeEnd     time.Time
	Step         time.Duration
	Aggregation  Aggregation
	ValueMode    ExportValueMode
	Format       ExportFormat
	Status       ExportStatus
	FileRef      string
	ErrorMessage string
	CreatedAt    time.Time
	UpdatedAt    time.Time
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

type MetricRetentionPolicy struct {
	ID                   ID
	TenantID             ID
	TargetID             ID
	HighPrecisionDays    uint32
	ManualCleanupEnabled bool
	Notes                string
	CreatedAt            time.Time
	UpdatedAt            time.Time
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
