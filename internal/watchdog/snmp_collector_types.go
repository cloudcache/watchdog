package watchdog

import (
	"context"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

type SNMPCollectorEntityType string

const (
	SNMPCollectorEntityDevice         SNMPCollectorEntityType = "device"
	SNMPCollectorEntityPort           SNMPCollectorEntityType = "port"
	SNMPCollectorEntitySensor         SNMPCollectorEntityType = "sensor"
	SNMPCollectorEntityProcessor      SNMPCollectorEntityType = "processor"
	SNMPCollectorEntityMemory         SNMPCollectorEntityType = "memory"
	SNMPCollectorEntityStorage        SNMPCollectorEntityType = "storage"
	SNMPCollectorEntityBGPPeer        SNMPCollectorEntityType = "bgp_peer"
	SNMPCollectorEntityVLAN           SNMPCollectorEntityType = "vlan"
	SNMPCollectorEntityLAG            SNMPCollectorEntityType = "lag"
	SNMPCollectorEntityPhysicalEntity SNMPCollectorEntityType = "physical_entity"
)

type SNMPCollectorValueType string

const (
	SNMPCollectorValueCounter32 SNMPCollectorValueType = "counter32"
	SNMPCollectorValueCounter64 SNMPCollectorValueType = "counter64"
	SNMPCollectorValueGauge     SNMPCollectorValueType = "gauge"
	SNMPCollectorValueState     SNMPCollectorValueType = "state"
	SNMPCollectorValueString    SNMPCollectorValueType = "string"
	SNMPCollectorValueTimeTicks SNMPCollectorValueType = "timeticks"
	SNMPCollectorValueIPAddr    SNMPCollectorValueType = "ipaddr"
	SNMPCollectorValueMACAddr   SNMPCollectorValueType = "macaddr"
)

type SNMPCollectorModuleType = snmpdomain.ModuleType

const (
	SNMPCollectorModuleDiscovery = snmpdomain.ModuleDiscovery
	SNMPCollectorModulePoller    = snmpdomain.ModulePoller
	SNMPCollectorModuleTrap      = snmpdomain.ModuleTrap
)

type SNMPCollectorModuleStatus string

const (
	SNMPCollectorModuleUnknown SNMPCollectorModuleStatus = "unknown"
	SNMPCollectorModuleOK      SNMPCollectorModuleStatus = "ok"
	SNMPCollectorModuleFailed  SNMPCollectorModuleStatus = "failed"
	SNMPCollectorModuleSkipped SNMPCollectorModuleStatus = "skipped"
)

type SNMPCollectorOSFingerprint = snmpdomain.OSFingerprint

type SNMPCollectorOSMatch = snmpdomain.OSMatch

type SNMPCollectorOSDefinition = snmpdomain.OSDefinition

type SNMPCollectorModuleDefinition = snmpdomain.ModuleDefinition

type SNMPCollectorDeviceModule struct {
	ID               ID
	TenantID         ID
	DeviceID         ID
	ModuleName       string
	DiscoveryEnabled bool
	PollingEnabled   bool
	DiscoveryStatus  SNMPCollectorModuleStatus
	PollingStatus    SNMPCollectorModuleStatus
	LastDiscoveredAt time.Time
	LastPolledAt     time.Time
	LastError        string
	Metadata         map[string]string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type SNMPCollectorQueryFlags struct {
	UseBulk        bool
	MaxOids        int
	MaxRepetitions uint32
	NumericIndex   bool
	EnumStrings    bool
	AbortOnFailure bool
}

type SNMPCollectorTarget struct {
	Host string
	Port uint16
}

type SNMPCollectorGetRequest struct {
	Target  SNMPCollectorTarget
	Profile SNMPProfile
	Context string
	OIDs    []string
	Flags   SNMPCollectorQueryFlags
}

type SNMPCollectorWalkRequest struct {
	Target  SNMPCollectorTarget
	Profile SNMPProfile
	Context string
	BaseOID string
	Flags   SNMPCollectorQueryFlags
}

type SNMPCollectorVarBind struct {
	OID       string
	Value     any
	ValueType SNMPCollectorValueType
}

type SNMPCollectorResponse struct {
	VarBinds []SNMPCollectorVarBind
}

type SNMPCollectorTable struct {
	Rows map[string]map[string]SNMPCollectorVarBind
}

type SNMPCollectorPreCacheRequest struct {
	Module string
	MIB    string
	OID    string
	Mode   string
	Flags  SNMPCollectorQueryFlags
}

type SNMPCollectorPreCache struct {
	ByOID   map[string]SNMPCollectorResponse
	ByTable map[string]SNMPCollectorTable
}

type SNMPCollectorDiscoveryModule interface {
	Name() string
	Discover(ctx context.Context, req SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error)
}

type SNMPCollectorDiscoveryContext struct {
	TenantID   ID
	TargetID   ID
	Target     SNMPCollectorTarget
	Device     NetworkDevice
	Profile    SNMPProfile
	OS         SNMPCollectorOSMatch
	Query      SNMPCollectorQueryEngine
	PreCache   SNMPCollectorPreCache
	Definition SNMPCollectorModuleDefinition
	// OSDiscovery is the matched OS's LibreNMS os_discovery "modules" map;
	// definition-driven modules walk what it declares instead of hardcoding
	// vendor branches.
	OSDiscovery map[string]any
}

type SNMPCollectorDiscoveryResult struct {
	DeviceUpdates      NetworkDevice
	CompletedModules   []string
	Ports              []NetworkPort
	InterfaceAddresses []NetworkInterfaceAddress
	Sensors            []NetworkDeviceSensor
	PhysicalEntities   []PhysicalEntity
	BGPSessions        []BGPSession
	VLANs              []DeviceVLAN
	LAGs               []DeviceLAGGroup
	Recipes            []SNMPCollectionRecipe
	Events             []SNMPEvent
}

type SNMPCollectionRecipe struct {
	ID                    ID
	TenantID              ID
	DeviceID              ID
	EntityType            SNMPCollectorEntityType
	EntityID              ID
	ModuleName            string
	MetricName            string
	ValueType             SNMPCollectorValueType
	OID                   string
	NumericOID            string
	OIDIndex              string
	MIB                   string
	ContextName           string
	PollerType            string
	Divisor               float64
	HasDivisor            bool
	Multiplier            float64
	HasMultiplier         bool
	UserFunc              string
	StateMapID            ID
	Unit                  string
	SampleIntervalSeconds uint32
	Labels                map[string]string
	Options               map[string]string
	Enabled               bool
	DiscoveredAt          time.Time
	LastSeenAt            time.Time
	LastPolledAt          time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type SNMPStateTranslation = snmpdomain.StateTranslation

type SNMPStateValue = snmpdomain.SNMPStateValue

type SNMPRawSample struct {
	TenantID     ID
	TargetID     ID
	DeviceID     ID
	EntityType   SNMPCollectorEntityType
	EntityID     ID
	RecipeID     ID
	MetricName   string
	ValueType    SNMPCollectorValueType
	FloatValue   float64
	CounterValue uint64
	CounterValid bool
	CounterWidth uint8
	StringValue  string
	SampledAt    time.Time
	IntervalMS   uint32
	QualityFlags uint32
	PollSequence uint64
	SourceRunID  string
	SampleIndex  uint32
	Labels       map[string]string
}

type SNMPTrapHandlerDefinition = snmpdomain.TrapHandlerDefinition

type SNMPTrap struct {
	SourceIP   string
	Hostname   string
	TrapOID    string
	Uptime     uint64
	VarBinds   []SNMPTrapVarBind
	ReceivedAt time.Time
	RawText    string
}

type SNMPTrapVarBind struct {
	OID   string
	Value string
}

type SNMPEvent struct {
	ID         ID
	TenantID   ID
	DeviceID   ID
	EntityType SNMPCollectorEntityType
	EntityID   ID
	Source     string
	Severity   string
	EventType  string
	Message    string
	Raw        map[string]any
	OccurredAt time.Time
	CreatedAt  time.Time
}

type SNMPTrapHandleResult struct {
	Events              []SNMPEvent
	PortUpdates         []NetworkPort
	BGPUpdates          []BGPSession
	ImmediatePollRecipe []ID
	RediscoverDevice    bool
}
