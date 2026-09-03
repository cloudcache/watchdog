package watchdog

import "time"

type ID string

type Tenant struct {
	ID        ID
	Name      string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type User struct {
	ID        ID
	TenantID  ID
	Email     string
	Name      string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TargetType string

const (
	TargetTypeSystem  TargetType = "system"
	TargetTypeNetwork TargetType = "network"
)

type AgentMode string

const (
	AgentModePull AgentMode = "pull"
	AgentModePush AgentMode = "push"
)

type AgentType string

const (
	AgentTypeSNMP   AgentType = "snmp"
	AgentTypeSystem AgentType = "system"
)

type Target struct {
	ID        ID
	TenantID  ID
	Name      string
	Type      TargetType
	Host      string
	Status    string
	Labels    map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type NetworkDevice struct {
	ID            ID
	TenantID      ID
	TargetID      ID
	Vendor        string
	Model         string
	Platform      string
	OSName        string
	OSVersion     string
	SysObjectID   string
	SysName       string
	SysDescr      string
	SysLocation   string
	Uptime        time.Duration
	SNMPProfileID ID
	SNMPPort      uint16
	SNMPSecurity  map[string]string `json:"-"`
}

type NetworkPort struct {
	ID          ID
	TenantID    ID
	DeviceID    ID
	IfIndex     uint64
	IfName      string
	IfAlias     string
	IfDescr     string
	AdminStatus string
	OperStatus  string
	SpeedBps    uint64
	Metadata    map[string]string
}

// NormalizeIfStatus canonicalizes IF-MIB ifAdminStatus/ifOperStatus values to
// their enum names. Discovery walks yield raw integers ("1") while trap
// handlers and forms use names ("up"); storage and comparisons use names only.
func NormalizeIfStatus(value string) string {
	switch value {
	case "1":
		return "up"
	case "2":
		return "down"
	case "3":
		return "testing"
	case "4":
		return "unknown"
	case "5":
		return "dormant"
	case "6":
		return "notPresent"
	case "7":
		return "lowerLayerDown"
	}
	return value
}

type NetworkPortTransceiver struct {
	ID           ID
	TenantID     ID
	PortID       ID
	ModuleType   string
	Vendor       string
	Model        string
	Serial       string
	WavelengthNM uint64
	DistanceM    uint64
	Connector    string
	Raw          map[string]any
	UpdatedAt    time.Time
}

type NetworkDeviceSensor struct {
	ID          ID
	TenantID    ID
	DeviceID    ID
	PortID      ID
	SensorIndex uint64
	Class       string
	Name        string
	OID         string
	Unit        string
	Value       float64
	WarnLimit   float64
	CritLimit   float64
	Status      string
	Metadata    map[string]string
	UpdatedAt   time.Time
}

// PhysicalEntity is one row from ENTITY-MIB::entPhysicalTable, discovered via a
// standard MIB walk. Gives hardware inventory: modules, line cards, power
// supplies, fans, serials, models — whatever the device exposes.
type PhysicalEntity struct {
	Index            uint64
	Name             string
	Description      string
	Class            string
	VendorType       string
	ContainedIn      uint64
	ParentRelPos     int
	HardwareRevision string
	FirmwareRevision string
	SoftwareRevision string
	SerialNumber     string
	ManufacturerName string
	ModelName        string
	Alias            string
	AssetID          string
	IsFRU            bool
}

// DeviceVLAN is one VLAN discovered from Q-BRIDGE-MIB::dot1qVlanStaticTable.
type DeviceVLAN struct {
	VLANID uint32
	Name   string
	Status string
}

// DeviceLAGGroup is one link-aggregation group from IEEE8023-LAG-MIB::dot3adAggTable.
type DeviceLAGGroup struct {
	AggregateIndex uint64
	MACAddress     string
	Mode           string
}

type BGPSession struct {
	ID                 ID
	TenantID           ID
	DeviceID           ID
	PeerAddr           string
	PeerAS             uint64
	LocalAS            uint64
	AFI                string
	SAFI               string
	State              string
	AcceptedPrefixes   uint64
	DeniedPrefixes     uint64
	AdvertisedPrefixes uint64
	Uptime             time.Duration
	Metadata           map[string]string
	UpdatedAt          time.Time
}

type AggregateGraphMethod string

const (
	AggregateSum   AggregateGraphMethod = "sum"
	AggregateAvg   AggregateGraphMethod = "avg"
	AggregateMax   AggregateGraphMethod = "max"
	AggregateMin   AggregateGraphMethod = "min"
	AggregateCount AggregateGraphMethod = "count"
)

type AggregateGraph struct {
	ID          ID
	TenantID    ID
	Name        string
	Aggregation AggregateGraphMethod
	ValueMode   MetricValueMode
	Unit        string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type AggregateGraphItem struct {
	ID        ID
	TenantID  ID
	GraphID   ID
	Sequence  uint32
	Metric    string
	Direction string
	Label     string
	GraphType string
	Total     bool
	CreatedAt time.Time
}

type AggregateGraphPort struct {
	AggregateGraphID ID
	TenantID         ID
	PortID           ID
	CreatedAt        time.Time
}

// AggregateGraphDataPoint is one immutable stored aggregate sample for one
// graph item (metric/direction) at a point in time.
type AggregateGraphDataPoint struct {
	ID        ID
	TenantID  ID
	GraphID   ID
	ItemID    ID
	Timestamp time.Time
	Value     float64
	CreatedAt time.Time
}

type SNMPVersion string

const (
	SNMPVersion2c SNMPVersion = "2c"
	SNMPVersion3  SNMPVersion = "3"
)

type SNMPProfile struct {
	ID        ID
	TenantID  ID
	Name      string
	Version   SNMPVersion
	Security  map[string]string
	Timeout   time.Duration
	Retries   uint8
	CreatedAt time.Time
	UpdatedAt time.Time
}

type MIBModule struct {
	ID        ID
	Name      string
	Source    string
	Version   string
	Checksum  string
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type MetricLabelSet struct {
	TenantID ID
	TargetID ID
	DeviceID ID
	PortID   ID
	IfIndex  uint64
	SideType PortSideType
	Vendor   string
	Model    string
	IfName   string
}

type BGPMetricLabelSet struct {
	TenantID  ID
	TargetID  ID
	DeviceID  ID
	SessionID ID
	PeerAddr  string
	PeerAS    uint64
	AFI       string
	SAFI      string
	Vendor    string
	Model     string
}

const (
	MetricSystemCPUPercent    = "watchdog_system_cpu_percent"
	MetricSystemMemoryPercent = "watchdog_system_memory_percent"
	MetricSystemDiskPercent   = "watchdog_system_disk_percent"
	MetricSystemNetInBps      = "watchdog_system_net_in_bps"
	MetricSystemNetOutBps     = "watchdog_system_net_out_bps"
	MetricGPUCount            = "watchdog_gpu_count"
	MetricGPUUp               = "watchdog_gpu_up"
	MetricGPUUtilPercent      = "watchdog_gpu_utilization_percent"
	MetricContainerCPUPercent = "watchdog_container_cpu_percent"
	MetricContainerMemory     = "watchdog_container_memory_bytes"
	MetricContainerNetTxBps   = "watchdog_container_net_tx_bps"
	MetricContainerNetRxBps   = "watchdog_container_net_rx_bps"

	MetricSNMPIfInBps            = "watchdog_snmp_if_in_bps"
	MetricSNMPIfOutBps           = "watchdog_snmp_if_out_bps"
	MetricSNMPIfInOctetsTotal    = "watchdog_snmp_if_in_octets_total"
	MetricSNMPIfOutOctetsTotal   = "watchdog_snmp_if_out_octets_total"
	MetricSNMPIfOperStatus       = "watchdog_snmp_if_oper_status"
	MetricSNMPIfAdminStatus      = "watchdog_snmp_if_admin_status"
	MetricSNMPIfInErrorsTotal    = "watchdog_snmp_if_in_errors_total"
	MetricSNMPIfOutErrorsTotal   = "watchdog_snmp_if_out_errors_total"
	MetricSNMPIfInDiscardsTotal  = "watchdog_snmp_if_in_discards_total"
	MetricSNMPIfOutDiscardsTotal = "watchdog_snmp_if_out_discards_total"
	MetricSNMPIfInCRCTotal       = "watchdog_snmp_if_in_crc_total"
	MetricSNMPIfOutCRCTotal      = "watchdog_snmp_if_out_crc_total"
	MetricSNMPOpticalRxDBM       = "watchdog_snmp_optical_rx_dbm"
	MetricSNMPOpticalTxDBM       = "watchdog_snmp_optical_tx_dbm"
	MetricSNMPOpticalTempC       = "watchdog_snmp_optical_temp_celsius"
	MetricSNMPDeviceCPUPercent   = "watchdog_snmp_device_cpu_percent"
	MetricSNMPDeviceMemPercent   = "watchdog_snmp_device_memory_percent"
	MetricSNMPDeviceMemUsed      = "watchdog_snmp_device_memory_used_bytes"
	MetricSNMPDeviceMemTotal     = "watchdog_snmp_device_memory_total_bytes"
	MetricBGPState               = "watchdog_bgp_session_state"
	MetricBGPAcceptedPrefixes    = "watchdog_bgp_accepted_prefixes"
	MetricBGPDeniedPrefixes      = "watchdog_bgp_denied_prefixes"
	MetricBGPAdvertisedPrefixes  = "watchdog_bgp_advertised_prefixes"
)
