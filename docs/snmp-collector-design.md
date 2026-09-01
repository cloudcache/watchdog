# SNMP Collector Design

## 1. Hard Rules

This collector is a Go implementation of the LibreNMS SNMP collection model.

It is not an external collector configuration generator.

It is not an OID lookup table.

It is not a collector that stores business-adjusted values.

Non-negotiable rules:

1. Use Go and gosnmp for collection.
2. Discovery creates collection recipes.
3. Polling executes collection recipes.
4. Trap handlers process events and local state changes.
5. VictoriaMetrics stores raw SNMP-derived values only.
6. API, charts, exports, and billing calculate rate, unit conversion, and correction.
7. Device support is added through OS definitions, discovery modules, poller modules, and trap handlers.
8. Device support is not added through `vendor + metric_name + oid`.
9. Every stored time series is scoped by `tenant_id`, `device_id`, and `recipe_id`.
10. No external collector configuration path exists in this design.
11. No alternate collector path exists in this design.

## 2. Correct Model

The correct model is:

```text
LibreNMS definitions + modules + trap handlers
  -> OS detection
  -> discovery modules
  -> assets
  -> collection recipes
  -> poller execution
  -> raw samples
  -> API projection
```

## 3. LibreNMS Model To Reimplement

### 3.1 OS Detection

The collector starts from core SNMP facts:

```text
SNMPv2-MIB::sysObjectID.0
SNMPv2-MIB::sysDescr.0
SNMPv2-MIB::sysName.0
SNMPv2-MIB::sysUpTime.0
SNMP-FRAMEWORK-MIB::snmpEngineID.0
```

OS detection evaluates definitions equivalent to LibreNMS `includes/definitions/*.yaml`:

```text
sysObjectID exact prefix
sysObjectID regex
sysDescr regex
sysName regex
snmpget comparison
snmpwalk existence
negative match rules
```

Output:

```go
type SNMPOSFingerprint struct {
	SysObjectID  string
	SysDescr     string
	SysName      string
	SysUpTime    uint64
	SNMPEngineID string
}

type SNMPOSMatch struct {
	OSName  string
	OSGroup string
	Vendor  string
	Class   string
	Model   string
	Reason  string
}
```

The OS name controls module selection, MIB dependencies, bad interface rules,
sensor YAML rules, BGP strategy, and trap behavior.

### 3.2 Module Selection

Every module has explicit discovery and polling capability.

```go
type SNMPModuleDefinition struct {
	Name              string
	DiscoveryEnabled  bool
	PollingEnabled    bool
	DiscoveryRequires []string
	PollingRequires   []string
	OSAllow           []string
	OSDeny            []string
	DeviceClassAllow  []string
}
```

Required modules:

```text
core
ports
sensors
processors
memory
storage
bgp-peers
vlans
lags
entity-physical
os
```

No module receives a vendor OID lookup table. Modules receive:

```text
device facts
OS definition
pre-cache data
SNMP query engine
asset repository
recipe compiler
```

### 3.3 Pre-Cache

LibreNMS avoids repeated walks by pre-caching tables per OS/module.

Go equivalent:

```go
type SNMPPreCacheRequest struct {
	Module string
	MIB    string
	OID    string
	Mode   string // walk, get
	Flags  SNMPQueryFlags
}

type SNMPPreCache struct {
	ByOID   map[string]SNMPResponse
	ByTable map[string]SNMPTable
}
```

Rules:

1. Same table is walked once per discovery run.
2. Pre-cache is scoped to one device discovery.
3. Pre-cache output is not stored as time series.
4. Pre-cache feeds discovery modules and recipe compilation.

### 3.4 Discovery Output

Discovery writes two kinds of data:

1. Assets:
   - devices
   - ports
   - sensors
   - physical entities
   - BGP sessions
   - VLANs
   - LAGs
   - OS metadata

2. Collection recipes:
   - exact OID
   - numeric OID
   - entity binding
   - metric name
   - value type
   - sample interval
   - context name
   - divisor
   - multiplier
   - state translation
   - SNMP flags

Polling reads recipes. Polling does not rediscover support by guessing OIDs.

### 3.5 Polling

Polling uses known recipes.

The normal path is BatchGet:

```text
recipe.numeric_oid -> gosnmp.Get()
```

Walks in polling are allowed only when a poller module explicitly marks a
recipe group as table-refresh. The default is exact OID polling.

### 3.6 Trap Handlers

Trap handlers are first-class modules.

Required handlers:

```text
linkDown
linkUp
authenticationFailure
bgpBackwardTransition
coldStart
warmStart
```

Handler output:

```text
event log
asset state update
targeted immediate poll request
rediscovery request for unknown entity
```

Trap handlers do not create high-frequency metrics.

## 4. Storage Model

### 4.1 `snmp_os_definitions`

```sql
CREATE TABLE snmp_os_definitions (
  id CHAR(26) PRIMARY KEY,
  os_name VARCHAR(128) NOT NULL,
  os_group VARCHAR(128) NOT NULL DEFAULT '',
  vendor VARCHAR(128) NOT NULL DEFAULT '',
  class VARCHAR(64) NOT NULL DEFAULT '',
  definition_json JSON NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  source_version VARCHAR(128) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_os_definitions_name_source (os_name, source)
);
```

`definition_json` contains OS detection rules, module defaults, MIB list,
bad interface rules, and discovery options.

### 4.2 `snmp_module_definitions`

```sql
CREATE TABLE snmp_module_definitions (
  id CHAR(26) PRIMARY KEY,
  module_name VARCHAR(96) NOT NULL,
  module_type VARCHAR(32) NOT NULL,
  definition_json JSON NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  source_version VARCHAR(128) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_module_definitions_name_type_source (module_name, module_type, source)
);
```

`module_type`:

```text
discovery
poller
trap
```

### 4.3 `snmp_device_modules`

```sql
CREATE TABLE snmp_device_modules (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  module_name VARCHAR(96) NOT NULL,
  discovery_enabled TINYINT(1) NOT NULL DEFAULT 1,
  polling_enabled TINYINT(1) NOT NULL DEFAULT 1,
  discovery_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  polling_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  last_discovered_at DATETIME(3) NULL,
  last_polled_at DATETIME(3) NULL,
  last_error TEXT NULL,
  metadata_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_device_modules (tenant_id, device_id, module_name),
  KEY idx_snmp_device_modules_status (tenant_id, device_id, polling_enabled)
);
```

### 4.4 `snmp_collection_recipes`

This is the collector's central table.

```sql
CREATE TABLE snmp_collection_recipes (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  entity_type VARCHAR(48) NOT NULL,
  entity_id CHAR(26) NULL,
  module_name VARCHAR(64) NOT NULL,
  metric_name VARCHAR(160) NOT NULL,
  value_type VARCHAR(32) NOT NULL,
  oid VARCHAR(512) NOT NULL,
  numeric_oid VARCHAR(512) NOT NULL,
  oid_index VARCHAR(96) NOT NULL DEFAULT '',
  mib VARCHAR(128) NOT NULL DEFAULT '',
  context_name VARCHAR(96) NOT NULL DEFAULT '',
  poller_type VARCHAR(48) NOT NULL DEFAULT 'snmp',
  divisor DOUBLE NULL,
  multiplier DOUBLE NULL,
  user_func VARCHAR(128) NOT NULL DEFAULT '',
  state_map_id CHAR(26) NULL,
  unit VARCHAR(64) NOT NULL DEFAULT '',
  sample_interval_seconds INT UNSIGNED NOT NULL DEFAULT 60,
  labels_json JSON NULL,
  options_json JSON NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  discovered_at DATETIME(3) NOT NULL,
  last_seen_at DATETIME(3) NOT NULL,
  last_polled_at DATETIME(3) NULL,
  last_error TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_collection_recipe (
    tenant_id, device_id, module_name, entity_type, entity_id, metric_name, oid_index, context_name
  ),
  KEY idx_snmp_collection_due (tenant_id, enabled, sample_interval_seconds, last_polled_at),
  KEY idx_snmp_collection_device_module (tenant_id, device_id, module_name)
);
```

Allowed `entity_type` values:

```text
device
port
sensor
processor
memory
storage
bgp_peer
vlan
lag
physical_entity
```

Allowed `value_type` values:

```text
counter32
counter64
gauge
state
string
timeticks
ipaddr
macaddr
```

### 4.5 `snmp_state_translations`

```sql
CREATE TABLE snmp_state_translations (
  id CHAR(26) PRIMARY KEY,
  name VARCHAR(190) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  states_json JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_state_translations_name_source (name, source)
);
```

State example:

```json
[
  {"value": 1, "generic": 0, "label": "up"},
  {"value": 2, "generic": 2, "label": "down"}
]
```

### 4.6 `snmp_trap_handlers`

```sql
CREATE TABLE snmp_trap_handlers (
  id CHAR(26) PRIMARY KEY,
  trap_oid VARCHAR(512) NOT NULL,
  handler_key VARCHAR(190) NOT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  options_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_trap_handlers_oid (trap_oid)
);
```

### 4.7 `snmp_events`

```sql
CREATE TABLE snmp_events (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  entity_type VARCHAR(48) NOT NULL DEFAULT '',
  entity_id CHAR(26) NULL,
  source VARCHAR(32) NOT NULL,
  severity VARCHAR(32) NOT NULL DEFAULT 'info',
  event_type VARCHAR(96) NOT NULL,
  message TEXT NOT NULL,
  raw_json JSON NULL,
  occurred_at DATETIME(3) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_snmp_events_device_time (tenant_id, device_id, occurred_at),
  KEY idx_snmp_events_entity_time (tenant_id, entity_type, entity_id, occurred_at)
);
```

## 5. Go Types

### 5.1 Query Engine

```go
type SNMPQueryEngine interface {
	Get(ctx context.Context, req SNMPGetRequest) (SNMPResponse, error)
	Walk(ctx context.Context, req SNMPWalkRequest) (SNMPResponse, error)
}

type SNMPGetRequest struct {
	Target  SNMPTarget
	Profile SNMPProfile
	Context string
	OIDs    []string
	Flags   SNMPQueryFlags
}

type SNMPWalkRequest struct {
	Target  SNMPTarget
	Profile SNMPProfile
	Context string
	BaseOID string
	Flags   SNMPQueryFlags
}

type SNMPQueryFlags struct {
	UseBulk        bool
	MaxOids        int
	MaxRepetitions uint32
	NumericIndex   bool
	EnumStrings    bool
	AbortOnFailure bool
}
```

### 5.2 Discovery Module

```go
type SNMPDiscoveryModule interface {
	Name() string
	Discover(ctx context.Context, req SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error)
}

type SNMPCollectorDiscoveryContext struct {
	TenantID   ID
	TargetID   ID
	Device     NetworkDevice
	Profile    SNMPProfile
	OS         SNMPOSMatch
	Query      SNMPQueryEngine
	PreCache   SNMPPreCache
	Definition SNMPModuleDefinition
}

type SNMPCollectorDiscoveryResult struct {
	DeviceUpdates    NetworkDevice
	Ports            []NetworkPort
	Sensors          []NetworkDeviceSensor
	PhysicalEntities []PhysicalEntity
	BGPSessions      []BGPSession
	VLANs            []DeviceVLAN
	LAGs             []DeviceLAGGroup
	Recipes          []SNMPCollectionRecipe
	Events           []SNMPEvent
}
```

### 5.3 Poller

```go
type SNMPPoller struct {
	Repo   SNMPCollectorRepository
	Query  SNMPQueryEngine
	Writer SNMPRawSampleWriter
}

type SNMPPollJob struct {
	TenantID ID
	Device   NetworkDevice
	Profile  SNMPProfile
	Recipes  []SNMPCollectionRecipe
}
```

### 5.4 Raw Samples

```go
type SNMPRawSample struct {
	TenantID    ID
	TargetID    ID
	DeviceID    ID
	EntityType  string
	EntityID    ID
	RecipeID    ID
	MetricName  string
	ValueType   string
	FloatValue  float64
	StringValue string
	SampledAt   time.Time
	Labels      map[string]string
}
```

Collector-side normalization allowed:

```text
SNMP varbind -> number/string/state
divisor
multiplier
LibreNMS user_func for sensor decoding
state translation
```

Collector-side projection forbidden:

```text
Bps
pps
95th percentile
1000/1024 conversion
billing correction
display rounding
tenant policy correction
```

## 6. Discovery Algorithms

### 6.1 Core Discovery

Steps:

1. Query core OIDs.
2. Detect OS.
3. Store OS facts on `network_devices`.
4. Load module definitions for that OS.
5. Build pre-cache request list.
6. Execute pre-cache walks.
7. Execute discovery modules.
8. Upsert assets.
9. Upsert collection recipes.
10. Store module status.

Failure behavior:

- Core failure means device discovery failed.
- Module failure marks that module failed.
- A failed module does not delete recipes from a previous successful run because deletion is not part of this design.
- A module that wants to disable a recipe must emit an explicit disabled recipe record.

### 6.2 Ports Discovery

Required OIDs:

```text
IF-MIB::ifDescr
IF-MIB::ifName
IF-MIB::ifAlias
IF-MIB::ifType
IF-MIB::ifSpeed
IF-MIB::ifHighSpeed
IF-MIB::ifAdminStatus
IF-MIB::ifOperStatus
IF-MIB::ifConnectorPresent
```

Validation:

```text
reject bad ifType from OS definition
reject bad ifName regex from OS definition
reject bad ifDescr regex from OS definition
reject empty ifIndex
```

Port identity:

```text
default: ifIndex
OS override: ifName
OS override: ifDescr
```

Recipes per valid port:

```text
IF-MIB::ifHCInOctets.{ifIndex}        -> watchdog_snmp_if_in_octets_total
IF-MIB::ifHCOutOctets.{ifIndex}       -> watchdog_snmp_if_out_octets_total
IF-MIB::ifInErrors.{ifIndex}          -> watchdog_snmp_if_in_errors_total
IF-MIB::ifOutErrors.{ifIndex}         -> watchdog_snmp_if_out_errors_total
IF-MIB::ifInDiscards.{ifIndex}        -> watchdog_snmp_if_in_discards_total
IF-MIB::ifOutDiscards.{ifIndex}       -> watchdog_snmp_if_out_discards_total
IF-MIB::ifAdminStatus.{ifIndex}       -> watchdog_snmp_if_admin_status
IF-MIB::ifOperStatus.{ifIndex}        -> watchdog_snmp_if_oper_status
```

Fallback:

```text
ifHCInOctets missing  -> ifInOctets, value_type counter32
ifHCOutOctets missing -> ifOutOctets, value_type counter32
```

### 6.3 Sensors Discovery

Sources:

```text
ENTITY-SENSOR-MIB
LibreNMS YAML discovery definitions
OS-specific Go discovery module
```

Every sensor recipe stores:

```text
sensor_class
sensor_index
oid
numeric_oid
divisor
multiplier
user_func
state_map_id
unit
entPhysicalIndex
group
poller_type
```

Sensor raw metrics:

```text
watchdog_snmp_sensor_value
watchdog_snmp_sensor_state
```

### 6.4 BGP Discovery

Required strategies:

```text
generic BGP4-MIB
Cisco cbgpPeer2
Juniper BGP MIB
Arista BGP MIB
Huawei VRP BGP MIB
VRF/context polling
```

Recipes per peer:

```text
peer state
accepted prefixes
active prefixes
denied prefixes
messages in
messages out
established time
last error
```

`context_name` is part of the recipe key.

## 7. Polling Algorithm

Poller input:

```text
enabled recipes whose sample interval is due
```

Grouping key:

```text
tenant_id
device_id
profile_id
target host
target port
context_name
query flags
sample_interval_seconds
```

Execution:

1. Build one gosnmp session per group.
2. Split exact OIDs into chunks.
3. Execute Get per chunk.
4. Convert varbind to raw sample using recipe metadata.
5. Write samples to VictoriaMetrics.
6. Record per-recipe poll status.

Defaults:

```text
global_concurrency = 32
per_device_concurrency = 1
max_oids_per_get = 60
max_repetitions = 25
request_timeout = 10s
job_deadline = 30s
```

Backoff:

```text
one timeout: record error
three consecutive timeouts: skip next due interval
ten consecutive timeouts: mark module polling_status failed
successful poll: clear consecutive timeout count
```

No panic is swallowed. Panic recovery records:

```text
device_id
module_name
recipe_id
stack
```

## 8. VictoriaMetrics Metrics

### 8.1 Stored Metrics

Port counters:

```text
watchdog_snmp_if_in_octets_total
watchdog_snmp_if_out_octets_total
watchdog_snmp_if_in_errors_total
watchdog_snmp_if_out_errors_total
watchdog_snmp_if_in_discards_total
watchdog_snmp_if_out_discards_total
```

Port states:

```text
watchdog_snmp_if_admin_status
watchdog_snmp_if_oper_status
```

Sensors:

```text
watchdog_snmp_sensor_value
watchdog_snmp_sensor_state
```

BGP:

```text
watchdog_snmp_bgp_peer_state
watchdog_snmp_bgp_prefixes_accepted
watchdog_snmp_bgp_prefixes_denied
watchdog_snmp_bgp_prefixes_active
watchdog_snmp_bgp_messages_in_total
watchdog_snmp_bgp_messages_out_total
```

### 8.2 Labels

Required labels on every SNMP sample:

```text
tenant_id
device_id
recipe_id
entity_type
entity_id
module
```

Reason:

```text
Different devices expose different OIDs, indexes, sensor classes, BGP contexts,
and vendor module behavior. The metric name describes the raw value family;
device_id identifies the device boundary; recipe_id identifies the exact
discovered collection recipe for that device/entity/metric/context.
```

Allowed additional labels:

```text
target_id
port_id
if_index
if_name
sensor_id
sensor_class
bgp_session_id
afi
safi
module
```

Forbidden identity model:

```text
metric_name alone
metric_name + vendor
dynamic per-device metric names
OID as label
numeric OID as label
```

Forbidden labels:

```text
oid
numeric_oid
if_alias
if_descr
sensor_descr
raw trap text
long device description
```

## 9. API Projection

### 9.1 Port Rate

API receives:

```text
metric = watchdog_snmp_if_in_bps
```

API queries:

```text
rate(watchdog_snmp_if_in_octets_total[window]) * 8
```

API receives:

```text
metric = watchdog_snmp_if_out_bps
```

API queries:

```text
rate(watchdog_snmp_if_out_octets_total[window]) * 8
```

### 9.2 Billing Bytes

Billing uses raw counter deltas:

```text
delta = current - previous
```

Counter reset handling:

```text
current >= previous: use current - previous
current < previous and counter64: treat as reset and skip segment
current < previous and counter32: use recipe option counter_bits=32 only when device definition allows wrap repair
```

### 9.3 Unit Conversion

API applies unit base:

```text
decimal: 1000
binary: 1024
```

The collector never applies this.

### 9.4 Correction

Correction applies only when all conditions are true:

```text
metric family = traffic
request value mode = corrected or both
caller has permission
port policy exists
```

Correction never applies to:

```text
CPU
memory
sensor value
sensor state
BGP state
BGP prefixes
interface admin status
interface oper status
```

## 10. Trap Handling

### 10.1 Input

HTTP endpoint:

```text
POST /api/v1/snmp/traps
```

Request:

```json
{
  "source_ip": "192.0.2.10",
  "hostname": "sw1",
  "trap_oid": ".1.3.6.1.6.3.1.1.5.3",
  "uptime": 123456,
  "varbinds": [
    {"oid": "IF-MIB::ifIndex.12", "value": "12"}
  ],
  "raw_text": "..."
}
```

### 10.2 Handler Registry

```go
type SNMPTrapHandler interface {
	Handle(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
}

type SNMPTrapHandleResult struct {
	Events              []SNMPEvent
	PortUpdates         []NetworkPort
	BGPUpdates          []BGPSession
	ImmediatePollRecipe []ID
	RediscoverDevice    bool
}
```

Required handlers:

```text
linkDown
linkUp
authenticationFailure
bgpBackwardTransition
coldStart
warmStart
```

### 10.3 Link Handler

Input:

```text
trap oid = linkDown or linkUp
varbind contains ifIndex
```

Action:

```text
find network_ports by device_id + if_index
update cached oper_status
write snmp_events
enqueue immediate poll for that port's recipes
```

Unknown ifIndex:

```text
write event_type = unknown_interface_trap
set RediscoverDevice = true
```

### 10.4 BGP Handler

Input:

```text
trap oid = bgpBackwardTransition
peer address from varbind or OID suffix
```

Action:

```text
find bgp_sessions by device_id + peer_addr + context_name
update cached state
write snmp_events
enqueue immediate poll for that peer's recipes
```

Unknown peer:

```text
write event_type = unknown_bgp_peer_trap
set RediscoverDevice = true
```

## 11. Definition Import

Importer input:

```text
LibreNMS checkout path
```

Importer reads:

```text
includes/definitions/*.yaml
includes/definitions/discovery/*.yaml
LibreNMS/OS/*.php metadata that can be represented safely
LibreNMS/Snmptrap/Handlers handler list
config/snmptraps.php trap OID map
```

Importer outputs:

```text
snmp_os_definitions
snmp_module_definitions
snmp_state_translations
snmp_trap_handlers
```

Importer does not output:

```text
vendor metric lookup records
external collector config
```

PHP handling:

```text
YAML definitions are imported directly.
PHP OS behavior is represented by named Go modules.
Unsupported PHP behavior is recorded as missing module coverage.
Missing module coverage fails tests for that OS until implemented.
```

## 12. Implementation Files

Create these files:

```text
deploy/migration/mysql/007_snmp_collector.sql
internal/watchdog/snmp_collector_types.go
internal/watchdog/snmp_collector_repository.go
internal/watchdog/mysql_snmp_collector_repository.go
internal/watchdog/snmp_query_engine.go
internal/watchdog/snmp_definition_importer.go
internal/watchdog/snmp_discovery_engine.go
internal/watchdog/snmp_discovery_ports.go
internal/watchdog/snmp_discovery_sensors.go
internal/watchdog/snmp_discovery_bgp.go
internal/watchdog/snmp_poller.go
internal/watchdog/snmp_raw_writer.go
internal/watchdog/snmp_trap_dispatcher.go
internal/watchdog/snmp_trap_handlers.go
internal/watchdog/api_snmp_collector.go
```

Delete these from the new collector path:

```text
vendor metric lookup dependency
external collector config dependency
collector-side Bps write
collector-side correction write
```

## 13. Tests That Must Exist

### 13.1 Definition Import Tests

```text
imports OS detection rules
imports YAML sensor discovery rules
imports state translations
imports trap OID handler registry
does not create vendor metric lookup records
does not create external collector config
```

### 13.2 Discovery Tests

```text
detects OS from sysObjectID
creates port assets from IF-MIB
rejects bad interface rules
creates counter64 recipes when ifHC exists
creates counter32 recipes when ifHC is missing
creates ENTITY-SENSOR-MIB recipes
creates BGP recipes with context_name
does not read vendor metric lookup records
```

### 13.3 Poller Tests

```text
groups recipes by device/profile/context
chunks GET requests
writes raw octets
writes raw sensor value
writes raw BGP state
does not write watchdog_snmp_if_in_bps
does not write watchdog_snmp_if_out_bps
records partial OID failures
records panic with stack
```

### 13.4 API Tests

```text
rewrites if_in_bps query to rate(if_in_octets_total) * 8
rewrites if_out_bps query to rate(if_out_octets_total) * 8
applies decimal unit conversion in API
applies binary unit conversion in API
applies correction only to traffic metrics
does not apply correction to sensor or BGP metrics
```

### 13.5 Trap Tests

```text
linkDown updates port state
linkUp updates port state
unknown interface trap creates event and rediscovery request
bgpBackwardTransition updates peer state
unknown BGP peer creates event and rediscovery request
unregistered trap OID creates unhandled event
```

## 14. Acceptance Criteria

The design is implemented only when all statements are true:

1. Vendor metric lookup records are absent from collector, discovery, poller, importer, and trap code.
2. No collector file imports or generates external collector configuration.
3. No collector path writes `watchdog_snmp_if_in_bps`.
4. No collector path writes `watchdog_snmp_if_out_bps`.
5. Discovery produces `snmp_collection_recipes`.
6. Poller consumes `snmp_collection_recipes`.
7. Trap dispatcher uses `snmp_trap_handlers`.
8. VM receives raw counter/gauge/state metrics only.
9. API derives rate/unit/correction.
10. Tests fail if vendor metric lookup records or external collector configuration re-enter the collector path.
