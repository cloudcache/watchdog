package metricdomain

import "time"

type Scope string

const (
	ScopeTarget Scope = "target"
	ScopeDevice Scope = "device"
	ScopePort   Scope = "port"
	ScopeBGP    Scope = "bgp_session"
)

type ValueMode string

const (
	ValueCorrected ValueMode = "corrected"
	ValueRaw       ValueMode = "raw"
	ValueBoth      ValueMode = "both"
)

type Definition struct {
	Name        string
	Family      string
	Scope       Scope
	Unit        string
	Description string
	DefaultStep time.Duration
	ValueModes  []ValueMode
}

const (
	SystemCPUPercent    = "watchdog_system_cpu_percent"
	SystemMemoryPercent = "watchdog_system_memory_percent"
	SystemDiskPercent   = "watchdog_system_disk_percent"
	SystemNetInBps      = "watchdog_system_net_in_bps"
	SystemNetOutBps     = "watchdog_system_net_out_bps"
	GPUCount            = "watchdog_gpu_count"
	GPUUp               = "watchdog_gpu_up"
	GPUUtilPercent      = "watchdog_gpu_utilization_percent"
	ContainerCPUPercent = "watchdog_container_cpu_percent"
	ContainerMemory     = "watchdog_container_memory_bytes"
	ContainerNetTxBps   = "watchdog_container_net_tx_bps"
	ContainerNetRxBps   = "watchdog_container_net_rx_bps"

	SNMPIfInBps            = "watchdog_snmp_if_in_bps"
	SNMPIfOutBps           = "watchdog_snmp_if_out_bps"
	SNMPIfInOctetsTotal    = "watchdog_snmp_if_in_octets_total"
	SNMPIfOutOctetsTotal   = "watchdog_snmp_if_out_octets_total"
	SNMPIfOperStatus       = "watchdog_snmp_if_oper_status"
	SNMPIfAdminStatus      = "watchdog_snmp_if_admin_status"
	SNMPIfInErrorsTotal    = "watchdog_snmp_if_in_errors_total"
	SNMPIfOutErrorsTotal   = "watchdog_snmp_if_out_errors_total"
	SNMPIfInDiscardsTotal  = "watchdog_snmp_if_in_discards_total"
	SNMPIfOutDiscardsTotal = "watchdog_snmp_if_out_discards_total"
	SNMPIfInCRCTotal       = "watchdog_snmp_if_in_crc_total"
	SNMPIfOutCRCTotal      = "watchdog_snmp_if_out_crc_total"
	SNMPOpticalRxDBM       = "watchdog_snmp_optical_rx_dbm"
	SNMPOpticalTxDBM       = "watchdog_snmp_optical_tx_dbm"
	SNMPOpticalTempC       = "watchdog_snmp_optical_temp_celsius"
	SNMPDeviceCPUPercent   = "watchdog_snmp_device_cpu_percent"
	SNMPProcessorUsage     = "watchdog_snmp_processor_usage_percent"
	SNMPDeviceMemPercent   = "watchdog_snmp_device_memory_percent"
	SNMPDeviceMemUsed      = "watchdog_snmp_device_memory_used_bytes"
	SNMPDeviceMemTotal     = "watchdog_snmp_device_memory_total_bytes"
	BGPState               = "watchdog_bgp_session_state"
	BGPAcceptedPrefixes    = "watchdog_bgp_accepted_prefixes"
	BGPDeniedPrefixes      = "watchdog_bgp_denied_prefixes"
	BGPAdvertisedPrefixes  = "watchdog_bgp_advertised_prefixes"
)

var Catalog = []Definition{
	{Name: SystemCPUPercent, Family: "system", Scope: ScopeTarget, Unit: "percent", Description: "Target CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SystemMemoryPercent, Family: "system", Scope: ScopeTarget, Unit: "percent", Description: "Target memory utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SystemDiskPercent, Family: "system", Scope: ScopeTarget, Unit: "percent", Description: "Target disk utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SystemNetInBps, Family: "system", Scope: ScopeTarget, Unit: "bps", Description: "Target inbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SystemNetOutBps, Family: "system", Scope: ScopeTarget, Unit: "bps", Description: "Target outbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: GPUCount, Family: "gpu", Scope: ScopeTarget, Unit: "count", Description: "GPU count", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: GPUUp, Family: "gpu", Scope: ScopeTarget, Unit: "state", Description: "GPU availability state", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: GPUUtilPercent, Family: "gpu", Scope: ScopeTarget, Unit: "percent", Description: "GPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: ContainerCPUPercent, Family: "container", Scope: ScopeTarget, Unit: "percent", Description: "Container CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: ContainerMemory, Family: "container", Scope: ScopeTarget, Unit: "bytes", Description: "Container memory usage", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: ContainerNetTxBps, Family: "container", Scope: ScopeTarget, Unit: "bps", Description: "Container outbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: ContainerNetRxBps, Family: "container", Scope: ScopeTarget, Unit: "bps", Description: "Container inbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPIfInBps, Family: "snmp_interface", Scope: ScopePort, Unit: "bps", Description: "SNMP interface inbound throughput", DefaultStep: time.Minute, ValueModes: allValueModes()},
	{Name: SNMPIfOutBps, Family: "snmp_interface", Scope: ScopePort, Unit: "bps", Description: "SNMP interface outbound throughput", DefaultStep: time.Minute, ValueModes: allValueModes()},
	{Name: SNMPIfInOctetsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "octets", Description: "SNMP interface inbound counter", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfOutOctetsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "octets", Description: "SNMP interface outbound counter", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfOperStatus, Family: "snmp_interface", Scope: ScopePort, Unit: "state", Description: "SNMP interface operational status", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPIfAdminStatus, Family: "snmp_interface", Scope: ScopePort, Unit: "state", Description: "SNMP interface administrative status", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPIfInErrorsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "errors", Description: "SNMP interface inbound errors", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfOutErrorsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "errors", Description: "SNMP interface outbound errors", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfInDiscardsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "discards", Description: "SNMP interface inbound discards", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfOutDiscardsTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "discards", Description: "SNMP interface outbound discards", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfInCRCTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "errors", Description: "SNMP interface inbound CRC errors", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPIfOutCRCTotal, Family: "snmp_interface", Scope: ScopePort, Unit: "errors", Description: "SNMP interface outbound CRC errors", DefaultStep: time.Minute, ValueModes: []ValueMode{ValueRaw}},
	{Name: SNMPOpticalRxDBM, Family: "snmp_optics", Scope: ScopePort, Unit: "dBm", Description: "Optical receive power", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPOpticalTxDBM, Family: "snmp_optics", Scope: ScopePort, Unit: "dBm", Description: "Optical transmit power", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPOpticalTempC, Family: "snmp_optics", Scope: ScopePort, Unit: "celsius", Description: "Optical module temperature", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPDeviceCPUPercent, Family: "snmp_device", Scope: ScopeDevice, Unit: "percent", Description: "SNMP device CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPProcessorUsage, Family: "snmp_device", Scope: ScopeDevice, Unit: "percent", Description: "SNMP processor usage", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPDeviceMemPercent, Family: "snmp_device", Scope: ScopeDevice, Unit: "percent", Description: "SNMP device memory utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPDeviceMemUsed, Family: "snmp_device", Scope: ScopeDevice, Unit: "bytes", Description: "SNMP device memory used", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: SNMPDeviceMemTotal, Family: "snmp_device", Scope: ScopeDevice, Unit: "bytes", Description: "SNMP device memory total", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: BGPState, Family: "bgp", Scope: ScopeBGP, Unit: "state", Description: "BGP session state", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: BGPAcceptedPrefixes, Family: "bgp", Scope: ScopeBGP, Unit: "prefixes", Description: "BGP accepted prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: BGPDeniedPrefixes, Family: "bgp", Scope: ScopeBGP, Unit: "prefixes", Description: "BGP denied prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: BGPAdvertisedPrefixes, Family: "bgp", Scope: ScopeBGP, Unit: "prefixes", Description: "BGP advertised prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
}

func IsKnown(name string) bool {
	for _, metric := range Catalog {
		if metric.Name == name {
			return true
		}
	}
	return false
}

func correctedOnly() []ValueMode { return []ValueMode{ValueCorrected} }
func allValueModes() []ValueMode { return []ValueMode{ValueCorrected, ValueRaw, ValueBoth} }
