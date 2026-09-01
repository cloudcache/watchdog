package watchdog

import "time"

type MetricScope string

const (
	MetricScopeTarget MetricScope = "target"
	MetricScopeDevice MetricScope = "device"
	MetricScopePort   MetricScope = "port"
	MetricScopeBGP    MetricScope = "bgp_session"
)

type MetricDefinition struct {
	Name        string
	Family      string
	Scope       MetricScope
	Unit        string
	Description string
	DefaultStep time.Duration
	ValueModes  []MetricValueMode
}

var MetricCatalog = []MetricDefinition{
	{Name: MetricSystemCPUPercent, Family: "system", Scope: MetricScopeTarget, Unit: "percent", Description: "Target CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSystemMemoryPercent, Family: "system", Scope: MetricScopeTarget, Unit: "percent", Description: "Target memory utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSystemDiskPercent, Family: "system", Scope: MetricScopeTarget, Unit: "percent", Description: "Target disk utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSystemNetInBps, Family: "system", Scope: MetricScopeTarget, Unit: "bps", Description: "Target inbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSystemNetOutBps, Family: "system", Scope: MetricScopeTarget, Unit: "bps", Description: "Target outbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricGPUCount, Family: "gpu", Scope: MetricScopeTarget, Unit: "count", Description: "GPU count", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricGPUUp, Family: "gpu", Scope: MetricScopeTarget, Unit: "state", Description: "GPU availability state", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricGPUUtilPercent, Family: "gpu", Scope: MetricScopeTarget, Unit: "percent", Description: "GPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricContainerCPUPercent, Family: "container", Scope: MetricScopeTarget, Unit: "percent", Description: "Container CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricContainerMemory, Family: "container", Scope: MetricScopeTarget, Unit: "bytes", Description: "Container memory usage", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricContainerNetTxBps, Family: "container", Scope: MetricScopeTarget, Unit: "bps", Description: "Container outbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricContainerNetRxBps, Family: "container", Scope: MetricScopeTarget, Unit: "bps", Description: "Container inbound network throughput", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPIfInBps, Family: "snmp_interface", Scope: MetricScopePort, Unit: "bps", Description: "SNMP interface inbound throughput", DefaultStep: time.Minute, ValueModes: allValueModes()},
	{Name: MetricSNMPIfOutBps, Family: "snmp_interface", Scope: MetricScopePort, Unit: "bps", Description: "SNMP interface outbound throughput", DefaultStep: time.Minute, ValueModes: allValueModes()},
	{Name: MetricSNMPIfInOctetsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "octets", Description: "SNMP interface inbound counter", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfOutOctetsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "octets", Description: "SNMP interface outbound counter", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfOperStatus, Family: "snmp_interface", Scope: MetricScopePort, Unit: "state", Description: "SNMP interface operational status", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPIfAdminStatus, Family: "snmp_interface", Scope: MetricScopePort, Unit: "state", Description: "SNMP interface administrative status", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPIfInErrorsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "errors", Description: "SNMP interface inbound errors", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfOutErrorsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "errors", Description: "SNMP interface outbound errors", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfInDiscardsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "discards", Description: "SNMP interface inbound discards", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfOutDiscardsTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "discards", Description: "SNMP interface outbound discards", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfInCRCTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "errors", Description: "SNMP interface inbound CRC errors", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPIfOutCRCTotal, Family: "snmp_interface", Scope: MetricScopePort, Unit: "errors", Description: "SNMP interface outbound CRC errors", DefaultStep: time.Minute, ValueModes: []MetricValueMode{MetricValueRaw}},
	{Name: MetricSNMPOpticalRxDBM, Family: "snmp_optics", Scope: MetricScopePort, Unit: "dBm", Description: "Optical receive power", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPOpticalTxDBM, Family: "snmp_optics", Scope: MetricScopePort, Unit: "dBm", Description: "Optical transmit power", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPOpticalTempC, Family: "snmp_optics", Scope: MetricScopePort, Unit: "celsius", Description: "Optical module temperature", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPDeviceCPUPercent, Family: "snmp_device", Scope: MetricScopeDevice, Unit: "percent", Description: "SNMP device CPU utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPProcessorUsage, Family: "snmp_device", Scope: MetricScopeDevice, Unit: "percent", Description: "SNMP processor usage", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPDeviceMemPercent, Family: "snmp_device", Scope: MetricScopeDevice, Unit: "percent", Description: "SNMP device memory utilization", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPDeviceMemUsed, Family: "snmp_device", Scope: MetricScopeDevice, Unit: "bytes", Description: "SNMP device memory used", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricSNMPDeviceMemTotal, Family: "snmp_device", Scope: MetricScopeDevice, Unit: "bytes", Description: "SNMP device memory total", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricBGPState, Family: "bgp", Scope: MetricScopeBGP, Unit: "state", Description: "BGP session state", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricBGPAcceptedPrefixes, Family: "bgp", Scope: MetricScopeBGP, Unit: "prefixes", Description: "BGP accepted prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricBGPDeniedPrefixes, Family: "bgp", Scope: MetricScopeBGP, Unit: "prefixes", Description: "BGP denied prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
	{Name: MetricBGPAdvertisedPrefixes, Family: "bgp", Scope: MetricScopeBGP, Unit: "prefixes", Description: "BGP advertised prefixes", DefaultStep: time.Minute, ValueModes: correctedOnly()},
}

func IsKnownMetric(name string) bool {
	for _, metric := range MetricCatalog {
		if metric.Name == name {
			return true
		}
	}
	return false
}

func correctedOnly() []MetricValueMode {
	return []MetricValueMode{MetricValueCorrected}
}

func allValueModes() []MetricValueMode {
	return []MetricValueMode{MetricValueCorrected, MetricValueRaw, MetricValueBoth}
}
