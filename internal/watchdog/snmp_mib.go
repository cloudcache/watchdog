package watchdog

import "github.com/cloudcache/watchdog/internal/snmpdomain"

// Deprecated: the MIB registry and embedded bundle are owned by
// internal/snmpdomain. These wrappers keep the remaining legacy SNMP engine
// buildable while its files are moved in later KISS-08 slices.
type SNMPMIBRegistry = snmpdomain.SNMPMIBRegistry

func DefaultSNMPMIBRegistry() *SNMPMIBRegistry {
	return snmpdomain.DefaultSNMPMIBRegistry()
}

func ConfigureSNMPMIBRegistry(cfg SNMPConfig) error {
	return snmpdomain.ConfigureSNMPMIBRegistry(snmpdomain.SNMPConfig{
		MIBDirs: cfg.MIBDirs,
		MIBLoad: cfg.MIBLoad,
	})
}

func EmbeddedSNMPMIBModules() ([]MIBModule, error) {
	return snmpdomain.EmbeddedSNMPMIBModules()
}

func IsEmbeddedSNMPMIBSource(source string) bool {
	return snmpdomain.IsEmbeddedSNMPMIBSource(source)
}

func snmpMIBOID(ref string) string {
	return snmpdomain.DefaultSNMPMIBRegistry().MustOID(ref)
}

func snmpMIBDisplayOID(oid string) string {
	return snmpdomain.DefaultSNMPMIBRegistry().DisplayName(oid)
}

func isNumericOID(ref string) bool {
	return snmpdomain.IsNumericOID(ref)
}
