package watchdog

import "github.com/cloudcache/watchdog/internal/snmpdomain"

// ParseLibrenmsDefinitions is a temporary compatibility wrapper for legacy
// repository tests. Production discovery parses definitions in snmpdomain.
func ParseLibrenmsDefinitions(dir, sourceVersion string) (SNMPDefinitionImport, error) {
	return snmpdomain.ParseLibrenmsDefinitions(dir, sourceVersion)
}
