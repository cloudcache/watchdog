package watchdog

import "github.com/cloudcache/watchdog/internal/snmpdomain"

func DetectSNMPCollectorOS(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, bool) {
	return snmpdomain.DetectOS(fingerprint, definitions)
}

func DetectSNMPCollectorOSWithDefinition(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, SNMPCollectorOSDefinition, bool) {
	return snmpdomain.DetectOSWithDefinition(fingerprint, definitions)
}
