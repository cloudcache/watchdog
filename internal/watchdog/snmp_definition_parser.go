package watchdog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type librenmsOSDefinition struct {
	OS               string                `yaml:"os"`
	Text             string                `yaml:"text"`
	Type             string                `yaml:"type"`
	Group            string                `yaml:"group"`
	Icon             string                `yaml:"icon"`
	MIBDir           string                `yaml:"mib_dir"`
	Discovery        []librenmsOSDiscovery `yaml:"discovery"`
	DiscoveryModules map[string]bool       `yaml:"discovery_modules"`
	PollerModules    map[string]bool       `yaml:"poller_modules"`
	BadIfType        librenmsStringList    `yaml:"bad_iftype"`
	BadIfName        librenmsStringList    `yaml:"bad_ifname_regexp"`
	BadIfDescr       librenmsStringList    `yaml:"bad_ifdescr_regexp"`
	Extra            map[string]any        `yaml:",inline"`
}

type librenmsOSDiscovery struct {
	SysObjectID            librenmsStringList `yaml:"sysObjectID" json:"sysObjectID,omitempty"`
	SysObjectIDRegex       librenmsStringList `yaml:"sysObjectID_regex" json:"sysObjectID_regex,omitempty"`
	SysDescr               librenmsStringList `yaml:"sysDescr" json:"sysDescr,omitempty"`
	SysDescrRegex          librenmsStringList `yaml:"sysDescr_regex" json:"sysDescr_regex,omitempty"`
	SysName                librenmsStringList `yaml:"sysName" json:"sysName,omitempty"`
	SysNameRegex           librenmsStringList `yaml:"sysName_regex" json:"sysName_regex,omitempty"`
	SysObjectIDExcept      librenmsStringList `yaml:"sysObjectID_except" json:"sysObjectID_except,omitempty"`
	SysObjectIDRegexExcept librenmsStringList `yaml:"sysObjectID_regex_except" json:"sysObjectID_regex_except,omitempty"`
	SysDescrExcept         librenmsStringList `yaml:"sysDescr_except" json:"sysDescr_except,omitempty"`
	SysDescrRegexExcept    librenmsStringList `yaml:"sysDescr_regex_except" json:"sysDescr_regex_except,omitempty"`
	SysNameExcept          librenmsStringList `yaml:"sysName_except" json:"sysName_except,omitempty"`
	SysNameRegexExcept     librenmsStringList `yaml:"sysName_regex_except" json:"sysName_regex_except,omitempty"`
	SNMPGet                map[string]any     `yaml:"snmpget" json:"snmpget,omitempty"`
	SNMPWalk               map[string]any     `yaml:"snmpwalk" json:"snmpwalk,omitempty"`
}

type librenmsTrapConfig struct {
	Traps map[string]struct {
		OID     string `yaml:"oid"`
		Handler string `yaml:"handler"`
	} `yaml:"traps"`
}

func ParseLibrenmsDefinitions(dir, sourceVersion string) (SNMPDefinitionImport, error) {
	var result SNMPDefinitionImport
	defsDir := filepath.Join(dir, "resources", "definitions")
	if _, err := os.Stat(defsDir); os.IsNotExist(err) {
		defsDir = filepath.Join(dir, "includes", "definitions")
	}
	osDetectionDir := filepath.Join(defsDir, "os_detection")
	if _, err := os.Stat(osDetectionDir); os.IsNotExist(err) {
		osDetectionDir = defsDir
	}

	osDefs, err := parseLibrenmsOSDefinitions(osDetectionDir, sourceVersion)
	if err != nil {
		return result, fmt.Errorf("parse os definitions: %w", err)
	}
	result.OSDefinitions = osDefs

	stateTranslations, err := parseLibrenmsStateTranslations(osDetectionDir, sourceVersion)
	if err != nil {
		return result, fmt.Errorf("parse state translations: %w", err)
	}
	result.StateTranslations = stateTranslations

	trapHandlers, err := parseLibrenmsTrapHandlers(dir, sourceVersion)
	if err != nil {
		return result, fmt.Errorf("parse trap handlers: %w", err)
	}
	result.TrapHandlers = trapHandlers

	return result, nil
}

func parseLibrenmsOSDefinitions(dir, sourceVersion string) ([]SNMPCollectorOSDefinition, error) {
	scanDir := filepath.Join(dir, "os_detection")
	if _, err := os.Stat(scanDir); os.IsNotExist(err) {
		scanDir = dir
	}
	matches, err := filepath.Glob(filepath.Join(scanDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var defs []SNMPCollectorOSDefinition
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		var raw librenmsOSDefinition
		if err := yamlUnmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if raw.OS == "" {
			continue
		}
		// The sibling os_discovery YAML declares what to collect for this OS
		// (processors / mempools / storage / sensors tables by MIB name);
		// merge it so the discovery engine is definition-driven.
		osDiscovery := loadLibrenmsOSDiscovery(dir, raw.OS)
		discovery, err := normalizeLibrenmsDiscoveryRules(raw.Discovery)
		if err != nil {
			return nil, fmt.Errorf("normalize discovery rules in %s: %w", path, err)
		}
		definition := map[string]any{
			"os_discovery":       osDiscovery,
			"text":               raw.Text,
			"type":               raw.Type,
			"group":              raw.Group,
			"mib_dir":            raw.MIBDir,
			"discovery":          discovery,
			"discovery_modules":  raw.DiscoveryModules,
			"poller_modules":     raw.PollerModules,
			"bad_iftype":         raw.BadIfType,
			"bad_ifname_regexp":  raw.BadIfName,
			"bad_ifdescr_regexp": raw.BadIfDescr,
		}
		for k, v := range raw.Extra {
			definition[k] = v
		}
		vendor := raw.Group
		if raw.Icon != "" {
			vendor = raw.Icon
		}
		defs = append(defs, SNMPCollectorOSDefinition{
			ID:            collectorStableID("snmp-os-def", raw.OS, "librenms"),
			OSName:        raw.OS,
			OSGroup:       raw.Group,
			Vendor:        vendor,
			Class:         raw.Type,
			Definition:    definition,
			Source:        "librenms",
			SourceVersion: sourceVersion,
		})
	}
	return defs, nil
}

func normalizeLibrenmsDiscoveryRules(rules []librenmsOSDiscovery) ([]map[string]any, error) {
	data, err := json.Marshal(rules)
	if err != nil {
		return nil, err
	}
	var normalized []map[string]any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

// loadLibrenmsOSDiscovery reads resources/definitions/os_discovery/<os>.yaml
// (a sibling of the os_detection dir) and returns its "modules" map.
func loadLibrenmsOSDiscovery(osDetectionDir, osName string) map[string]any {
	discoveryPath := filepath.Join(filepath.Dir(osDetectionDir), "os_discovery", osName+".yaml")
	data, err := os.ReadFile(discoveryPath)
	if err != nil {
		return nil
	}
	var raw struct {
		Modules map[string]any `yaml:"modules"`
	}
	if err := yamlUnmarshal(data, &raw); err != nil {
		return nil
	}
	return raw.Modules
}

func parseLibrenmsModuleDefinitions(dir string, moduleType SNMPCollectorModuleType, sourceVersion string) ([]SNMPCollectorModuleDefinition, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var defs []SNMPCollectorModuleDefinition
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var raw map[string]any
		if err := yamlUnmarshal(data, &raw); err != nil {
			continue
		}
		name := strings.TrimSuffix(filepath.Base(path), ".yaml")
		if name == "" {
			continue
		}
		defs = append(defs, SNMPCollectorModuleDefinition{
			ID:            collectorStableID("snmp-mod-def", name, string(moduleType), "librenms"),
			ModuleName:    name,
			ModuleType:    moduleType,
			Definition:    raw,
			Source:        "librenms",
			SourceVersion: sourceVersion,
		})
	}
	return defs, nil
}

func parseLibrenmsStateTranslations(dir, sourceVersion string) ([]SNMPStateTranslation, error) {
	scanDir := filepath.Join(dir, "os_detection")
	if _, err := os.Stat(scanDir); os.IsNotExist(err) {
		scanDir = dir
	}
	matches, err := filepath.Glob(filepath.Join(scanDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var translations []SNMPStateTranslation
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var raw librenmsOSDefinition
		if err := yamlUnmarshal(data, &raw); err != nil {
			continue
		}
		if raw.OS == "" {
			continue
		}
		states := extractStateTranslations(raw.Extra)
		if len(states) > 0 {
			translations = append(translations, SNMPStateTranslation{
				ID:     collectorStableID("snmp-state-trans", raw.OS, "librenms"),
				Name:   raw.OS,
				Source: "librenms",
				States: states,
			})
		}
	}
	return translations, nil
}

func extractStateTranslations(extra map[string]any) []SNMPStateValue {
	var states []SNMPStateValue
	if sensors, ok := extra["sensors"]; ok {
		if sensorList, ok := sensors.([]any); ok {
			for _, s := range sensorList {
				if m, ok := s.(map[string]any); ok {
					if statesData, ok := m["states"]; ok {
						if stateMap, ok := statesData.(map[string]any); ok {
							for k, v := range stateMap {
								var sv SNMPStateValue
								var n int
								if _, err := fmt.Sscanf(k, "%d", &n); err == nil {
									sv.Value = n
								}
								if label, ok := v.(string); ok {
									sv.Label = label
								}
								states = append(states, sv)
							}
						}
					}
				}
			}
		}
	}
	return states
}

var trapOIDPattern = regexp.MustCompile(`'([^']+)'\s*=>\s*(?:LibreNMS\\Snmptrap\\Handlers\\)?(\w+)::class`)

func parseLibrenmsTrapHandlers(dir, sourceVersion string) ([]SNMPTrapHandlerDefinition, error) {
	configPath := filepath.Join(dir, "config", "snmptraps.php")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil
	}
	var handlers []SNMPTrapHandlerDefinition
	matches := trapOIDPattern.FindAllStringSubmatch(string(data), -1)
	for _, m := range matches {
		oid := m[1]
		handler := m[2]
		if oid == "" || handler == "" {
			continue
		}
		handlers = append(handlers, SNMPTrapHandlerDefinition{
			ID:         collectorStableID("snmp-trap-handler", oid),
			TrapOID:    oid,
			HandlerKey: handler,
			Enabled:    true,
		})
	}
	return handlers, nil
}

func yamlUnmarshal(data []byte, out any) error {
	return yamlUnmarshalImpl(data, out)
}

func definitionToJSON(m map[string]any) ([]byte, error) {
	return json.Marshal(m)
}
