package watchdog

import (
	"regexp"
	"sort"
	"strings"
)

func DetectSNMPCollectorOS(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, bool) {
	match, _, ok := DetectSNMPCollectorOSWithDefinition(fingerprint, definitions)
	return match, ok
}

func DetectSNMPCollectorOSWithDefinition(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, SNMPCollectorOSDefinition, bool) {
	// LibreNMS loads os_detection YAML files in filename order and returns the
	// first complete rule match. Keep that ordering contract: scoring matches
	// changes its semantics and lets broad sysObjectID prefixes override an
	// earlier, explicit OS signature in sysDescr.
	ordered := append([]SNMPCollectorOSDefinition(nil), definitions...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].OSName < ordered[j].OSName
	})

	for pass := 0; pass < 2; pass++ {
		for _, definition := range ordered {
			if snmpOSDefinitionIsDeferred(definition.OSName) != (pass == 1) {
				continue
			}
			for _, rule := range snmpOSDetectionRules(definition.Definition) {
				matched, reason := matchSNMPOSRule(fingerprint, rule)
				if matched {
					return SNMPCollectorOSMatch{
						OSName:  definition.OSName,
						OSGroup: definition.OSGroup,
						Vendor:  definition.Vendor,
						Class:   definition.Class,
						Reason:  reason,
					}, definition, true
				}
			}
		}
	}
	return SNMPCollectorOSMatch{}, SNMPCollectorOSDefinition{}, false
}

func snmpOSDefinitionIsDeferred(osName string) bool {
	switch osName {
	case "airos", "freebsd", "generic", "linux":
		return true
	default:
		return false
	}
}

func snmpOSDetectionRules(definition map[string]any) []map[string]any {
	if len(definition) == 0 {
		return nil
	}
	if raw, ok := definition["discovery"]; ok {
		if rules := anyMapSlice(raw); len(rules) > 0 {
			return rules
		}
	}
	return []map[string]any{definition}
}

func matchSNMPOSRule(fingerprint SNMPCollectorOSFingerprint, rule map[string]any) (bool, string) {
	// snmpget/snmpwalk are active probes, not fingerprint predicates. A rule
	// containing one is incomplete until that probe has been evaluated and
	// must never degrade into a match on only its broad static predicates.
	if rule["snmpget"] != nil || rule["snmpwalk"] != nil {
		return false, ""
	}
	if snmpOSNegativeRuleMatches(fingerprint, rule) {
		return false, ""
	}
	var reasons []string
	conditions := 0
	if values := anyStringSlice(rule["sysObjectID"]); len(values) > 0 {
		conditions++
		matched := anyPrefixMatch(normalizedOID(fingerprint.SysObjectID), normalizedOIDs(values))
		if !matched {
			return false, ""
		}
		reasons = append(reasons, "sysObjectID")
	}
	if values := anyStringSlice(rule["sysObjectID_regex"]); len(values) > 0 {
		conditions++
		if !anyRegexMatch(fingerprint.SysObjectID, values) {
			return false, ""
		}
		reasons = append(reasons, "sysObjectID_regex")
	}
	if values := anyStringSlice(rule["sysDescr"]); len(values) > 0 {
		conditions++
		matched, _ := anyContainsMatchLen(fingerprint.SysDescr, values)
		if !matched {
			return false, ""
		}
		reasons = append(reasons, "sysDescr")
	}
	if values := anyStringSlice(rule["sysDescr_regex"]); len(values) > 0 {
		conditions++
		matched := false
		for _, v := range values {
			re, err := regexp.Compile(v)
			if err != nil {
				continue
			}
			if loc := re.FindStringIndex(fingerprint.SysDescr); loc != nil {
				matched = true
			}
		}
		if !matched {
			return false, ""
		}
		reasons = append(reasons, "sysDescr_regex")
	}
	if values := anyStringSlice(rule["sysName"]); len(values) > 0 {
		conditions++
		matched, _ := anyContainsMatchLen(fingerprint.SysName, values)
		if !matched {
			return false, ""
		}
		reasons = append(reasons, "sysName")
	}
	if values := anyStringSlice(rule["sysName_regex"]); len(values) > 0 {
		conditions++
		if !anyRegexMatch(fingerprint.SysName, values) {
			return false, ""
		}
		reasons = append(reasons, "sysName_regex")
	}
	if conditions == 0 {
		return false, ""
	}
	return true, strings.Join(reasons, "+")
}

func snmpOSNegativeRuleMatches(fingerprint SNMPCollectorOSFingerprint, rule map[string]any) bool {
	if values := anyStringSlice(rule["sysDescr_except"]); len(values) > 0 {
		if matched, _ := anyContainsMatchLen(fingerprint.SysDescr, values); matched {
			return true
		}
	}
	if values := anyStringSlice(rule["sysObjectID_except"]); len(values) > 0 {
		if anyPrefixMatch(normalizedOID(fingerprint.SysObjectID), normalizedOIDs(values)) {
			return true
		}
	}
	if values := anyStringSlice(rule["sysObjectID_regex_except"]); len(values) > 0 {
		if anyRegexMatch(fingerprint.SysObjectID, values) {
			return true
		}
	}
	if values := anyStringSlice(rule["sysDescr_regex_except"]); len(values) > 0 {
		if anyRegexMatch(fingerprint.SysDescr, values) {
			return true
		}
	}
	if values := anyStringSlice(rule["sysName_regex_except"]); len(values) > 0 {
		if anyRegexMatch(fingerprint.SysName, values) {
			return true
		}
	}
	if values := anyStringSlice(rule["sysName_except"]); len(values) > 0 {
		if matched, _ := anyContainsMatchLen(fingerprint.SysName, values); matched {
			return true
		}
	}
	return false
}

func anyMapSlice(value any) []map[string]any {
	switch v := value.(type) {
	case []map[string]any:
		return v
	case []any:
		rules := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if rule, ok := item.(map[string]any); ok {
				rules = append(rules, rule)
			}
		}
		return rules
	case map[string]any:
		return []map[string]any{v}
	default:
		return nil
	}
}

func anyStringSlice(value any) []string {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	case []string:
		return v
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				values = append(values, text)
			}
		}
		return values
	default:
		return nil
	}
}

func anyPrefixMatch(value string, prefixes []string) bool {
	matched, _ := anyPrefixMatchLen(value, prefixes)
	return matched
}

func anyPrefixMatchLen(value string, prefixes []string) (bool, int) {
	bestLen := 0
	for _, prefix := range prefixes {
		if prefix != "" && strings.HasPrefix(value, prefix) {
			if len(prefix) > bestLen {
				bestLen = len(prefix)
			}
		}
	}
	return bestLen > 0, bestLen
}

func anyRegexMatch(value string, patterns []string) bool {
	for _, pattern := range patterns {
		re, err := regexp.Compile(librenmsRegex(pattern))
		if err != nil {
			continue
		}
		if re.MatchString(value) {
			return true
		}
	}
	return false
}

func anyContainsMatchLen(value string, candidates []string) (bool, int) {
	bestLen := 0
	for _, candidate := range candidates {
		if candidate != "" && strings.Contains(value, candidate) && len(candidate) > bestLen {
			bestLen = len(candidate)
		}
	}
	return bestLen > 0, bestLen
}

func librenmsRegex(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if len(pattern) < 2 || pattern[0] != '/' {
		return pattern
	}
	end := strings.LastIndex(pattern[1:], "/")
	if end < 0 {
		return pattern
	}
	end++
	body := pattern[1:end]
	modifiers := pattern[end+1:]
	flags := ""
	for _, modifier := range modifiers {
		if strings.ContainsRune("imsU", modifier) && !strings.ContainsRune(flags, modifier) {
			flags += string(modifier)
		}
	}
	if flags != "" {
		return "(?" + flags + ")" + body
	}
	return body
}

func normalizedOIDs(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, normalizedOID(value))
	}
	return normalized
}

func normalizedOID(value string) string {
	return strings.Trim(strings.TrimSpace(value), ".")
}
