package watchdog

import (
	"regexp"
	"strings"
)

func DetectSNMPCollectorOS(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, bool) {
	match, _, ok := DetectSNMPCollectorOSWithDefinition(fingerprint, definitions)
	return match, ok
}

func DetectSNMPCollectorOSWithDefinition(fingerprint SNMPCollectorOSFingerprint, definitions []SNMPCollectorOSDefinition) (SNMPCollectorOSMatch, SNMPCollectorOSDefinition, bool) {
	var best SNMPCollectorOSMatch
	var bestDef SNMPCollectorOSDefinition
	bestScore := 0
	for _, definition := range definitions {
		for _, rule := range snmpOSDetectionRules(definition.Definition) {
			score, reason := matchSNMPOSRule(fingerprint, rule)
			if score > bestScore {
				bestScore = score
				best = SNMPCollectorOSMatch{
					OSName:  definition.OSName,
					OSGroup: definition.OSGroup,
					Vendor:  definition.Vendor,
					Class:   definition.Class,
					Reason:  reason,
				}
				bestDef = definition
			}
		}
	}
	return best, bestDef, bestScore > 0
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

func matchSNMPOSRule(fingerprint SNMPCollectorOSFingerprint, rule map[string]any) (int, string) {
	if snmpOSNegativeRuleMatches(fingerprint, rule) {
		return 0, ""
	}
	score := 0
	var reasons []string
	conditions := 0
	if values := anyStringSlice(rule["sysObjectID"]); len(values) > 0 {
		conditions++
		matched, prefixLen := anyPrefixMatchLen(normalizedOID(fingerprint.SysObjectID), normalizedOIDs(values))
		if !matched {
			return 0, ""
		}
		effectiveLen := prefixLen
		if prefixLen < 20 {
			effectiveLen = prefixLen / 2
		}
		score += 100 + effectiveLen
		reasons = append(reasons, "sysObjectID")
	}
	if values := anyStringSlice(rule["sysObjectID_regex"]); len(values) > 0 {
		conditions++
		if !anyRegexMatch(fingerprint.SysObjectID, values) {
			return 0, ""
		}
		score += 80
		reasons = append(reasons, "sysObjectID_regex")
	}
	if values := anyStringSlice(rule["sysDescr_regex"]); len(values) > 0 {
		conditions++
		matched := false
		matchLen := 0
		for _, v := range values {
			re, err := regexp.Compile(v)
			if err != nil {
				continue
			}
			if loc := re.FindStringIndex(fingerprint.SysDescr); loc != nil {
				matched = true
				if l := loc[1] - loc[0]; l > matchLen {
					matchLen = l
				}
			}
		}
		if !matched {
			return 0, ""
		}
		score += 100 + matchLen
		reasons = append(reasons, "sysDescr_regex")
	}
	if values := anyStringSlice(rule["sysName_regex"]); len(values) > 0 {
		conditions++
		if !anyRegexMatch(fingerprint.SysName, values) {
			return 0, ""
		}
		score += 40
		reasons = append(reasons, "sysName_regex")
	}
	if conditions == 0 {
		return 0, ""
	}
	return score, strings.Join(reasons, "+")
}

func snmpOSNegativeRuleMatches(fingerprint SNMPCollectorOSFingerprint, rule map[string]any) bool {
	if values := anyStringSlice(rule["except"]); len(values) > 0 {
		if anyRegexMatch(fingerprint.SysDescr, values) {
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
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		if re.MatchString(value) {
			return true
		}
	}
	return false
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
