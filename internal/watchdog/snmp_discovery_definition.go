package watchdog

import (
	"context"
	"regexp"
	"strconv"
	"strings"
)

// LibreNMS os_discovery definitions drive what a device is asked for: each
// entry names a table and columns by MIB object ("HUAWEI-ENTITY-EXTENT-MIB::
// hwEntityCpuUsage"), optionally with skip rules and description templates.
// This executor resolves those names through the MIB registry and walks them,
// so per-vendor collection comes from imported data instead of Go branches.

// osDiscoverySection returns the data entries of one module section
// ("processors", "mempools", "storage") from the OS discovery definition.
func osDiscoverySection(osDiscovery map[string]any, section string) []map[string]any {
	raw, ok := osDiscovery[section]
	if !ok {
		return nil
	}
	sectionMap, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return anyMapList(sectionMap["data"])
}

// osDiscoverySensorClasses returns class -> entries for the sensors section.
func osDiscoverySensorClasses(osDiscovery map[string]any) map[string][]map[string]any {
	raw, ok := osDiscovery["sensors"]
	if !ok {
		return nil
	}
	sensorMap, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	classes := map[string][]map[string]any{}
	for class, classRaw := range sensorMap {
		if class == "additional_oids" {
			continue
		}
		classMap, ok := classRaw.(map[string]any)
		if !ok {
			continue
		}
		if entries := anyMapList(classMap["data"]); len(entries) > 0 {
			classes[class] = entries
		}
	}
	return classes
}

func anyMapList(raw any) []map[string]any {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	entries := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if entry, ok := item.(map[string]any); ok {
			entries = append(entries, entry)
		}
	}
	return entries
}

func defString(entry map[string]any, key string) string {
	value, _ := entry[key].(string)
	return strings.TrimSpace(value)
}

var defIndexTemplate = regexp.MustCompile(`\{\{\s*\$index\s*\}\}`)

// defValueOID resolves the value column of an entry: the MIB reference in
// `value`, falling back to the numeric prefix of the `num_oid` template when
// the module is not loadable from the configured MIB dirs.
func defValueOID(entry map[string]any, key string) string {
	if ref := defString(entry, key); ref != "" {
		if oid, err := DefaultSNMPMIBRegistry().OID(ref); err == nil {
			return oid
		}
	}
	numOID := defString(entry, "num_oid")
	if key != "value" || numOID == "" {
		return ""
	}
	numOID = strings.TrimSpace(defIndexTemplate.ReplaceAllString(numOID, ""))
	numOID = strings.TrimSuffix(numOID, ".")
	numOID = strings.TrimPrefix(numOID, ".")
	if numOID == "" || !isNumericOID(numOID) {
		return ""
	}
	return numOID
}

// defResolve resolves any MIB reference field (descr/skip/total/...).
func defResolve(ref string) string {
	if ref == "" {
		return ""
	}
	oid, err := DefaultSNMPMIBRegistry().OID(ref)
	if err != nil {
		return ""
	}
	return oid
}

func defWalk(ctx context.Context, req SNMPCollectorDiscoveryContext, oid string) map[string]string {
	if oid == "" {
		return map[string]string{}
	}
	resp, err := req.Query.Walk(ctx, SNMPCollectorWalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: oid,
		Flags: SNMPCollectorQueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if err != nil {
		return map[string]string{}
	}
	return valuesBySNMPIndex(oid, resp)
}

// defSkipper evaluates the entry's skip_values rules per row index.
func defSkipper(ctx context.Context, req SNMPCollectorDiscoveryContext, entry map[string]any) func(index string) bool {
	rules := anyMapList(entry["skip_values"])
	if len(rules) == 0 {
		return func(string) bool { return false }
	}
	type compiled struct {
		values map[string]string
		op     string
		value  string
	}
	compiledRules := make([]compiled, 0, len(rules))
	for _, rule := range rules {
		oid := defResolve(defString(rule, "oid"))
		if oid == "" {
			continue
		}
		operand := ""
		switch v := rule["value"].(type) {
		case string:
			operand = v
		case int:
			operand = strconv.Itoa(v)
		case float64:
			operand = strconv.FormatFloat(v, 'f', -1, 64)
		}
		compiledRules = append(compiledRules, compiled{
			values: defWalk(ctx, req, oid),
			op:     defString(rule, "op"),
			value:  operand,
		})
	}
	return func(index string) bool {
		for _, rule := range compiledRules {
			if defCompare(cleanSNMPValue(rule.values[index]), rule.op, rule.value) {
				return true
			}
		}
		return false
	}
}

func defCompare(sample, op, operand string) bool {
	left, leftErr := strconv.ParseFloat(sample, 64)
	right, rightErr := strconv.ParseFloat(operand, 64)
	numeric := leftErr == nil && rightErr == nil
	switch op {
	case "=", "==":
		if numeric {
			return left == right
		}
		return sample == operand
	case "!=":
		if numeric {
			return left != right
		}
		return sample != operand
	case "<":
		return numeric && left < right
	case "<=":
		return numeric && left <= right
	case ">":
		return numeric && left > right
	case ">=":
		return numeric && left >= right
	default:
		return false
	}
}

var defTemplateToken = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)

// defDescriber renders the entry's descr template per row: `{{ $index }}` and
// `{{ MOD::object }}` lookups are supported; templates using other tokens
// fall back to the plain label plus index.
func defDescriber(ctx context.Context, req SNMPCollectorDiscoveryContext, entry map[string]any, fallback string) func(index string) string {
	template := defString(entry, "descr")
	if template == "" {
		return func(index string) string { return fallback + " " + index }
	}
	refValues := map[string]map[string]string{}
	supported := true
	for _, match := range defTemplateToken.FindAllStringSubmatch(template, -1) {
		token := strings.TrimSpace(match[1])
		if token == "$index" {
			continue
		}
		if strings.HasPrefix(token, "$") {
			supported = false
			break
		}
		oid := defResolve(token)
		if oid == "" {
			supported = false
			break
		}
		refValues[token] = defWalk(ctx, req, oid)
	}
	if !supported {
		return func(index string) string { return fallback + " " + index }
	}
	return func(index string) string {
		rendered := defTemplateToken.ReplaceAllStringFunc(template, func(match string) string {
			token := strings.TrimSpace(defTemplateToken.FindStringSubmatch(match)[1])
			if token == "$index" {
				return index
			}
			return cleanSNMPValue(refValues[token][index])
		})
		rendered = strings.TrimSpace(rendered)
		if rendered == "" {
			return fallback + " " + index
		}
		return rendered
	}
}

func defDivisor(entry map[string]any) float64 {
	switch v := entry["divisor"].(type) {
	case int:
		return float64(v)
	case float64:
		return v
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// discoverDefinitionProcessors builds CPU recipes from the OS discovery
// definition's processors section.
func discoverDefinitionProcessors(ctx context.Context, req SNMPCollectorDiscoveryContext) []SNMPCollectionRecipe {
	var recipes []SNMPCollectionRecipe
	for _, entry := range osDiscoverySection(req.OSDiscovery, "processors") {
		valueOID := defValueOID(entry, "value")
		if valueOID == "" {
			continue
		}
		values := defWalk(ctx, req, valueOID)
		if len(values) == 0 {
			continue
		}
		skip := defSkipper(ctx, req, entry)
		describe := defDescriber(ctx, req, entry, "Processor")
		for index := range values {
			if skip(index) {
				continue
			}
			entityID := collectorStableID("processor", string(req.TenantID), string(req.Device.ID), "def", index)
			descr := describe(index)
			recipes = append(recipes, processorRecipe(req, entityID, index, valueOID+"."+index, defMIBLabel(entry), descr))
			recipes = append(recipes, deviceCPUPercentRecipe(req, entityID, index, valueOID+"."+index, defMIBLabel(entry), descr))
		}
	}
	return recipes
}

// discoverDefinitionMempools builds memory recipes from the mempools section.
// Supported columns: percent/percent_used (device memory %), total, used.
func discoverDefinitionMempools(ctx context.Context, req SNMPCollectorDiscoveryContext) []SNMPCollectionRecipe {
	var recipes []SNMPCollectionRecipe
	for _, entry := range osDiscoverySection(req.OSDiscovery, "mempools") {
		percentOID := defValueOID(entry, "percent_used")
		if percentOID == "" {
			percentOID = defValueOID(entry, "percent")
		}
		totalOID := defValueOID(entry, "total")
		usedOID := defValueOID(entry, "used")
		anchorOID := firstNonEmptySNMPString(percentOID, usedOID, totalOID)
		if anchorOID == "" {
			continue
		}
		rows := defWalk(ctx, req, anchorOID)
		if len(rows) == 0 {
			continue
		}
		skip := defSkipper(ctx, req, entry)
		describe := defDescriber(ctx, req, entry, "Memory")
		for index := range rows {
			if skip(index) {
				continue
			}
			entityID := collectorStableID("memory", string(req.TenantID), string(req.Device.ID), "def", index)
			descr := describe(index)
			if percentOID != "" {
				recipe := memoryRecipe(req, entityID, MetricSNMPDeviceMemPercent, index, percentOID+"."+index, defMIBLabel(entry), descr, 1, nil)
				recipe.Unit = "percent"
				recipes = append(recipes, recipe)
			}
			if totalOID != "" {
				recipes = append(recipes, memoryRecipe(req, entityID, MetricSNMPMemoryTotal, index, totalOID+"."+index, defMIBLabel(entry), descr, 1, nil))
			}
			if usedOID != "" {
				recipes = append(recipes, memoryRecipe(req, entityID, MetricSNMPMemoryUsed, index, usedOID+"."+index, defMIBLabel(entry), descr, 1, nil))
			}
		}
	}
	return recipes
}

// discoverDefinitionStorage builds storage recipes from the storage section
// (size + optional used column; `units` multiplies raw values into bytes).
func discoverDefinitionStorage(ctx context.Context, req SNMPCollectorDiscoveryContext) []SNMPCollectionRecipe {
	var recipes []SNMPCollectionRecipe
	for _, entry := range osDiscoverySection(req.OSDiscovery, "storage") {
		sizeOID := defValueOID(entry, "size")
		if sizeOID == "" {
			continue
		}
		units := defDivisor(map[string]any{"divisor": entry["units"]})
		if units == 0 {
			units = 1
		}
		sizes := defWalk(ctx, req, sizeOID)
		if len(sizes) == 0 {
			continue
		}
		usedOID := defValueOID(entry, "used")
		skip := defSkipper(ctx, req, entry)
		describe := defDescriber(ctx, req, entry, "Storage")
		descrOID := defResolve(defString(entry, "descr"))
		descrValues := map[string]string{}
		if descrOID != "" {
			descrValues = defWalk(ctx, req, descrOID)
		}
		for index := range sizes {
			if skip(index) {
				continue
			}
			entityID := collectorStableID("storage", string(req.TenantID), string(req.Device.ID), "def", index)
			name := cleanSNMPValue(descrValues[index])
			if name == "" {
				name = describe(index)
			}
			total := storageDefinitionRecipe(req, entityID, MetricSNMPStorageTotal, index, sizeOID+"."+index, defMIBLabel(entry), name, units)
			recipes = append(recipes, total)
			if usedOID != "" {
				recipes = append(recipes, storageDefinitionRecipe(req, entityID, MetricSNMPStorageUsed, index, usedOID+"."+index, defMIBLabel(entry), name, units))
			}
		}
	}
	return recipes
}

func storageDefinitionRecipe(req SNMPCollectorDiscoveryContext, entityID ID, metric, idx, oid, mib, name string, units float64) SNMPCollectionRecipe {
	recipe := SNMPCollectionRecipe{
		ID:                    collectorStableID("snmp-recipe", string(req.TenantID), string(req.Device.ID), snmpCollectorModuleStorage, string(SNMPCollectorEntityStorage), string(entityID), metric, idx, ""),
		TenantID:              req.TenantID,
		DeviceID:              req.Device.ID,
		EntityType:            SNMPCollectorEntityStorage,
		EntityID:              entityID,
		ModuleName:            snmpCollectorModuleStorage,
		MetricName:            metric,
		ValueType:             SNMPCollectorValueGauge,
		OID:                   snmpMIBDisplayOID(oid),
		NumericOID:            oid,
		OIDIndex:              idx,
		MIB:                   mib,
		PollerType:            "snmp",
		Unit:                  "bytes",
		SampleIntervalSeconds: 60,
		Labels: map[string]string{
			"target_id":   string(req.TargetID),
			"module":      snmpCollectorModuleStorage,
			"description": name,
		},
		Enabled: true,
	}
	if units != 1 {
		recipe.Multiplier = units
		recipe.HasMultiplier = true
	}
	return recipe
}

// discoverDefinitionSensors builds sensor rows and recipes from the sensors
// section for value-bearing classes.
func discoverDefinitionSensors(ctx context.Context, req SNMPCollectorDiscoveryContext) ([]NetworkDeviceSensor, []SNMPCollectionRecipe) {
	classUnits := map[string]string{
		"temperature": "C",
		"voltage":     "V",
		"current":     "A",
		"power":       "W",
		"fanspeed":    "rpm",
		"humidity":    "%",
		"dbm":         "dBm",
	}
	var sensors []NetworkDeviceSensor
	var recipes []SNMPCollectionRecipe
	for class, entries := range osDiscoverySensorClasses(req.OSDiscovery) {
		unit, supported := classUnits[class]
		if !supported {
			continue
		}
		for entryIdx, entry := range entries {
			valueOID := defValueOID(entry, "value")
			if valueOID == "" {
				continue
			}
			values := defWalk(ctx, req, valueOID)
			if len(values) == 0 {
				continue
			}
			skip := defSkipper(ctx, req, entry)
			describe := defDescriber(ctx, req, entry, class)
			divisor := defDivisor(entry)
			for index, raw := range values {
				if skip(index) {
					continue
				}
				value, err := parseFloatValue(raw)
				if err != nil {
					continue
				}
				if divisor > 1 {
					value /= divisor
				}
				name := describe(index)
				sensor := NetworkDeviceSensor{
					ID:       collectorStableID("sensor", string(req.TenantID), string(req.Device.ID), "def", class, strconv.Itoa(entryIdx), index),
					TenantID: req.TenantID,
					DeviceID: req.Device.ID,
					Class:    class,
					Name:     name,
					OID:      valueOID + "." + index,
					Unit:     unit,
					Value:    value,
					Status:   "ok",
					Metadata: map[string]string{"sensor_type": "os_discovery:" + class},
				}
				sensors = append(sensors, sensor)
				recipe := SNMPCollectionRecipe{
					ID:                    collectorStableID("snmp-recipe", string(req.TenantID), string(req.Device.ID), snmpCollectorModuleSensors, string(SNMPCollectorEntitySensor), string(sensor.ID), MetricSNMPSensorValue, index, ""),
					TenantID:              req.TenantID,
					DeviceID:              req.Device.ID,
					EntityType:            SNMPCollectorEntitySensor,
					EntityID:              sensor.ID,
					ModuleName:            snmpCollectorModuleSensors,
					MetricName:            MetricSNMPSensorValue,
					ValueType:             SNMPCollectorValueGauge,
					OID:                   snmpMIBDisplayOID(valueOID + "." + index),
					NumericOID:            valueOID + "." + index,
					OIDIndex:              index,
					MIB:                   defMIBLabel(entry),
					PollerType:            "snmp",
					Unit:                  unit,
					SampleIntervalSeconds: 60,
					Labels: map[string]string{
						"target_id":    string(req.TargetID),
						"sensor_id":    string(sensor.ID),
						"sensor_class": class,
						"module":       snmpCollectorModuleSensors,
					},
					Enabled: true,
				}
				if divisor > 1 {
					recipe.Divisor = divisor
					recipe.HasDivisor = true
				}
				recipes = append(recipes, recipe)
			}
		}
	}
	return sensors, recipes
}

// defMIBLabel reports which MIB module an entry's value came from.
func defMIBLabel(entry map[string]any) string {
	ref := defString(entry, "value")
	if ref == "" {
		ref = defString(entry, "oid")
	}
	if module, _, ok := strings.Cut(ref, "::"); ok && module != "" {
		return module
	}
	return "os_discovery"
}
