// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/ch-go/proto"
)

const maxDetailColumnFilters = 32

func compileDetailColumnFiltersForSource(view View, legacy DetailFilters, filters []DetailColumnFilter, excludeField string) ([]string, []proto.Parameter, error) {
	if len(filters) > maxDetailColumnFilters {
		return nil, nil, requestError("column_filters", ErrorLimitExceeded, "too many detail column filters")
	}
	seen := make(map[string]struct{}, len(filters))
	conditions := make([]string, 0, len(filters))
	parameters := make([]proto.Parameter, 0)
	total := 0
	for index, filter := range filters {
		field := strings.TrimSpace(filter.Field)
		if field == "" {
			return nil, nil, requestError(fmt.Sprintf("column_filters[%d].field", index), ErrorRequired, "column filter field is required")
		}
		if _, exists := seen[field]; exists {
			return nil, nil, requestError("column_filters", ErrorInvalid, fmt.Sprintf("column filter field %q is duplicated", field))
		}
		seen[field] = struct{}{}
		if len(filter.Values) == 0 {
			return nil, nil, requestError(fmt.Sprintf("column_filters[%d].values", index), ErrorRequired, "column filter values are required")
		}
		if len(filter.Values) > maxDetailValuesPerFilter || total+len(filter.Values) > maxDetailFilterValues {
			return nil, nil, requestError(fmt.Sprintf("column_filters[%d].values", index), ErrorLimitExceeded, "too many detail column filter values")
		}
		total += len(filter.Values)
		spec, err := detailFieldSpecForFilter(view, field)
		if err != nil {
			return nil, nil, err
		}
		if detailLegacyFilterHasValues(legacy, field) {
			return nil, nil, requestError("column_filters", ErrorInvalid, fmt.Sprintf("column filter %q duplicates a typed filter", field))
		}
		values, err := normalizeDetailColumnValues(spec, filter.Values)
		if err != nil {
			return nil, nil, requestError(fmt.Sprintf("column_filters[%d].values", index), ErrorInvalid, err.Error())
		}
		if field == excludeField {
			continue
		}
		expression := detailFieldExpression(spec)
		if isDetailIPField(spec.field) {
			expression = "source." + spec.column
		}
		if spec.kind == detailKindBool {
			expression = "toUInt8(" + expression + ")"
		}
		placeholders := make([]string, 0, len(values))
		for valueIndex, value := range values {
			key := fmt.Sprintf("detail_column_%d_%d", index, valueIndex)
			placeholder, parameter := detailColumnValueParameter(key, spec, value)
			placeholders = append(placeholders, placeholder)
			parameters = append(parameters, parameter)
		}
		conditions = append(conditions, fmt.Sprintf("AND %s IN (%s)", expression, strings.Join(placeholders, ", ")))
	}
	return conditions, parameters, nil
}

func detailFieldSpecForFilter(view View, field string) (detailFieldSpec, error) {
	if field == "event_time" {
		return detailFieldSpec{field: DetailField(field), column: "event_time", kind: detailKindTime}, nil
	}
	detailField := DetailField(field)
	if _, exists := detailFieldRegistry[detailField]; !exists {
		return detailFieldSpec{}, requestError("column_filters.field", ErrorUnsupported, fmt.Sprintf("column filter field %q is not in the detail registry", field))
	}
	viewSpec, exists := detailViewRegistry[view]
	if !exists {
		return detailFieldSpec{}, requestError("view", ErrorUnsupported, fmt.Sprintf("detail view %q is unsupported", view))
	}
	if viewSpec.fields != nil {
		if _, allowed := viewSpec.fields[detailField]; !allowed {
			return detailFieldSpec{}, requestError("column_filters.field", ErrorUnsupported, fmt.Sprintf("column filter field %q is not available in %s view", field, view))
		}
	}
	return detailFieldSpecForView(view, detailField), nil
}

func detailLegacyFilterHasValues(filters DetailFilters, field string) bool {
	switch DetailField(field) {
	case DetailFieldBusinessDirection:
		return len(filters.Directions) > 0
	case DetailFieldCategory:
		return len(filters.Categories) > 0
	case DetailFieldBusiness:
		return len(filters.Businesses) > 0
	case DetailFieldTargetID:
		return len(filters.TargetIDs) > 0
	case DetailFieldDeviceID:
		return len(filters.DeviceIDs) > 0
	case DetailFieldExporterID:
		return len(filters.ExporterIDs) > 0
	default:
		return false
	}
}

func normalizeDetailColumnValues(spec detailFieldSpec, input []string) ([]any, error) {
	values := make(map[string]any, len(input))
	for _, raw := range input {
		canonical, value, err := normalizeDetailColumnValue(spec, raw)
		if err != nil {
			return nil, err
		}
		values[canonical] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]any, 0, len(keys))
	for _, key := range keys {
		result = append(result, values[key])
	}
	return result, nil
}

func normalizeDetailColumnValue(spec detailFieldSpec, raw string) (string, any, error) {
	switch spec.kind {
	case detailKindString:
		if !utf8.ValidString(raw) || len(raw) > 1024 {
			return "", nil, fmt.Errorf("string filter value must be valid UTF-8 and no longer than 1024 bytes")
		}
		if isDetailIPField(spec.field) {
			ip, err := netip.ParseAddr(raw)
			if err != nil || ip.Zone() != "" {
				return "", nil, fmt.Errorf("IP filter value must be an IPv4 or IPv6 address without a zone")
			}
			raw = ip.Unmap().String()
		}
		return raw, raw, nil
	case detailKindUInt64:
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != raw {
			return "", nil, fmt.Errorf("unsigned integer filter value must be canonical decimal")
		}
		return raw, value, nil
	case detailKindBool:
		value, err := strconv.ParseBool(raw)
		if err != nil || strconv.FormatBool(value) != raw {
			return "", nil, fmt.Errorf("boolean filter value must be true or false")
		}
		return raw, value, nil
	case detailKindTime:
		value, err := time.Parse("2006-01-02T15:04:05.000Z", raw)
		if err != nil {
			return "", nil, fmt.Errorf("time filter value must be a UTC RFC3339 timestamp with millisecond precision")
		}
		return raw, value.UTC(), nil
	default:
		return "", nil, fmt.Errorf("column filter value type is unsupported")
	}
}

func detailColumnValueParameter(key string, spec detailFieldSpec, value any) (string, proto.Parameter) {
	if isDetailIPField(spec.field) {
		return fmt.Sprintf("toIPv6({%s:String})", key), stringParameter(key, value.(string))
	}
	switch spec.kind {
	case detailKindUInt64:
		return fmt.Sprintf("{%s:UInt64}", key), uintParameter(key, value.(uint64))
	case detailKindBool:
		encoded := uint64(0)
		if value.(bool) {
			encoded = 1
		}
		return fmt.Sprintf("{%s:UInt8}", key), uintParameter(key, encoded)
	case detailKindTime:
		return fmt.Sprintf("{%s:DateTime64(3, 'UTC')}", key), stringParameter(key, formatDateTime64(value.(time.Time)))
	default:
		return fmt.Sprintf("{%s:String}", key), stringParameter(key, value.(string))
	}
}
