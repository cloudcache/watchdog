// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

const (
	maxFilterDepth = 8
	maxFilterNodes = 64
)

type FilterNodeOperation string

const (
	FilterAnd       FilterNodeOperation = "and"
	FilterOr        FilterNodeOperation = "or"
	FilterNot       FilterNodeOperation = "not"
	FilterPredicate FilterNodeOperation = "predicate"
)

type FilterOperator string

const (
	FilterEqual              FilterOperator = "eq"
	FilterNotEqual           FilterOperator = "ne"
	FilterIn                 FilterOperator = "in"
	FilterNotIn              FilterOperator = "not_in"
	FilterGreaterThan        FilterOperator = "gt"
	FilterGreaterThanOrEqual FilterOperator = "gte"
	FilterLessThan           FilterOperator = "lt"
	FilterLessThanOrEqual    FilterOperator = "lte"
)

// FilterExpression is the wire AST accepted by Flow base-fact queries. SQL
// identifiers and operators never come from this object: the compiler resolves
// every field through filterFieldRegistry and emits values as typed parameters.
type FilterExpression struct {
	Op       FilterNodeOperation `json:"op"`
	Args     []FilterExpression  `json:"args,omitempty"`
	Field    string              `json:"field,omitempty"`
	Operator FilterOperator      `json:"operator,omitempty"`
	Values   []string            `json:"values,omitempty"`
}

type FilterFieldDefinition struct {
	Name      string           `json:"name"`
	ValueType string           `json:"value_type"`
	Operators []FilterOperator `json:"operators"`
	Values    []string         `json:"values,omitempty"`
}

type filterValueType string

const (
	filterString filterValueType = "string"
	filterEnum   filterValueType = "enum"
	filterUint   filterValueType = "uint"
	filterIP     filterValueType = "ip_or_cidr"
)

type filterFieldSpec struct {
	column    string
	valueType filterValueType
	bits      int
	allowed   map[string]struct{}
	aliases   map[string]string
}

var (
	stringFilterOperators = []FilterOperator{FilterEqual, FilterNotEqual, FilterIn, FilterNotIn}
	uintFilterOperators   = []FilterOperator{FilterEqual, FilterNotEqual, FilterIn, FilterNotIn, FilterGreaterThan, FilterGreaterThanOrEqual, FilterLessThan, FilterLessThanOrEqual}
	protocolAliases       = map[string]string{
		"tcp": "6", "udp": "17", "icmp": "1", "icmpv6": "58", "ipv6-icmp": "58",
		"gre": "47", "esp": "50", "ah": "51", "sctp": "132",
	}
	filterFieldRegistry = map[string]filterFieldSpec{
		"direction":             {column: "business_direction", valueType: filterEnum, allowed: validDirections},
		"category":              {column: "category", valueType: filterEnum, allowed: validCategories},
		"business":              {column: "business", valueType: filterString},
		"target":                {column: "target_id", valueType: filterString},
		"device":                {column: "device_id", valueType: filterString},
		"exporter":              {column: "exporter_id", valueType: filterString},
		"src_ip":                {column: "src_ip", valueType: filterIP},
		"dst_ip":                {column: "dst_ip", valueType: filterIP},
		"local_ip":              {column: "local_ip", valueType: filterIP},
		"remote_ip":             {column: "remote_ip", valueType: filterIP},
		"asn":                   {column: "remote_asn", valueType: filterUint, bits: 32},
		"isp":                   {column: "remote_isp_id", valueType: filterUint, bits: 16},
		"geo.continent":         {column: "remote_geo_continent_id", valueType: filterString},
		"geo.region":            {column: "remote_geo_region_id", valueType: filterString},
		"geo.country":           {column: "remote_geo_country_id", valueType: filterString},
		"geo.province":          {column: "remote_geo_province_id", valueType: filterString},
		"geo.city":              {column: "remote_geo_city_id", valueType: filterString},
		"local_prefix":          {column: "local_prefix_id", valueType: filterString},
		"remote_prefix":         {column: "remote_prefix_id", valueType: filterString},
		"local_port":            {column: "local_port", valueType: filterUint, bits: 16},
		"remote_port":           {column: "remote_port", valueType: filterUint, bits: 16},
		"protocol":              {column: "ip_protocol", valueType: filterUint, bits: 8, aliases: protocolAliases},
		"observation_interface": {column: "observation_if_index", valueType: filterUint, bits: 32},
	}
)

var aggregateFilterFields = map[string]struct{}{
	"direction": {}, "category": {}, "business": {}, "target": {}, "device": {}, "exporter": {},
}

// FlowFilterCatalog is a stable completion/validation registry for clients.
func FlowFilterCatalog() []FilterFieldDefinition {
	fields := make([]string, 0, len(filterFieldRegistry))
	for name := range filterFieldRegistry {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	result := make([]FilterFieldDefinition, 0, len(fields))
	for _, name := range fields {
		spec := filterFieldRegistry[name]
		operators := stringFilterOperators
		if spec.valueType == filterUint {
			operators = uintFilterOperators
		}
		definition := FilterFieldDefinition{Name: name, ValueType: string(spec.valueType), Operators: append([]FilterOperator(nil), operators...)}
		if spec.allowed != nil {
			for value := range spec.allowed {
				definition.Values = append(definition.Values, value)
			}
			sort.Strings(definition.Values)
		} else if name == "protocol" {
			for value := range protocolAliases {
				definition.Values = append(definition.Values, value)
			}
			sort.Strings(definition.Values)
		}
		result = append(result, definition)
	}
	return result
}

// CanonicalFilter validates, normalizes and deterministically orders a filter
// AST. AND/OR children are commutative, so sorting them gives saved filters and
// query hashes a stable representation.
func CanonicalFilter(input FilterExpression) (FilterExpression, error) {
	state := filterValidationState{}
	return state.canonical(input, 1)
}

// AggregateFilterSupported reports whether an expression can be evaluated on
// flow_aggregate_1m/1h without losing cross-dimension semantics. Other fields
// require the bounded base-fact path (or a published joint index).
func AggregateFilterSupported(input FilterExpression) (bool, error) {
	canonical, err := CanonicalFilter(input)
	if err != nil {
		return false, err
	}
	return aggregateFilterNodeSupported(canonical), nil
}

// FilterFields returns the unique fields referenced by a validated expression.
// Authorization adapters use it to keep resource-scoped predicates behind the
// same permission checks as the legacy resource selectors.
func FilterFields(input FilterExpression) ([]string, error) {
	canonical, err := CanonicalFilter(input)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var visit func(FilterExpression)
	visit = func(node FilterExpression) {
		if node.Op == FilterPredicate {
			seen[node.Field] = struct{}{}
			return
		}
		for _, child := range node.Args {
			visit(child)
		}
	}
	visit(canonical)
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields, nil
}

func aggregateFilterNodeSupported(input FilterExpression) bool {
	if input.Op == FilterPredicate {
		_, exists := aggregateFilterFields[input.Field]
		return exists
	}
	for _, child := range input.Args {
		if !aggregateFilterNodeSupported(child) {
			return false
		}
	}
	return true
}

type filterValidationState struct {
	nodes  int
	values int
}

func (s *filterValidationState) canonical(input FilterExpression, depth int) (FilterExpression, error) {
	s.nodes++
	if depth > maxFilterDepth || s.nodes > maxFilterNodes {
		return FilterExpression{}, requestError("filter", ErrorLimitExceeded, "filter AST is too deep or contains too many nodes")
	}
	switch input.Op {
	case FilterAnd, FilterOr:
		if input.Field != "" || input.Operator != "" || len(input.Values) != 0 || len(input.Args) < 2 {
			return FilterExpression{}, requestError("filter", ErrorInvalid, "and/or nodes require at least two args and no predicate fields")
		}
		args := make([]FilterExpression, 0, len(input.Args))
		for _, child := range input.Args {
			canonical, err := s.canonical(child, depth+1)
			if err != nil {
				return FilterExpression{}, err
			}
			if canonical.Op == input.Op {
				args = append(args, canonical.Args...)
			} else {
				args = append(args, canonical)
			}
		}
		args = canonicalFilterArgs(args)
		if len(args) == 1 {
			return args[0], nil
		}
		return FilterExpression{Op: input.Op, Args: args}, nil
	case FilterNot:
		if input.Field != "" || input.Operator != "" || len(input.Values) != 0 || len(input.Args) != 1 {
			return FilterExpression{}, requestError("filter", ErrorInvalid, "not nodes require exactly one arg and no predicate fields")
		}
		child, err := s.canonical(input.Args[0], depth+1)
		if err != nil {
			return FilterExpression{}, err
		}
		if child.Op == FilterNot {
			return child.Args[0], nil
		}
		return FilterExpression{Op: FilterNot, Args: []FilterExpression{child}}, nil
	case FilterPredicate:
		if len(input.Args) != 0 || input.Field == "" || input.Operator == "" {
			return FilterExpression{}, requestError("filter", ErrorInvalid, "predicate nodes require field, operator and values")
		}
		spec, exists := filterFieldRegistry[input.Field]
		if !exists {
			return FilterExpression{}, requestError("filter.field", ErrorUnsupported, fmt.Sprintf("filter field %q is not in the Flow registry", input.Field))
		}
		if !filterOperatorAllowed(spec, input.Operator) {
			return FilterExpression{}, requestError("filter.operator", ErrorUnsupported, fmt.Sprintf("operator %q is not valid for %s", input.Operator, input.Field))
		}
		if len(input.Values) == 0 || ((input.Operator != FilterIn && input.Operator != FilterNotIn) && len(input.Values) != 1) {
			return FilterExpression{}, requestError("filter.values", ErrorInvalid, "operator requires one value; in/not_in require one or more values")
		}
		s.values += len(input.Values)
		if s.values > maxFilterValues || len(input.Values) > maxValuesPerFilter {
			return FilterExpression{}, requestError("filter.values", ErrorLimitExceeded, "too many filter values")
		}
		values, err := canonicalFilterValues(input.Field, spec, input.Values)
		if err != nil {
			return FilterExpression{}, err
		}
		return FilterExpression{Op: FilterPredicate, Field: input.Field, Operator: input.Operator, Values: values}, nil
	default:
		return FilterExpression{}, requestError("filter.op", ErrorUnsupported, "filter node op must be and, or, not or predicate")
	}
}

func canonicalFilterArgs(input []FilterExpression) []FilterExpression {
	type encodedArg struct {
		value FilterExpression
		json  string
	}
	encoded := make([]encodedArg, 0, len(input))
	for _, value := range input {
		data, _ := json.Marshal(value)
		encoded = append(encoded, encodedArg{value: value, json: string(data)})
	}
	sort.Slice(encoded, func(i, j int) bool { return encoded[i].json < encoded[j].json })
	result := make([]FilterExpression, 0, len(encoded))
	previous := ""
	for _, value := range encoded {
		if value.json == previous {
			continue
		}
		result = append(result, value.value)
		previous = value.json
	}
	return result
}

func filterOperatorAllowed(spec filterFieldSpec, operator FilterOperator) bool {
	operators := stringFilterOperators
	if spec.valueType == filterUint {
		operators = uintFilterOperators
	}
	for _, allowed := range operators {
		if operator == allowed {
			return true
		}
	}
	return false
}

func canonicalFilterValues(field string, spec filterFieldSpec, input []string) ([]string, error) {
	values := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, raw := range input {
		value := raw
		var err error
		switch spec.valueType {
		case filterString, filterEnum:
			normalized, normalizeErr := normalizeStrings("filter.values", []string{raw}, spec.allowed)
			if normalizeErr != nil {
				return nil, normalizeErr
			}
			value = normalized[0]
		case filterUint:
			value, err = canonicalUintFilterValue(field, raw, spec)
		case filterIP:
			value, err = canonicalIPFilterValue(raw)
		}
		if err != nil {
			return nil, err
		}
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return nil, requestError("filter.values", ErrorInvalid, "filter requires a value")
	}
	if len(values) > 1 {
		if spec.valueType == filterUint {
			sort.Slice(values, func(i, j int) bool {
				left, _ := strconv.ParseUint(values[i], 10, spec.bits)
				right, _ := strconv.ParseUint(values[j], 10, spec.bits)
				return left < right
			})
		} else {
			sort.Strings(values)
		}
	}
	return values, nil
}

func canonicalUintFilterValue(field, raw string, spec filterFieldSpec) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if alias, exists := spec.aliases[value]; exists {
		value = alias
	}
	if field == "asn" && strings.HasPrefix(value, "as") {
		value = strings.TrimPrefix(value, "as")
	}
	parsed, err := strconv.ParseUint(value, 10, spec.bits)
	if err != nil {
		return "", requestError("filter.values", ErrorInvalid, fmt.Sprintf("%s requires an unsigned %d-bit integer", field, spec.bits))
	}
	return strconv.FormatUint(parsed, 10), nil
}

func canonicalIPFilterValue(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix.Masked().String(), nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil || address.Zone() != "" {
		return "", requestError("filter.values", ErrorInvalid, "IP filter values must be an IPv4/IPv6 address or canonicalizable CIDR")
	}
	return address.String(), nil
}

type filterCompileState struct {
	parameters []proto.Parameter
	next       int
}

func compileBaseFilter(input FilterExpression) (string, []proto.Parameter, error) {
	canonical, err := CanonicalFilter(input)
	if err != nil {
		return "", nil, err
	}
	state := filterCompileState{parameters: make([]proto.Parameter, 0)}
	expression, err := state.compile(canonical)
	return expression, state.parameters, err
}

func (s *filterCompileState) compile(input FilterExpression) (string, error) {
	switch input.Op {
	case FilterAnd, FilterOr:
		parts := make([]string, 0, len(input.Args))
		for _, child := range input.Args {
			part, err := s.compile(child)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+part+")")
		}
		separator := " AND "
		if input.Op == FilterOr {
			separator = " OR "
		}
		return strings.Join(parts, separator), nil
	case FilterNot:
		part, err := s.compile(input.Args[0])
		if err != nil {
			return "", err
		}
		return "NOT (" + part + ")", nil
	case FilterPredicate:
		return s.compilePredicate(input)
	default:
		return "", requestError("filter.op", ErrorUnsupported, "unsupported canonical filter operation")
	}
}

func (s *filterCompileState) compilePredicate(input FilterExpression) (string, error) {
	spec := filterFieldRegistry[input.Field]
	if spec.valueType == filterIP {
		parts := make([]string, 0, len(input.Values))
		for _, value := range input.Values {
			if strings.Contains(value, "/") {
				parameter := s.addString(clickHouseIPRange(value))
				parts = append(parts, fmt.Sprintf("isIPAddressInRange(toString(%s), %s)", spec.column, parameter))
			} else {
				parameter := s.addString(value)
				parts = append(parts, fmt.Sprintf("%s = toIPv6(%s)", spec.column, parameter))
			}
		}
		expression := parts[0]
		if len(parts) > 1 {
			expression = "(" + strings.Join(parts, " OR ") + ")"
		}
		if input.Operator == FilterNotEqual || input.Operator == FilterNotIn {
			expression = "NOT (" + expression + ")"
		}
		return expression, nil
	}
	parameters := make([]string, 0, len(input.Values))
	for _, value := range input.Values {
		if spec.valueType == filterUint {
			parameters = append(parameters, s.addUint(value, spec.bits))
		} else {
			parameters = append(parameters, s.addString(value))
		}
	}
	switch input.Operator {
	case FilterEqual:
		return fmt.Sprintf("%s = %s", spec.column, parameters[0]), nil
	case FilterNotEqual:
		return fmt.Sprintf("%s != %s", spec.column, parameters[0]), nil
	case FilterIn, FilterNotIn:
		operator := "IN"
		if input.Operator == FilterNotIn {
			operator = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", spec.column, operator, strings.Join(parameters, ", ")), nil
	case FilterGreaterThan, FilterGreaterThanOrEqual, FilterLessThan, FilterLessThanOrEqual:
		operator := map[FilterOperator]string{FilterGreaterThan: ">", FilterGreaterThanOrEqual: ">=", FilterLessThan: "<", FilterLessThanOrEqual: "<="}[input.Operator]
		return fmt.Sprintf("%s %s %s", spec.column, operator, parameters[0]), nil
	default:
		return "", requestError("filter.operator", ErrorUnsupported, "unsupported canonical filter operator")
	}
}

// ClickHouse stores IPv4 values in IPv6 columns as IPv4-mapped addresses.
// isIPAddressInRange does not match ::ffff:a.b.c.d against an IPv4 CIDR, so
// compile IPv4 ranges into the equivalent mapped IPv6 prefix.
func clickHouseIPRange(value string) string {
	prefix, err := netip.ParsePrefix(value)
	if err != nil || !prefix.Addr().Is4() {
		return value
	}
	mapped := netip.AddrFrom16(prefix.Addr().As16())
	return netip.PrefixFrom(mapped, prefix.Bits()+96).Masked().String()
}

func (s *filterCompileState) addString(value string) string {
	key := fmt.Sprintf("typed_filter_%d", s.next)
	s.next++
	s.parameters = append(s.parameters, stringParameter(key, value))
	return fmt.Sprintf("{%s:String}", key)
}

func (s *filterCompileState) addUint(value string, bits int) string {
	key := fmt.Sprintf("typed_filter_%d", s.next)
	s.next++
	parsed, _ := strconv.ParseUint(value, 10, bits)
	s.parameters = append(s.parameters, uintParameter(key, parsed))
	return fmt.Sprintf("{%s:UInt%d}", key, bits)
}
