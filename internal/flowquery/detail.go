// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	maxDetailRange            = 24 * time.Hour
	maxDetailLimit            = 500
	maxDetailValuesPerFilter  = 100
	maxDetailFilterValues     = 256
	detailCursorPrefix        = "v1."
	detailCursorPayloadSize   = 8 + 32
	minimumSupplierFactSchema = 2
)

type DetailEndpoint string

const (
	DetailEndpointSource      DetailEndpoint = "source"
	DetailEndpointDestination DetailEndpoint = "destination"
	DetailEndpointEither      DetailEndpoint = "either"
)

type DetailField string

const (
	DetailFieldReceivedTime          DetailField = "received_time"
	DetailFieldSourceIP              DetailField = "src_ip"
	DetailFieldDestinationIP         DetailField = "dst_ip"
	DetailFieldSourcePort            DetailField = "src_port"
	DetailFieldDestinationPort       DetailField = "dst_port"
	DetailFieldIPProtocol            DetailField = "ip_protocol"
	DetailFieldTCPFlags              DetailField = "tcp_flags"
	DetailFieldBusinessDirection     DetailField = "business_direction"
	DetailFieldBusiness              DetailField = "business"
	DetailFieldCategory              DetailField = "category"
	DetailFieldSourceASN             DetailField = "source_asn"
	DetailFieldDestinationASN        DetailField = "destination_asn"
	DetailFieldRemoteASN             DetailField = "remote_asn"
	DetailFieldRemoteASNSource       DetailField = "remote_asn_source"
	DetailFieldRemoteCountry         DetailField = "remote_country"
	DetailFieldRawBytes              DetailField = "raw_bytes"
	DetailFieldRawPackets            DetailField = "raw_packets"
	DetailFieldEstimatedBytes        DetailField = "estimated_bytes"
	DetailFieldEstimatedPackets      DetailField = "estimated_packets"
	DetailFieldEstimatedValid        DetailField = "estimated_valid"
	DetailFieldSamplingMode          DetailField = "sampling_mode"
	DetailFieldSamplingRate          DetailField = "sampling_rate"
	DetailFieldSamplingSource        DetailField = "sampling_source"
	DetailFieldQualityFlags          DetailField = "quality_flags"
	DetailFieldFlowDurationMS        DetailField = "flow_duration_ms"
	DetailFieldTargetID              DetailField = "target_id"
	DetailFieldDeviceID              DetailField = "device_id"
	DetailFieldExporterID            DetailField = "exporter_id"
	DetailFieldObservationIfIndex    DetailField = "observation_if_index"
	DetailFieldIngressIfIndex        DetailField = "ingress_if_index"
	DetailFieldEgressIfIndex         DetailField = "egress_if_index"
	DetailFieldObservationDirection  DetailField = "observation_direction"
	DetailFieldDimensionSnapshotID   DetailField = "dimension_snapshot_id"
	DetailFieldDimensionVersion      DetailField = "dimension_version"
	DetailFieldGeoVersion            DetailField = "geo_version"
	DetailFieldClassificationVersion DetailField = "classification_version"
	DetailFieldLocalIP               DetailField = "local_ip"
	DetailFieldRemoteIP              DetailField = "remote_ip"
	DetailFieldLocalPort             DetailField = "local_port"
	DetailFieldRemotePort            DetailField = "remote_port"
	DetailFieldLocalPrefixID         DetailField = "local_prefix_id"
	DetailFieldRemotePrefixID        DetailField = "remote_prefix_id"
	DetailFieldRemoteISPID           DetailField = "remote_isp_id"
	DetailFieldRemoteGeoContinentID  DetailField = "remote_geo_continent_id"
	DetailFieldRemoteGeoRegionID     DetailField = "remote_geo_region_id"
	DetailFieldRemoteGeoCountryID    DetailField = "remote_geo_country_id"
	DetailFieldRemoteGeoProvinceID   DetailField = "remote_geo_province_id"
	DetailFieldRemoteGeoCityID       DetailField = "remote_geo_city_id"
)

type DetailFilter string

const (
	DetailFilterDirection DetailFilter = "directions"
	DetailFilterCategory  DetailFilter = "categories"
	DetailFilterBusiness  DetailFilter = "businesses"
	DetailFilterTarget    DetailFilter = "target_ids"
	DetailFilterDevice    DetailFilter = "device_ids"
	DetailFilterExporter  DetailFilter = "exporter_ids"
)

type DetailViewCapability struct {
	View          View           `json:"view"`
	Fields        []DetailField  `json:"fields"`
	DefaultFields []DetailField  `json:"default_fields"`
	Filters       []DetailFilter `json:"filters"`
}

type detailFieldKind uint8

const (
	detailKindString detailFieldKind = iota + 1
	detailKindUInt64
	detailKindBool
	detailKindTime
)

type detailFieldSpec struct {
	field  DetailField
	column string
	kind   detailFieldKind
}

var detailFieldRegistry = map[DetailField]detailFieldSpec{
	DetailFieldReceivedTime:          {DetailFieldReceivedTime, "received_time", detailKindTime},
	DetailFieldSourceIP:              {DetailFieldSourceIP, "src_ip", detailKindString},
	DetailFieldDestinationIP:         {DetailFieldDestinationIP, "dst_ip", detailKindString},
	DetailFieldSourcePort:            {DetailFieldSourcePort, "src_port", detailKindUInt64},
	DetailFieldDestinationPort:       {DetailFieldDestinationPort, "dst_port", detailKindUInt64},
	DetailFieldIPProtocol:            {DetailFieldIPProtocol, "ip_protocol", detailKindUInt64},
	DetailFieldTCPFlags:              {DetailFieldTCPFlags, "tcp_flags", detailKindUInt64},
	DetailFieldBusinessDirection:     {DetailFieldBusinessDirection, "business_direction", detailKindString},
	DetailFieldBusiness:              {DetailFieldBusiness, "business", detailKindString},
	DetailFieldCategory:              {DetailFieldCategory, "category", detailKindString},
	DetailFieldSourceASN:             {DetailFieldSourceASN, "source_asn", detailKindUInt64},
	DetailFieldDestinationASN:        {DetailFieldDestinationASN, "destination_asn", detailKindUInt64},
	DetailFieldRemoteASN:             {DetailFieldRemoteASN, "remote_asn", detailKindUInt64},
	DetailFieldRemoteASNSource:       {DetailFieldRemoteASNSource, "remote_asn_source", detailKindString},
	DetailFieldRemoteCountry:         {DetailFieldRemoteCountry, "remote_country", detailKindString},
	DetailFieldRawBytes:              {DetailFieldRawBytes, "raw_bytes", detailKindUInt64},
	DetailFieldRawPackets:            {DetailFieldRawPackets, "raw_packets", detailKindUInt64},
	DetailFieldEstimatedBytes:        {DetailFieldEstimatedBytes, "estimated_bytes", detailKindUInt64},
	DetailFieldEstimatedPackets:      {DetailFieldEstimatedPackets, "estimated_packets", detailKindUInt64},
	DetailFieldEstimatedValid:        {DetailFieldEstimatedValid, "estimated_valid", detailKindBool},
	DetailFieldSamplingMode:          {DetailFieldSamplingMode, "sampling_mode", detailKindString},
	DetailFieldSamplingRate:          {DetailFieldSamplingRate, "sampling_rate", detailKindUInt64},
	DetailFieldSamplingSource:        {DetailFieldSamplingSource, "sampling_source", detailKindString},
	DetailFieldQualityFlags:          {DetailFieldQualityFlags, "quality_flags", detailKindUInt64},
	DetailFieldFlowDurationMS:        {DetailFieldFlowDurationMS, "flow_duration_ms", detailKindUInt64},
	DetailFieldTargetID:              {DetailFieldTargetID, "target_id", detailKindString},
	DetailFieldDeviceID:              {DetailFieldDeviceID, "device_id", detailKindString},
	DetailFieldExporterID:            {DetailFieldExporterID, "exporter_id", detailKindString},
	DetailFieldObservationIfIndex:    {DetailFieldObservationIfIndex, "observation_if_index", detailKindUInt64},
	DetailFieldIngressIfIndex:        {DetailFieldIngressIfIndex, "ingress_if_index", detailKindUInt64},
	DetailFieldEgressIfIndex:         {DetailFieldEgressIfIndex, "egress_if_index", detailKindUInt64},
	DetailFieldObservationDirection:  {DetailFieldObservationDirection, "observation_direction", detailKindString},
	DetailFieldDimensionSnapshotID:   {DetailFieldDimensionSnapshotID, "dimension_snapshot_id", detailKindString},
	DetailFieldDimensionVersion:      {DetailFieldDimensionVersion, "dimension_version", detailKindUInt64},
	DetailFieldGeoVersion:            {DetailFieldGeoVersion, "geo_version", detailKindString},
	DetailFieldClassificationVersion: {DetailFieldClassificationVersion, "classification_version", detailKindUInt64},
	DetailFieldLocalIP:               {DetailFieldLocalIP, "local_ip", detailKindString},
	DetailFieldRemoteIP:              {DetailFieldRemoteIP, "remote_ip", detailKindString},
	DetailFieldLocalPort:             {DetailFieldLocalPort, "local_port", detailKindUInt64},
	DetailFieldRemotePort:            {DetailFieldRemotePort, "remote_port", detailKindUInt64},
	DetailFieldLocalPrefixID:         {DetailFieldLocalPrefixID, "local_prefix_id", detailKindString},
	DetailFieldRemotePrefixID:        {DetailFieldRemotePrefixID, "remote_prefix_id", detailKindString},
	DetailFieldRemoteISPID:           {DetailFieldRemoteISPID, "remote_isp_id", detailKindUInt64},
	DetailFieldRemoteGeoContinentID:  {DetailFieldRemoteGeoContinentID, "remote_geo_continent_id", detailKindString},
	DetailFieldRemoteGeoRegionID:     {DetailFieldRemoteGeoRegionID, "remote_geo_region_id", detailKindString},
	DetailFieldRemoteGeoCountryID:    {DetailFieldRemoteGeoCountryID, "remote_geo_country_id", detailKindString},
	DetailFieldRemoteGeoProvinceID:   {DetailFieldRemoteGeoProvinceID, "remote_geo_province_id", detailKindString},
	DetailFieldRemoteGeoCityID:       {DetailFieldRemoteGeoCityID, "remote_geo_city_id", detailKindString},
}

var defaultDetailFields = []DetailField{
	DetailFieldSourceIP, DetailFieldDestinationIP, DetailFieldSourcePort, DetailFieldDestinationPort,
	DetailFieldIPProtocol, DetailFieldBusinessDirection, DetailFieldCategory, DetailFieldRemoteASN,
	DetailFieldRemoteCountry, DetailFieldRawBytes, DetailFieldEstimatedBytes, DetailFieldSamplingRate,
	DetailFieldQualityFlags,
}

var rawDetailFields = map[DetailField]struct{}{
	DetailFieldReceivedTime: {}, DetailFieldSourceIP: {}, DetailFieldDestinationIP: {},
	DetailFieldSourcePort: {}, DetailFieldDestinationPort: {}, DetailFieldIPProtocol: {}, DetailFieldTCPFlags: {},
	DetailFieldSourceASN: {}, DetailFieldDestinationASN: {}, DetailFieldRawBytes: {}, DetailFieldRawPackets: {},
	DetailFieldEstimatedBytes: {}, DetailFieldEstimatedPackets: {}, DetailFieldEstimatedValid: {},
	DetailFieldSamplingMode: {}, DetailFieldSamplingRate: {}, DetailFieldSamplingSource: {},
	DetailFieldQualityFlags: {}, DetailFieldFlowDurationMS: {}, DetailFieldTargetID: {}, DetailFieldDeviceID: {},
	DetailFieldExporterID: {}, DetailFieldObservationIfIndex: {}, DetailFieldIngressIfIndex: {},
	DetailFieldEgressIfIndex: {}, DetailFieldObservationDirection: {},
}

var defaultRawDetailFields = []DetailField{
	DetailFieldSourceIP, DetailFieldDestinationIP, DetailFieldSourcePort, DetailFieldDestinationPort,
	DetailFieldIPProtocol, DetailFieldSourceASN, DetailFieldDestinationASN, DetailFieldRawBytes,
	DetailFieldEstimatedBytes, DetailFieldEstimatedValid, DetailFieldSamplingRate, DetailFieldQualityFlags,
}

var supplierDetailFields = map[DetailField]struct{}{
	DetailFieldReceivedTime: {}, DetailFieldSourceIP: {}, DetailFieldDestinationIP: {},
	DetailFieldSourcePort: {}, DetailFieldDestinationPort: {}, DetailFieldIPProtocol: {}, DetailFieldTCPFlags: {},
	DetailFieldBusinessDirection: {}, DetailFieldCategory: {}, DetailFieldSourceASN: {}, DetailFieldDestinationASN: {},
	DetailFieldRemoteASN: {}, DetailFieldRemoteASNSource: {}, DetailFieldRemoteCountry: {},
	DetailFieldRawBytes: {}, DetailFieldRawPackets: {}, DetailFieldEstimatedBytes: {}, DetailFieldEstimatedPackets: {},
	DetailFieldEstimatedValid: {}, DetailFieldSamplingMode: {}, DetailFieldSamplingRate: {}, DetailFieldSamplingSource: {},
	DetailFieldQualityFlags: {}, DetailFieldFlowDurationMS: {}, DetailFieldTargetID: {}, DetailFieldDeviceID: {},
	DetailFieldExporterID: {}, DetailFieldObservationIfIndex: {}, DetailFieldIngressIfIndex: {},
	DetailFieldEgressIfIndex: {}, DetailFieldObservationDirection: {}, DetailFieldDimensionSnapshotID: {},
	DetailFieldDimensionVersion: {}, DetailFieldGeoVersion: {}, DetailFieldClassificationVersion: {},
	DetailFieldLocalIP: {}, DetailFieldRemoteIP: {}, DetailFieldLocalPort: {}, DetailFieldRemotePort: {},
	DetailFieldRemoteISPID: {}, DetailFieldRemoteGeoContinentID: {}, DetailFieldRemoteGeoRegionID: {},
	DetailFieldRemoteGeoCountryID: {}, DetailFieldRemoteGeoProvinceID: {}, DetailFieldRemoteGeoCityID: {},
}

var defaultSupplierDetailFields = []DetailField{
	DetailFieldSourceIP, DetailFieldDestinationIP, DetailFieldSourcePort, DetailFieldDestinationPort,
	DetailFieldIPProtocol, DetailFieldBusinessDirection, DetailFieldCategory, DetailFieldRemoteASN,
	DetailFieldRemoteCountry, DetailFieldRawBytes, DetailFieldEstimatedBytes, DetailFieldSamplingRate,
	DetailFieldQualityFlags,
}

type detailViewSpec struct {
	fields        map[DetailField]struct{}
	defaultFields []DetailField
	filters       []DetailFilter
}

var detailViewOrder = []View{ViewRaw, ViewSupplier, ViewCustomer}

var detailViewRegistry = map[View]detailViewSpec{
	ViewRaw: {
		fields: rawDetailFields, defaultFields: defaultRawDetailFields,
		filters: []DetailFilter{DetailFilterTarget, DetailFilterDevice, DetailFilterExporter},
	},
	ViewSupplier: {
		fields: supplierDetailFields, defaultFields: defaultSupplierDetailFields,
		filters: []DetailFilter{DetailFilterDirection, DetailFilterCategory, DetailFilterTarget, DetailFilterDevice, DetailFilterExporter},
	},
	ViewCustomer: {
		defaultFields: defaultDetailFields,
		filters:       []DetailFilter{DetailFilterDirection, DetailFilterCategory, DetailFilterBusiness, DetailFilterTarget, DetailFilterDevice, DetailFilterExporter},
	},
}

type DetailFilters struct {
	Directions  []string `json:"directions,omitempty"`
	Categories  []string `json:"categories,omitempty"`
	Businesses  []string `json:"businesses,omitempty"`
	TargetIDs   []string `json:"target_ids,omitempty"`
	DeviceIDs   []string `json:"device_ids,omitempty"`
	ExporterIDs []string `json:"exporter_ids,omitempty"`
}

type DetailRequest struct {
	IP       string         `json:"ip"`
	Endpoint DetailEndpoint `json:"endpoint"`
	From     time.Time      `json:"from"`
	To       time.Time      `json:"to"`
	View     View           `json:"view"`
	Fields   []DetailField  `json:"fields,omitempty"`
	Filters  DetailFilters  `json:"filters,omitempty"`
	Limit    uint16         `json:"limit"`
	Cursor   string         `json:"cursor,omitempty"`
}

type detailCursorKey struct {
	eventTime time.Time
	recordID  [32]byte
}

type CompiledDetail struct {
	Query         ch.Query
	View          View
	From          time.Time
	To            time.Time
	IP            netip.Addr
	Endpoint      DetailEndpoint
	Fields        []DetailField
	Limit         uint16
	MaxResultRows uint64
	cursor        *detailCursorKey
}

func DetailFields() []DetailField {
	result := make([]DetailField, 0, len(detailFieldRegistry))
	for field := range detailFieldRegistry {
		result = append(result, field)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// DetailCapabilities returns an immutable snapshot of the supported detail
// views. It is the only capability source an HTTP/UI adapter should expose;
// validation below consumes the same registry.
func DetailCapabilities() []DetailViewCapability {
	result := make([]DetailViewCapability, 0, len(detailViewOrder))
	for _, view := range detailViewOrder {
		capability, _ := DetailCapability(view)
		result = append(result, capability)
	}
	return result
}

func DetailCapability(view View) (DetailViewCapability, error) {
	spec, ok := detailViewRegistry[view]
	if !ok {
		return DetailViewCapability{}, requestError("view", ErrorUnsupported, fmt.Sprintf("detail view %q is unsupported", view))
	}
	fields := DetailFields()
	if spec.fields != nil {
		fields = fields[:0]
		for _, field := range DetailFields() {
			if _, allowed := spec.fields[field]; allowed {
				fields = append(fields, field)
			}
		}
	}
	defaults, err := normalizeDetailFields(view, spec.defaultFields)
	if err != nil {
		return DetailViewCapability{}, err
	}
	return DetailViewCapability{
		View: view, Fields: fields, DefaultFields: defaults,
		Filters: append([]DetailFilter(nil), spec.filters...),
	}, nil
}

func CompileDetail(scope Scope, request DetailRequest, now time.Time) (CompiledDetail, error) {
	if !validTenant(scope.TenantID) {
		return CompiledDetail{}, requestError("scope.tenant_id", ErrorInvalid, "authenticated tenant identity is invalid")
	}
	if request.View == "" {
		return CompiledDetail{}, requestError("view", ErrorRequired, "view is required")
	}
	if _, ok := detailViewRegistry[request.View]; !ok {
		return CompiledDetail{}, requestError("view", ErrorUnsupported, "detail schema v2 supports raw, supplier, and customer views")
	}
	ip, err := netip.ParseAddr(request.IP)
	if err != nil || ip.Zone() != "" {
		return CompiledDetail{}, requestError("ip", ErrorInvalid, "ip must be an IPv4 or IPv6 address without a zone")
	}
	ip = ip.Unmap()
	if request.Endpoint != DetailEndpointSource && request.Endpoint != DetailEndpointDestination && request.Endpoint != DetailEndpointEither {
		return CompiledDetail{}, requestError("endpoint", ErrorUnsupported, "endpoint must be source, destination, or either")
	}
	from, to := request.From.UTC(), request.To.UTC()
	if request.From.IsZero() || request.To.IsZero() {
		return CompiledDetail{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || to.Sub(from) > maxDetailRange || from.Nanosecond()%int(time.Millisecond) != 0 || to.Nanosecond()%int(time.Millisecond) != 0 {
		return CompiledDetail{}, requestError("from/to", ErrorInvalid, "range must be increasing, millisecond-aligned, and no longer than 24 hours")
	}
	if to.After(now.UTC()) {
		return CompiledDetail{}, requestError("to", ErrorIncompleteRange, "to cannot be in the future")
	}
	if request.Limit < 1 || request.Limit > maxDetailLimit {
		return CompiledDetail{}, requestError("limit", ErrorLimitExceeded, "limit must be 1..500")
	}
	fields, err := normalizeDetailFields(request.View, request.Fields)
	if err != nil {
		return CompiledDetail{}, err
	}
	conditions, filterParameters, err := compileDetailFiltersForSource(request.View, request.Filters)
	if err != nil {
		return CompiledDetail{}, err
	}
	parameters := []proto.Parameter{
		stringParameter("tenant", scope.TenantID),
		stringParameter("from", formatDateTime64(from)),
		stringParameter("to", formatDateTime64(to)),
		stringParameter("ip", ip.String()),
		uintParameter("fetch_limit", uint64(request.Limit)+1),
	}
	parameters = append(parameters, filterParameters...)
	matchCondition := "(source.src_ip = toIPv6({ip:String}) OR source.dst_ip = toIPv6({ip:String}))"
	switch request.Endpoint {
	case DetailEndpointSource:
		matchCondition = "source.src_ip = toIPv6({ip:String})"
	case DetailEndpointDestination:
		matchCondition = "source.dst_ip = toIPv6({ip:String})"
	}
	var cursor *detailCursorKey
	if request.Cursor != "" {
		decoded, err := decodeDetailCursor(request.Cursor)
		if err != nil {
			return CompiledDetail{}, requestError("cursor", ErrorInvalid, err.Error())
		}
		if decoded.eventTime.Before(from) || !decoded.eventTime.Before(to) {
			return CompiledDetail{}, requestError("cursor", ErrorInvalid, "cursor is outside the requested time range")
		}
		cursor = &decoded
		parameters = append(parameters,
			stringParameter("cursor_time", formatDateTime64(decoded.eventTime)),
			stringParameter("cursor_record_id", hex.EncodeToString(decoded.recordID[:])),
		)
	}
	selectFields := make([]string, 0, len(fields))
	for _, field := range fields {
		spec := detailFieldSpecForView(request.View, field)
		selectFields = append(selectFields, fmt.Sprintf("  %s AS %s", detailFieldExpression(spec), spec.field))
	}
	visibilityCondition := "AND source.disposition = 'count'"
	if request.View == ViewRaw {
		visibilityCondition = ""
	}
	cursorCondition := ""
	if cursor != nil {
		cursorCondition = "AND (source.event_time < {cursor_time:DateTime64(3, 'UTC')} OR (source.event_time = {cursor_time:DateTime64(3, 'UTC')} AND source.record_id < unhex({cursor_record_id:String})))"
	}
	var body string
	if request.View == ViewSupplier {
		selectAliases := make([]string, 0, len(fields))
		for _, field := range fields {
			selectAliases = append(selectAliases, "  "+string(field))
		}
		cursorExpression := "true"
		if cursor != nil {
			cursorExpression = "(source.event_time < {cursor_time:DateTime64(3, 'UTC')} OR (source.event_time = {cursor_time:DateTime64(3, 'UTC')} AND source.record_id < unhex({cursor_record_id:String})))"
		}
		body = fmt.Sprintf(supplierDetailQuerySQL, strings.Join(selectAliases, ",\n"), strings.Join(selectFields, ",\n"), cursorExpression, visibilityCondition, matchCondition, strings.Join(conditions, "\n  "))
	} else {
		conditions = append(conditions, cursorCondition)
		body = fmt.Sprintf(detailQuerySQL, strings.Join(selectFields, ",\n"), visibilityCondition, matchCondition, strings.Join(conditions, "\n  "))
	}
	maxRows := uint64(request.Limit) + 1
	query := ch.Query{
		Body: body, Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "10", Important: true},
			{Key: "max_result_rows", Value: strconv.FormatUint(maxRows, 10), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: "5000000", Important: true},
			{Key: "max_bytes_to_read", Value: "1073741824", Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
		},
	}
	return CompiledDetail{
		Query: query, View: request.View, From: from, To: to, IP: ip, Endpoint: request.Endpoint,
		Fields: fields, Limit: request.Limit, MaxResultRows: maxRows, cursor: cursor,
	}, nil
}

func normalizeDetailFields(view View, input []DetailField) ([]DetailField, error) {
	spec, ok := detailViewRegistry[view]
	if !ok {
		return nil, requestError("view", ErrorUnsupported, fmt.Sprintf("detail view %q is unsupported", view))
	}
	if len(input) == 0 {
		input = spec.defaultFields
	}
	if len(input) > len(detailFieldRegistry) {
		return nil, requestError("fields", ErrorLimitExceeded, "too many fields")
	}
	requested := make(map[DetailField]struct{}, len(input))
	for _, field := range input {
		if _, exists := detailFieldRegistry[field]; !exists {
			return nil, requestError("fields", ErrorUnsupported, fmt.Sprintf("field %q is not in the detail registry", field))
		}
		if spec.fields != nil {
			if _, exists := spec.fields[field]; !exists {
				return nil, requestError("fields", ErrorUnsupported, fmt.Sprintf("field %q is not available in %s view", field, view))
			}
		}
		requested[field] = struct{}{}
	}
	result := make([]DetailField, 0, len(requested))
	for _, field := range DetailFields() {
		if _, exists := requested[field]; exists {
			result = append(result, field)
		}
	}
	return result, nil
}

func compileDetailFilters(view View, filters DetailFilters) ([]string, []proto.Parameter, error) {
	return compileDetailFiltersWithQualifier(view, filters, "")
}

func compileDetailFiltersForSource(view View, filters DetailFilters) ([]string, []proto.Parameter, error) {
	return compileDetailFiltersWithQualifier(view, filters, "source.")
}

func compileDetailFiltersWithQualifier(view View, filters DetailFilters, qualifier string) ([]string, []proto.Parameter, error) {
	viewSpec, ok := detailViewRegistry[view]
	if !ok {
		return nil, nil, requestError("view", ErrorUnsupported, fmt.Sprintf("detail view %q is unsupported", view))
	}
	type filterSpec struct {
		kind                  DetailFilter
		field, column, prefix string
		values                []string
		allowed               map[string]struct{}
	}
	filtersList := []filterSpec{
		{DetailFilterDirection, "filters.directions", "business_direction", "detail_direction", filters.Directions, validDirections},
		{DetailFilterCategory, "filters.categories", "category", "detail_category", filters.Categories, validCategories},
		{DetailFilterBusiness, "filters.businesses", "business", "detail_business", filters.Businesses, nil},
		{DetailFilterTarget, "filters.target_ids", "target_id", "detail_target", filters.TargetIDs, nil},
		{DetailFilterDevice, "filters.device_ids", "device_id", "detail_device", filters.DeviceIDs, nil},
		{DetailFilterExporter, "filters.exporter_ids", "exporter_id", "detail_exporter", filters.ExporterIDs, nil},
	}
	if view == ViewSupplier {
		filtersList[1].column = "supplier_category"
	}
	allowedFilters := make(map[DetailFilter]struct{}, len(viewSpec.filters))
	for _, filter := range viewSpec.filters {
		allowedFilters[filter] = struct{}{}
	}
	conditions := make([]string, 0, len(filtersList))
	parameters := make([]proto.Parameter, 0)
	total := 0
	for _, filter := range filtersList {
		if _, allowed := allowedFilters[filter.kind]; !allowed {
			if len(filter.values) != 0 {
				return nil, nil, requestError(filter.field, ErrorUnsupported, fmt.Sprintf("filter %q is not available in %s view", filter.kind, view))
			}
			continue
		}
		if len(filter.values) > maxDetailValuesPerFilter || total+len(filter.values) > maxDetailFilterValues {
			return nil, nil, requestError(filter.field, ErrorLimitExceeded, "too many detail filter values")
		}
		total += len(filter.values)
		values, err := normalizeStrings(filter.field, filter.values, filter.allowed)
		if err != nil {
			return nil, nil, err
		}
		placeholders := make([]string, 0, len(values))
		for index, value := range values {
			key := fmt.Sprintf("%s_%d", filter.prefix, index)
			placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
			parameters = append(parameters, stringParameter(key, value))
		}
		if len(placeholders) > 0 {
			conditions = append(conditions, fmt.Sprintf("AND %s%s IN (%s)", qualifier, filter.column, strings.Join(placeholders, ", ")))
		}
	}
	return conditions, parameters, nil
}

func detailFieldSpecForView(view View, field DetailField) detailFieldSpec {
	spec := detailFieldRegistry[field]
	if view != ViewSupplier {
		return spec
	}
	switch field {
	case DetailFieldCategory:
		spec.column = "supplier_category"
	case DetailFieldRemoteASN:
		spec.column = "supplier_remote_asn"
	case DetailFieldRemoteASNSource:
		spec.column = "supplier_remote_asn_source"
	case DetailFieldRemoteCountry:
		spec.column = "supplier_remote_country"
	case DetailFieldGeoVersion:
		spec.column = "supplier_geo_version"
	case DetailFieldRemoteISPID:
		spec.column = "supplier_remote_isp_id"
	case DetailFieldRemoteGeoContinentID:
		spec.column = "supplier_remote_geo_continent_id"
	case DetailFieldRemoteGeoRegionID:
		spec.column = "supplier_remote_geo_region_id"
	case DetailFieldRemoteGeoCountryID:
		spec.column = "supplier_remote_geo_country_id"
	case DetailFieldRemoteGeoProvinceID:
		spec.column = "supplier_remote_geo_province_id"
	case DetailFieldRemoteGeoCityID:
		spec.column = "supplier_remote_geo_city_id"
	}
	return spec
}

func detailFieldExpression(spec detailFieldSpec) string {
	column := "source." + spec.column
	switch spec.kind {
	case detailKindString:
		return "toString(" + column + ")"
	case detailKindUInt64:
		return "toUInt64(" + column + ")"
	default:
		return column
	}
}

func EncodeDetailCursor(eventTime time.Time, recordIDHex string) (string, error) {
	if eventTime.IsZero() || eventTime.UnixMilli() < 0 || eventTime.Nanosecond()%int(time.Millisecond) != 0 {
		return "", requestError("cursor", ErrorInvalid, "cursor event time must be millisecond-aligned")
	}
	recordID, err := hex.DecodeString(recordIDHex)
	if err != nil || len(recordID) != 32 {
		return "", requestError("cursor", ErrorInvalid, "cursor record id must be 64 hexadecimal characters")
	}
	payload := make([]byte, detailCursorPayloadSize)
	binary.BigEndian.PutUint64(payload[:8], uint64(eventTime.UTC().UnixMilli()))
	copy(payload[8:], recordID)
	return detailCursorPrefix + base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeDetailCursor(value string) (detailCursorKey, error) {
	if !strings.HasPrefix(value, detailCursorPrefix) {
		return detailCursorKey{}, fmt.Errorf("cursor version is unsupported")
	}
	encoded := strings.TrimPrefix(value, detailCursorPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(payload) != detailCursorPayloadSize || base64.RawURLEncoding.EncodeToString(payload) != encoded {
		return detailCursorKey{}, fmt.Errorf("cursor payload is malformed")
	}
	millis := binary.BigEndian.Uint64(payload[:8])
	if millis > uint64(^uint64(0)>>1) {
		return detailCursorKey{}, fmt.Errorf("cursor time is invalid")
	}
	key := detailCursorKey{eventTime: time.UnixMilli(int64(millis)).UTC()}
	copy(key.recordID[:], payload[8:])
	return key, nil
}

func formatDateTime64(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}

const detailQuerySQL = `SELECT
  source.event_time,
  lower(hex(source.record_id)) AS record_id,
  toString(source.src_ip) AS _source_ip,
  toString(source.dst_ip) AS _destination_ip,
%s
FROM flow_records AS source FINAL
WHERE source.tenant_id = {tenant:String}
  AND source.event_time >= {from:DateTime64(3, 'UTC')} AND source.event_time < {to:DateTime64(3, 'UTC')}
  %s
  AND %s
  %s
ORDER BY event_time DESC, record_id DESC
LIMIT {fetch_limit:UInt16}`

// Supplier completeness is computed before the cursor predicate. Every data
// row carries the full-scope minimum fact schema. If a cursor excludes the
// entire page, _scope_row=1 still returns one metadata-only row so callers
// cannot bypass old fact_schema=1 evidence with a forged/reused cursor.
const supplierDetailQuerySQL = `SELECT
  event_time,
  lower(hex(_record_id)) AS record_id,
  toString(_source_ip) AS _source_ip,
  toString(_destination_ip) AS _destination_ip,
%s,
  _scope_match,
  _minimum_fact_schema
FROM (
  SELECT
    source.event_time,
    source.record_id AS _record_id,
    source.src_ip AS _source_ip,
    source.dst_ip AS _destination_ip,
%s,
    %s AS _scope_match,
    min(source.fact_schema) OVER () AS _minimum_fact_schema,
    row_number() OVER (ORDER BY source.event_time DESC, source.record_id DESC) AS _scope_row
  FROM flow_records AS source FINAL
  WHERE source.tenant_id = {tenant:String}
    AND source.event_time >= {from:DateTime64(3, 'UTC')} AND source.event_time < {to:DateTime64(3, 'UTC')}
    %s
    AND %s
    %s
)
WHERE _scope_match OR _scope_row = 1
ORDER BY _scope_match DESC, event_time DESC, _record_id DESC
LIMIT {fetch_limit:UInt16}`
