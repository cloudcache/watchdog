// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	maxDetailFacetLimit  = 100
	maxDetailFacetSearch = 128
)

type DetailFacetRequest struct {
	IP            string               `json:"ip"`
	Endpoint      DetailEndpoint       `json:"endpoint"`
	From          time.Time            `json:"from"`
	To            time.Time            `json:"to"`
	View          View                 `json:"view"`
	Filters       DetailFilters        `json:"filters,omitempty"`
	ColumnFilters []DetailColumnFilter `json:"column_filters,omitempty"`
	Field         string               `json:"field"`
	Search        string               `json:"search,omitempty"`
	Limit         uint16               `json:"limit"`
}

type CompiledDetailFacet struct {
	Query         ch.Query
	View          View
	Field         string
	Kind          detailFieldKind
	Limit         uint16
	MaxResultRows uint64
}

type DetailFacetOption struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type DetailFacetResult struct {
	Field string              `json:"field"`
	Items []DetailFacetOption `json:"items"`
}

func CompileDetailFacet(scope Scope, request DetailFacetRequest, now time.Time) (CompiledDetailFacet, error) {
	if request.Limit < 1 || request.Limit > maxDetailFacetLimit {
		return CompiledDetailFacet{}, requestError("limit", ErrorLimitExceeded, "facet limit must be 1..100")
	}
	search := strings.TrimSpace(request.Search)
	if !utf8.ValidString(search) || len(search) > maxDetailFacetSearch {
		return CompiledDetailFacet{}, requestError("search", ErrorLimitExceeded, "facet search must be valid UTF-8 and no longer than 128 bytes")
	}
	request.Field = strings.TrimSpace(request.Field)
	spec, err := detailFieldSpecForFilter(request.View, request.Field)
	if err != nil {
		return CompiledDetailFacet{}, err
	}
	validated, err := CompileDetail(scope, DetailRequest{
		IP: request.IP, Endpoint: request.Endpoint, From: request.From, To: request.To, View: request.View,
		Fields: []DetailField{DetailFieldSourceIP}, Filters: request.Filters, ColumnFilters: request.ColumnFilters,
		Limit: 1,
	}, now)
	if err != nil {
		return CompiledDetailFacet{}, err
	}
	if request.View == ViewSupplier {
		return CompiledDetailFacet{}, requestError("view", ErrorUnsupported, "supplier detail facets require provenance-complete facts and are not available yet")
	}
	conditions, filterParameters, err := compileDetailFiltersForSource(request.View, request.Filters)
	if err != nil {
		return CompiledDetailFacet{}, err
	}
	columnConditions, columnParameters, err := compileDetailColumnFiltersForSource(request.View, request.Filters, request.ColumnFilters, request.Field)
	if err != nil {
		return CompiledDetailFacet{}, err
	}
	conditions = append(conditions, columnConditions...)
	filterParameters = append(filterParameters, columnParameters...)
	matchCondition := "(source.src_ip = toIPv6({ip:String}) OR source.dst_ip = toIPv6({ip:String}))"
	switch request.Endpoint {
	case DetailEndpointSource:
		matchCondition = "source.src_ip = toIPv6({ip:String})"
	case DetailEndpointDestination:
		matchCondition = "source.dst_ip = toIPv6({ip:String})"
	}
	visibilityCondition := "AND source.disposition = 'count'"
	if request.View == ViewRaw {
		visibilityCondition = ""
	}
	valueExpression := detailFacetValueExpression(spec)
	if search != "" {
		conditions = append(conditions, "AND positionCaseInsensitiveUTF8("+valueExpression+", {facet_search:String}) > 0")
	}
	parameters := []proto.Parameter{
		stringParameter("tenant", scope.TenantID),
		stringParameter("from", formatDateTime64(validated.From)),
		stringParameter("to", formatDateTime64(validated.To)),
		stringParameter("ip", validated.IP.String()),
		uintParameter("facet_limit", uint64(request.Limit)),
	}
	if search != "" {
		parameters = append(parameters, stringParameter("facet_search", search))
	}
	parameters = append(parameters, filterParameters...)
	body := fmt.Sprintf(detailFacetQuerySQL, valueExpression, visibilityCondition, matchCondition, strings.Join(conditions, "\n  "))
	maxRows := uint64(request.Limit)
	return CompiledDetailFacet{
		Query: ch.Query{
			Body: body, Parameters: parameters,
			Settings: []ch.Setting{
				{Key: "max_execution_time", Value: "10", Important: true},
				{Key: "max_result_rows", Value: strconv.FormatUint(maxRows, 10), Important: true},
				{Key: "result_overflow_mode", Value: "throw", Important: true},
				{Key: "max_rows_to_read", Value: "5000000", Important: true},
				{Key: "max_bytes_to_read", Value: "1073741824", Important: true},
			},
		},
		View: request.View, Field: request.Field, Kind: spec.kind, Limit: request.Limit, MaxResultRows: maxRows,
	}, nil
}

func detailFacetValueExpression(spec detailFieldSpec) string {
	column := "source." + spec.column
	if isDetailIPField(spec.field) {
		return "toString(" + column + ")"
	}
	switch spec.kind {
	case detailKindBool:
		return "if(" + column + ", 'true', 'false')"
	case detailKindTime:
		return "concat(formatDateTime(" + column + ", '%Y-%m-%dT%H:%i:%S.', 'UTC'), substring(formatDateTime(" + column + ", '%f', 'UTC'), 1, 3), 'Z')"
	default:
		return "CAST(" + column + " AS String)"
	}
}

func (r *DetailRunner) RunFacet(ctx context.Context, compiled CompiledDetailFacet) (DetailFacetResult, error) {
	if r == nil || r.executor == nil {
		return DetailFacetResult{}, errors.New("ClickHouse Flow detail runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || compiled.Field == "" || compiled.Limit < 1 ||
		compiled.Limit > maxDetailFacetLimit || compiled.MaxResultRows != uint64(compiled.Limit) {
		return DetailFacetResult{}, errors.New("compiled Flow detail facet query is invalid")
	}
	values := new(proto.ColStr)
	counts := new(proto.ColUInt64)
	query := compiled.Query
	query.Result = proto.Results{{Name: "value", Data: values}, {Name: "count", Data: counts}}
	result := DetailFacetResult{Field: compiled.Field}
	seen := make(map[string]struct{})
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		if values.Rows() != counts.Rows() || len(result.Items)+values.Rows() > int(compiled.MaxResultRows) {
			return errors.New("invalid ClickHouse Flow detail facet result shape")
		}
		for index := 0; index < values.Rows(); index++ {
			value := values.Row(index)
			if compiled.Kind == detailKindString && isDetailIPField(DetailField(compiled.Field)) {
				ip, err := netip.ParseAddr(value)
				if err != nil || ip.Zone() != "" {
					return errors.New("invalid ClickHouse Flow detail facet IP value")
				}
				value = ip.Unmap().String()
			}
			if _, exists := seen[value]; exists || counts.Row(index) == 0 {
				return errors.New("invalid ClickHouse Flow detail facet result value")
			}
			seen[value] = struct{}{}
			result.Items = append(result.Items, DetailFacetOption{Value: value, Count: counts.Row(index)})
		}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return DetailFacetResult{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow detail facet query: %w", err))
	}
	return result, nil
}

const detailFacetQuerySQL = `SELECT
  %s AS value,
  count() AS count
FROM flow_records AS source FINAL
WHERE source.tenant_id = {tenant:String}
  AND source.event_time >= {from:DateTime64(3, 'UTC')} AND source.event_time < {to:DateTime64(3, 'UTC')}
  %s
  AND %s
  %s
GROUP BY value
ORDER BY count DESC, value ASC
LIMIT {facet_limit:UInt16}`
