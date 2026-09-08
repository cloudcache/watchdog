// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowEndpointDirectionSummary struct {
	Last    float64 `json:"last"`
	Average float64 `json:"average"`
	P95     float64 `json:"p95"`
	Maximum float64 `json:"maximum"`
	Total   float64 `json:"total"`
}

type flowEndpointCategorySummary struct {
	Inbound       float64  `json:"inbound"`
	Outbound      float64  `json:"outbound"`
	InboundShare  *float64 `json:"inbound_share,omitempty"`
	OutboundShare *float64 `json:"outbound_share,omitempty"`
}

type flowEndpointReportRow struct {
	flowTableRow
	Inbound    flowEndpointDirectionSummary           `json:"inbound"`
	Outbound   flowEndpointDirectionSummary           `json:"outbound"`
	Categories map[string]flowEndpointCategorySummary `json:"categories"`
	Residual   flowEndpointCategorySummary            `json:"residual"`
	Businesses []string                               `json:"businesses"`
}

type flowEndpointReportPage struct {
	Items         []flowEndpointReportRow            `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

var flowEndpointTableFields = func() map[string]struct{} {
	fields := make(map[string]struct{}, len(flowTableFields)+3+len(currentFlowReportCapabilities().Categories)+1)
	for field := range flowTableFields {
		fields[field] = struct{}{}
	}
	fields["inbound"] = struct{}{}
	fields["outbound"] = struct{}{}
	fields["business"] = struct{}{}
	for _, category := range currentFlowReportCapabilities().Categories {
		fields[category] = struct{}{}
	}
	fields["residual"] = struct{}{}
	return fields
}()

func normalizeFlowEndpointTableRequest(request *flowTableRequest) error {
	return normalizeFlowTableRequestWithFields(request, flowEndpointTableFields)
}

func composeFlowEndpointReportTable(panels []flowReportPanel, request flowTableRequest) ([]flowReportPanel, error) {
	endpointIndex := -1
	var endpointEnvelope struct {
		Table flowTablePage `json:"table"`
	}
	for index, panel := range panels {
		if panel.ID != "endpoint" || panel.Status != "ready" {
			continue
		}
		if err := json.Unmarshal(panel.Data, &endpointEnvelope); err != nil {
			return nil, err
		}
		endpointIndex = index
		break
	}
	if endpointIndex < 0 {
		return panels, nil
	}
	inbound := flowEndpointDirectionRows(panels, "endpoint_in")
	outbound := flowEndpointDirectionRows(panels, "endpoint_out")
	inboundCategories, err := flowEndpointCategoryRows(panels, "endpoint_category_in")
	if err != nil {
		return nil, err
	}
	outboundCategories, err := flowEndpointCategoryRows(panels, "endpoint_category_out")
	if err != nil {
		return nil, err
	}
	businesses, err := flowEndpointBusinessRows(panels, "endpoint_business")
	if err != nil {
		return nil, err
	}

	rows := make([]flowEndpointReportRow, 0, len(endpointEnvelope.Table.Items))
	for _, base := range endpointEnvelope.Table.Items {
		address := endpointAddress(base)
		in := inbound[address]
		out := outbound[address]
		row := flowEndpointReportRow{
			flowTableRow: base,
			Inbound:      endpointDirectionSummary(in),
			Outbound:     endpointDirectionSummary(out),
			Categories:   make(map[string]flowEndpointCategorySummary, len(currentFlowReportCapabilities().Categories)),
			Businesses:   append([]string(nil), businesses[address]...),
		}
		for _, category := range currentFlowReportCapabilities().Categories {
			row.Categories[category] = endpointCategorySummary(
				inboundCategories[address][category], outboundCategories[address][category], in.Total, out.Total,
			)
		}
		var residualIn, residualOut float64
		for _, category := range currentFlowReportCapabilities().Residuals {
			residualIn += inboundCategories[address][category]
			residualOut += outboundCategories[address][category]
		}
		row.Residual = endpointCategorySummary(residualIn, residualOut, in.Total, out.Total)
		rows = append(rows, row)
	}
	options := flowEndpointTableOptions(rows)
	filtered := rows[:0]
	search := strings.ToLower(request.Search)
	for _, row := range rows {
		if !flowEndpointTableMatchesFilters(row, request.Filters) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(flowEndpointTableSearchText(row)), search) {
			continue
		}
		filtered = append(filtered, row)
	}
	rows = filtered
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := flowEndpointTableCompare(rows[i], rows[j], request.SortBy)
		if comparison == 0 {
			comparison = strings.Compare(endpointAddress(rows[i].flowTableRow), endpointAddress(rows[j].flowTableRow))
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(rows)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	page := flowEndpointReportPage{
		Items: append([]flowEndpointReportRow(nil), rows[start:end]...), Total: total,
		Limit: request.Limit, Offset: request.Offset, FilterOptions: options,
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(panels[endpointIndex].Data, &envelope); err != nil {
		return nil, err
	}
	table, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	envelope["table"] = table
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	panels[endpointIndex].Data = data
	return panels, nil
}

func flowEndpointDirectionRows(panels []flowReportPanel, panelID string) map[string]flowTableRow {
	result := map[string]flowTableRow{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Table flowTablePage `json:"table"`
		}
		if json.Unmarshal(panel.Data, &envelope) != nil {
			return result
		}
		for _, row := range envelope.Table.Items {
			result[endpointAddress(row)] = row
		}
	}
	return result
}

func flowEndpointCategoryRows(panels []flowReportPanel, panelID string) (map[string]map[string]float64, error) {
	result := map[string]map[string]float64{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Summaries []struct {
				Address  string  `json:"address"`
				Category string  `json:"category"`
				Total    float64 `json:"total"`
			} `json:"summaries"`
		}
		if err := json.Unmarshal(panel.Data, &envelope); err != nil {
			return nil, err
		}
		for _, summary := range envelope.Summaries {
			if summary.Address == "" || summary.Category == "" {
				continue
			}
			if result[summary.Address] == nil {
				result[summary.Address] = map[string]float64{}
			}
			result[summary.Address][summary.Category] += summary.Total
		}
	}
	return result, nil
}

func flowEndpointBusinessRows(panels []flowReportPanel, panelID string) (map[string][]string, error) {
	sets := map[string]map[string]struct{}{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Points []flowquery.JointPoint `json:"points"`
		}
		if err := json.Unmarshal(panel.Data, &envelope); err != nil {
			return nil, err
		}
		for _, point := range envelope.Points {
			if len(point.DimensionValues) < 2 || point.Other || point.DimensionValues[0] == "" || point.DimensionValues[1] == "" {
				continue
			}
			if sets[point.DimensionValues[0]] == nil {
				sets[point.DimensionValues[0]] = map[string]struct{}{}
			}
			sets[point.DimensionValues[0]][point.DimensionValues[1]] = struct{}{}
		}
	}
	result := make(map[string][]string, len(sets))
	for address, values := range sets {
		for value := range values {
			result[address] = append(result[address], value)
		}
		sort.Strings(result[address])
	}
	return result, nil
}

func endpointAddress(row flowTableRow) string {
	if len(row.Path) > 0 {
		return row.Path[0]
	}
	return row.Label
}

func endpointDirectionSummary(row flowTableRow) flowEndpointDirectionSummary {
	return flowEndpointDirectionSummary{Last: row.Last, Average: row.Average, P95: row.P95, Maximum: row.Maximum, Total: row.Total}
}

func endpointCategorySummary(inbound, outbound, inboundTotal, outboundTotal float64) flowEndpointCategorySummary {
	result := flowEndpointCategorySummary{Inbound: inbound, Outbound: outbound}
	if inboundTotal > 0 {
		share := inbound / inboundTotal
		result.InboundShare = &share
	}
	if outboundTotal > 0 {
		share := outbound / outboundTotal
		result.OutboundShare = &share
	}
	return result
}

func flowEndpointTableValue(row flowEndpointReportRow, field string) string {
	if _, base := flowTableFields[field]; base {
		return flowTableValue(row.flowTableRow, field)
	}
	switch field {
	case "business":
		return strings.Join(row.Businesses, ",")
	case "inbound":
		return strconv.FormatFloat(row.Inbound.Total, 'g', -1, 64)
	case "outbound":
		return strconv.FormatFloat(row.Outbound.Total, 'g', -1, 64)
	case "residual":
		return endpointCategoryFilterValue(row.Residual)
	default:
		if value, ok := row.Categories[field]; ok {
			return endpointCategoryFilterValue(value)
		}
	}
	return ""
}

func endpointCategoryFilterValue(value flowEndpointCategorySummary) string {
	return fmt.Sprintf("%g/%g", value.Inbound, value.Outbound)
}

func flowEndpointTableOptions(rows []flowEndpointReportRow) map[string][]flowTableFilterOption {
	result := make(map[string][]flowTableFilterOption, len(flowEndpointTableFields))
	for field := range flowEndpointTableFields {
		counts := map[string]int{}
		for _, row := range rows {
			counts[flowEndpointTableValue(row, field)]++
		}
		values := make([]flowTableFilterOption, 0, len(counts))
		for value, count := range counts {
			values = append(values, flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(values, func(i, j int) bool { return values[i].Value < values[j].Value })
		result[field] = values
	}
	return result
}

func flowEndpointTableMatchesFilters(row flowEndpointReportRow, filters map[string][]string) bool {
	for field, values := range filters {
		if len(values) == 0 {
			continue
		}
		current := flowEndpointTableValue(row, field)
		matched := false
		for _, value := range values {
			matched = matched || value == current
		}
		if !matched {
			return false
		}
	}
	return true
}

func flowEndpointTableSearchText(row flowEndpointReportRow) string {
	values := make([]string, 0, len(flowEndpointTableFields))
	for field := range flowEndpointTableFields {
		values = append(values, flowEndpointTableValue(row, field))
	}
	return strings.Join(values, " ")
}

func flowEndpointTableCompare(left, right flowEndpointReportRow, field string) int {
	if field == "dimension" {
		return strings.Compare(left.Label, right.Label)
	}
	if field == "business" {
		return strings.Compare(strings.Join(left.Businesses, ","), strings.Join(right.Businesses, ","))
	}
	if value, ok := left.Categories[field]; ok {
		other := right.Categories[field]
		return cmpFloat64(value.Inbound+value.Outbound, other.Inbound+other.Outbound)
	}
	if field == "residual" {
		return cmpFloat64(left.Residual.Inbound+left.Residual.Outbound, right.Residual.Inbound+right.Residual.Outbound)
	}
	if field == "inbound" {
		return cmpFloat64(left.Inbound.Total, right.Inbound.Total)
	}
	if field == "outbound" {
		return cmpFloat64(left.Outbound.Total, right.Outbound.Total)
	}
	return flowTableCompare(left.flowTableRow, right.flowTableRow, field)
}
