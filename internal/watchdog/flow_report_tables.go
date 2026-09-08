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

var (
	flowBusinessMatrixFields = func() map[string]struct{} {
		fields := map[string]struct{}{"business": {}, "total": {}, "residual": {}}
		for _, category := range currentFlowReportCapabilities().Categories {
			fields[category] = struct{}{}
		}
		return fields
	}()
	flowVPNDistributionFields = map[string]struct{}{"value": {}, "count": {}, "bytes": {}}
)

func normalizeFlowReportTables(spec *flowReportSpec) error {
	if spec == nil || len(spec.Tables) == 0 {
		return nil
	}
	if len(spec.Tables) > 3 {
		return fmt.Errorf("report.tables contains too many tables")
	}
	for id, table := range spec.Tables {
		if table == nil {
			return fmt.Errorf("report.tables.%s is required", id)
		}
		var fields map[string]struct{}
		switch spec.Kind {
		case flowReportOverview:
			if id != "business_matrix" {
				return fmt.Errorf("report.tables contains unsupported table %q", id)
			}
			fields = flowBusinessMatrixFields
			if table.SortBy == "" {
				table.SortBy = "total"
			}
		case flowReportVPN:
			if id != "port_distribution" && id != "type_distribution" {
				return fmt.Errorf("report.tables contains unsupported table %q", id)
			}
			fields = flowVPNDistributionFields
			if table.SortBy == "" {
				table.SortBy = "bytes"
			}
		default:
			return errorsForReportTables(spec.Kind)
		}
		if err := normalizeFlowTableRequestWithFields(table, fields); err != nil {
			return fmt.Errorf("report.tables.%s: %w", id, err)
		}
	}
	return nil
}

func errorsForReportTables(kind flowReportKind) error {
	return fmt.Errorf("report.tables is unsupported for report kind %q", kind)
}

type flowBusinessMatrixRow struct {
	Business   string                                 `json:"business"`
	Total      flowEndpointCategorySummary            `json:"total"`
	Categories map[string]flowEndpointCategorySummary `json:"categories"`
	Residual   flowEndpointCategorySummary            `json:"residual"`
}

type flowBusinessMatrixPage struct {
	Items         []flowBusinessMatrixRow            `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

func composeFlowBusinessMatrixTable(panels []flowReportPanel, request flowTableRequest) ([]flowReportPanel, error) {
	inbound, inboundIndex, err := flowBusinessMatrixDirection(panels, "business_category_in")
	if err != nil || inboundIndex < 0 {
		return panels, err
	}
	outbound, _, err := flowBusinessMatrixDirection(panels, "business_category_out")
	if err != nil {
		return nil, err
	}
	businesses := map[string]struct{}{}
	for business := range inbound {
		businesses[business] = struct{}{}
	}
	for business := range outbound {
		businesses[business] = struct{}{}
	}
	var totalIn, totalOut float64
	for _, categories := range inbound {
		for _, value := range categories {
			totalIn += value
		}
	}
	for _, categories := range outbound {
		for _, value := range categories {
			totalOut += value
		}
	}
	rows := make([]flowBusinessMatrixRow, 0, len(businesses))
	for business := range businesses {
		row := flowBusinessMatrixRow{
			Business:   business,
			Categories: make(map[string]flowEndpointCategorySummary, len(currentFlowReportCapabilities().Categories)),
		}
		var rowIn, rowOut, residualIn, residualOut float64
		for category, value := range inbound[business] {
			rowIn += value
			if containsFlowReportCategory(category, currentFlowReportCapabilities().Residuals) {
				residualIn += value
			}
		}
		for category, value := range outbound[business] {
			rowOut += value
			if containsFlowReportCategory(category, currentFlowReportCapabilities().Residuals) {
				residualOut += value
			}
		}
		row.Total = endpointCategorySummary(rowIn, rowOut, totalIn, totalOut)
		for _, category := range currentFlowReportCapabilities().Categories {
			row.Categories[category] = endpointCategorySummary(inbound[business][category], outbound[business][category], totalIn, totalOut)
		}
		row.Residual = endpointCategorySummary(residualIn, residualOut, totalIn, totalOut)
		rows = append(rows, row)
	}
	options := flowBusinessMatrixOptions(rows)
	filtered := rows[:0]
	search := strings.ToLower(request.Search)
	for _, row := range rows {
		if !flowBusinessMatrixMatches(row, request.Filters) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(flowBusinessMatrixSearchText(row)), search) {
			continue
		}
		filtered = append(filtered, row)
	}
	rows = filtered
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := flowBusinessMatrixCompare(rows[i], rows[j], request.SortBy)
		if comparison == 0 {
			comparison = strings.Compare(rows[i].Business, rows[j].Business)
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(rows)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	page := flowBusinessMatrixPage{
		Items: append([]flowBusinessMatrixRow(nil), rows[start:end]...), Total: total,
		Limit: request.Limit, Offset: request.Offset, FilterOptions: options,
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(panels[inboundIndex].Data, &envelope); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	envelope["matrix_table"] = encoded
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	panels[inboundIndex].Data = data
	return panels, nil
}

func flowBusinessMatrixDirection(panels []flowReportPanel, id string) (map[string]map[string]float64, int, error) {
	result := map[string]map[string]float64{}
	index := -1
	for panelIndex, panel := range panels {
		if panel.ID != id || panel.Status != "ready" {
			continue
		}
		index = panelIndex
		var envelope struct {
			Points []flowquery.JointPoint `json:"points"`
		}
		if err := json.Unmarshal(panel.Data, &envelope); err != nil {
			return nil, -1, err
		}
		latest := map[string]map[string]int64{}
		for _, point := range envelope.Points {
			if len(point.DimensionValues) < 2 {
				continue
			}
			business, category := point.DimensionValues[0], point.DimensionValues[1]
			if result[business] == nil {
				result[business] = map[string]float64{}
				latest[business] = map[string]int64{}
			}
			bucket := point.Bucket.UnixNano()
			switch {
			case bucket > latest[business][category]:
				latest[business][category] = bucket
				result[business][category] = point.Value
			case bucket == latest[business][category]:
				result[business][category] += point.Value
			}
		}
	}
	return result, index, nil
}

func containsFlowReportCategory(value string, values []string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func flowBusinessMatrixValue(row flowBusinessMatrixRow, field string) string {
	switch field {
	case "business":
		return row.Business
	case "total":
		return endpointCategoryFilterValue(row.Total)
	case "residual":
		return endpointCategoryFilterValue(row.Residual)
	default:
		return endpointCategoryFilterValue(row.Categories[field])
	}
}

func flowBusinessMatrixOptions(rows []flowBusinessMatrixRow) map[string][]flowTableFilterOption {
	result := make(map[string][]flowTableFilterOption, len(flowBusinessMatrixFields))
	for field := range flowBusinessMatrixFields {
		counts := map[string]int{}
		for _, row := range rows {
			counts[flowBusinessMatrixValue(row, field)]++
		}
		for value, count := range counts {
			result[field] = append(result[field], flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(result[field], func(i, j int) bool { return result[field][i].Value < result[field][j].Value })
	}
	return result
}

func flowBusinessMatrixMatches(row flowBusinessMatrixRow, filters map[string][]string) bool {
	for field, values := range filters {
		matched := len(values) == 0
		for _, value := range values {
			matched = matched || value == flowBusinessMatrixValue(row, field)
		}
		if !matched {
			return false
		}
	}
	return true
}

func flowBusinessMatrixSearchText(row flowBusinessMatrixRow) string {
	values := []string{row.Business}
	for field := range flowBusinessMatrixFields {
		values = append(values, flowBusinessMatrixValue(row, field))
	}
	return strings.Join(values, " ")
}

func flowBusinessMatrixCompare(left, right flowBusinessMatrixRow, field string) int {
	if field == "business" {
		return strings.Compare(left.Business, right.Business)
	}
	leftValue, rightValue := left.Total, right.Total
	if field == "residual" {
		leftValue, rightValue = left.Residual, right.Residual
	} else if field != "total" {
		leftValue, rightValue = left.Categories[field], right.Categories[field]
	}
	return cmpFloat64(leftValue.Inbound+leftValue.Outbound, rightValue.Inbound+rightValue.Outbound)
}

type flowReportCountPage struct {
	Items         []flowReportCount                  `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

func buildFlowVPNDistributionTable(source []flowReportCount, request flowTableRequest) *flowReportCountPage {
	options := map[string][]flowTableFilterOption{}
	for field := range flowVPNDistributionFields {
		counts := map[string]int{}
		for _, item := range source {
			counts[flowReportCountValue(item, field)]++
		}
		for value, count := range counts {
			options[field] = append(options[field], flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(options[field], func(i, j int) bool { return options[field][i].Value < options[field][j].Value })
	}
	filtered := make([]flowReportCount, 0, len(source))
	search := strings.ToLower(request.Search)
	for _, item := range source {
		matched := true
		for field, values := range request.Filters {
			fieldMatched := len(values) == 0
			for _, value := range values {
				fieldMatched = fieldMatched || value == flowReportCountValue(item, field)
			}
			matched = matched && fieldMatched
		}
		if matched && (search == "" || strings.Contains(strings.ToLower(item.Value), search)) {
			filtered = append(filtered, item)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		comparison := strings.Compare(filtered[i].Value, filtered[j].Value)
		if request.SortBy == "count" {
			comparison = cmpFloat64(float64(filtered[i].Count), float64(filtered[j].Count))
		} else if request.SortBy == "bytes" {
			comparison = cmpFloat64(float64(filtered[i].Bytes), float64(filtered[j].Bytes))
		}
		if comparison == 0 {
			comparison = strings.Compare(filtered[i].Value, filtered[j].Value)
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(filtered)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	return &flowReportCountPage{
		Items: append([]flowReportCount(nil), filtered[start:end]...), Total: total,
		Limit: request.Limit, Offset: request.Offset, FilterOptions: options,
	}
}

func flowReportCountValue(item flowReportCount, field string) string {
	switch field {
	case "value":
		return item.Value
	case "count":
		return strconv.FormatUint(item.Count, 10)
	case "bytes":
		return strconv.FormatUint(item.Bytes, 10)
	default:
		return ""
	}
}
