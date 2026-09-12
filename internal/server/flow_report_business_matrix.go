// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// flow_report_business_matrix.go migrates the overview report's optional
// business_matrix table: a business×category matrix composed over the
// business_category_in/out joint panels the overview already runs (no new query).
// A faithful de-tenanted port of the hub composer, reusing the endpoint report's
// category-summary primitives and the six-class taxonomy vars.

var flowBusinessMatrixFields = func() map[string]struct{} {
	fields := map[string]struct{}{"business": {}, "total": {}, "residual": {}}
	for _, category := range flowReportCategories {
		fields[category] = struct{}{}
	}
	return fields
}()

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
			Categories: make(map[string]flowEndpointCategorySummary, len(flowReportCategories)),
		}
		var rowIn, rowOut, residualIn, residualOut float64
		for category, value := range inbound[business] {
			rowIn += value
			if containsFlowReportCategory(category, flowReportResiduals) {
				residualIn += value
			}
		}
		for category, value := range outbound[business] {
			rowOut += value
			if containsFlowReportCategory(category, flowReportResiduals) {
				residualOut += value
			}
		}
		row.Total = endpointCategorySummary(rowIn, rowOut, totalIn, totalOut)
		for _, category := range flowReportCategories {
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

// flowBusinessMatrixDirection reduces a business_category joint panel to the latest
// bucket's value per (business, category) (summing exact-tie buckets).
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
