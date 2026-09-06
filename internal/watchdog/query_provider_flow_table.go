package watchdog

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const (
	flowTableMaximumPageSize = 100
	flowTableMaximumSearch   = 256
	flowTableMaximumFilters  = 1_000
)

type flowTableRequest struct {
	Search        string              `json:"search,omitempty"`
	SortBy        string              `json:"sort_by,omitempty"`
	SortDirection string              `json:"sort_direction,omitempty"`
	Limit         uint16              `json:"limit"`
	Offset        uint32              `json:"offset,omitempty"`
	Filters       map[string][]string `json:"filters,omitempty"`
}

type flowTableRow struct {
	Name                   string   `json:"name"`
	Label                  string   `json:"label"`
	Path                   []string `json:"path"`
	Last                   float64  `json:"last"`
	Average                float64  `json:"average"`
	P95                    float64  `json:"p95"`
	Maximum                float64  `json:"maximum"`
	Minimum                float64  `json:"minimum"`
	Total                  float64  `json:"total"`
	ReceivedRecords        uint64   `json:"received_records"`
	UnknownSamplingRatio   float64  `json:"unknown_sampling_ratio"`
	QualityRecordRatio     float64  `json:"quality_record_ratio"`
	UnknownSamplingRecords uint64   `json:"unknown_sampling_records"`
	QualityRecords         uint64   `json:"quality_records"`
}

type flowTableFilterOption struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type flowTablePage struct {
	Items         []flowTableRow                     `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

var flowTableFields = map[string]struct{}{
	"dimension": {}, "last": {}, "average": {}, "p95": {}, "maximum": {}, "minimum": {},
	"total": {}, "records": {}, "unknown": {}, "quality": {},
}

func normalizeFlowTableRequest(request *flowTableRequest) error {
	if request == nil {
		return nil
	}
	request.Search = strings.TrimSpace(request.Search)
	if len(request.Search) > flowTableMaximumSearch {
		return errors.New("Flow table search must not exceed 256 bytes")
	}
	if request.Limit < 1 || request.Limit > flowTableMaximumPageSize {
		return errors.New("Flow table limit must be 1..100")
	}
	if request.Offset > 10_000 {
		return errors.New("Flow table offset must not exceed 10000")
	}
	if request.SortBy == "" {
		request.SortBy = "maximum"
	}
	if _, ok := flowTableFields[request.SortBy]; !ok {
		return fmt.Errorf("unsupported Flow table sort field %q", request.SortBy)
	}
	if request.SortDirection == "" {
		request.SortDirection = "desc"
	}
	if request.SortDirection != "asc" && request.SortDirection != "desc" {
		return errors.New("Flow table sort_direction must be asc or desc")
	}
	total := 0
	for field, values := range request.Filters {
		if _, ok := flowTableFields[field]; !ok {
			return fmt.Errorf("unsupported Flow table filter field %q", field)
		}
		if len(values) > flowTableMaximumPageSize {
			return fmt.Errorf("Flow table filter %q has too many values", field)
		}
		total += len(values)
		if total > flowTableMaximumFilters {
			return errors.New("Flow table has too many filter values")
		}
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if len(value) > flowTableMaximumSearch {
				return fmt.Errorf("Flow table filter %q contains an oversized value", field)
			}
			if _, exists := seen[value]; exists {
				return fmt.Errorf("Flow table filter %q contains duplicate values", field)
			}
			seen[value] = struct{}{}
		}
	}
	return nil
}

type flowTablePoint struct {
	bucket                time.Time
	path                  []string
	dimensionSnapshotID   string
	geoVersion            string
	classificationVersion uint32
	value                 float64
	received              uint64
	unknown               uint64
	quality               uint64
}

type flowTablePlan struct {
	from time.Time
	to   time.Time
	step time.Duration
}

func marshalFlowAggregateResult(result flowquery.Result, request *flowTableRequest) ([]byte, error) {
	if request == nil {
		return json.Marshal(result)
	}
	points := make([]flowTablePoint, 0, len(result.Points))
	for _, point := range result.Points {
		label := point.DimensionValue
		if point.Other {
			label = "Other"
		}
		points = append(points, flowTablePoint{
			bucket: point.Bucket, path: []string{label}, dimensionSnapshotID: point.DimensionSnapshotID,
			geoVersion: point.GeoVersion, classificationVersion: point.ClassificationVersion, value: point.Value,
			received: point.ReceivedRecords, unknown: point.UnknownSamplingRecords, quality: point.QualityRecords,
		})
	}
	plan := flowTablePlan{}
	if result.Plan != nil {
		plan = flowTablePlan{from: result.Plan.EffectiveFrom, to: result.Plan.EffectiveTo, step: time.Duration(result.Plan.StepSeconds) * time.Second}
	}
	table := buildFlowTable(points, plan, result.Metric.Unit, *request)
	return json.Marshal(struct {
		flowquery.Result
		Table flowTablePage `json:"table"`
	}{Result: result, Table: table})
}

func marshalFlowJointResult(result flowquery.JointResult, request *flowTableRequest) ([]byte, error) {
	if request == nil {
		return json.Marshal(result)
	}
	points := make([]flowTablePoint, 0, len(result.Points))
	for _, point := range result.Points {
		path := append([]string(nil), point.DimensionValues...)
		if point.Other {
			for index := range path {
				path[index] = "Other"
			}
		}
		points = append(points, flowTablePoint{
			bucket: point.Bucket, path: path, dimensionSnapshotID: point.DimensionSnapshotID,
			geoVersion: point.GeoVersion, classificationVersion: point.ClassificationVersion, value: point.Value,
			received: point.ReceivedRecords, unknown: point.UnknownSamplingRecords, quality: point.QualityRecords,
		})
	}
	plan := flowTablePlan{
		from: result.Plan.EffectiveFrom, to: result.Plan.EffectiveTo,
		step: time.Duration(result.Plan.StepSeconds) * time.Second,
	}
	table := buildFlowTable(points, plan, result.Metric.Unit, *request)
	return json.Marshal(struct {
		flowquery.JointResult
		Table flowTablePage `json:"table"`
	}{JointResult: result, Table: table})
}

func buildFlowTable(points []flowTablePoint, plan flowTablePlan, unit string, request flowTableRequest) flowTablePage {
	type group struct {
		path    []string
		version string
		rows    []flowTablePoint
	}
	groups := make(map[string]*group)
	for _, point := range points {
		pathJSON, _ := json.Marshal(point.path)
		key := string(pathJSON) + "\x00" + point.dimensionSnapshotID + "\x00" + point.geoVersion + "\x00" + strconv.FormatUint(uint64(point.classificationVersion), 10)
		current := groups[key]
		if current == nil {
			current = &group{
				path: append([]string(nil), point.path...),
				version: point.dimensionSnapshotID + "/" + point.geoVersion + "/" +
					strconv.FormatUint(uint64(point.classificationVersion), 10),
			}
			groups[key] = current
		}
		current.rows = append(current.rows, point)
	}
	rows := make([]flowTableRow, 0, len(groups))
	for _, current := range groups {
		sort.Slice(current.rows, func(i, j int) bool { return current.rows[i].bucket.Before(current.rows[j].bucket) })
		values := flowTableValues(current.rows, plan)
		total := flowTableTotal(current.rows, plan, unit)
		average := flowTableAverage(values, total, plan, unit)
		var received, unknown, quality uint64
		for _, point := range current.rows {
			received += point.received
			unknown += point.unknown
			quality += point.quality
		}
		label := strings.Join(current.path, " → ")
		row := flowTableRow{
			Name: label + " [" + current.version + "]", Label: label, Path: append([]string(nil), current.path...),
			Last: values[len(values)-1], Average: average, P95: flowTablePercentile(values, .95),
			Maximum: flowTableMaximum(values), Minimum: flowTableMinimum(values), Total: total,
			ReceivedRecords: received, UnknownSamplingRecords: unknown, QualityRecords: quality,
		}
		if received > 0 {
			row.UnknownSamplingRatio = float64(unknown) / float64(received)
			row.QualityRecordRatio = float64(quality) / float64(received)
		}
		rows = append(rows, row)
	}
	options := flowTableOptions(rows)
	filtered := rows[:0]
	search := strings.ToLower(request.Search)
	for _, row := range rows {
		if !flowTableMatchesFilters(row, request.Filters) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(flowTableSearchText(row)), search) {
			continue
		}
		filtered = append(filtered, row)
	}
	rows = filtered
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := flowTableCompare(rows[i], rows[j], request.SortBy)
		if comparison == 0 {
			comparison = strings.Compare(rows[i].Name, rows[j].Name)
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(rows)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	items := append([]flowTableRow(nil), rows[start:end]...)
	return flowTablePage{Items: items, Total: total, Limit: request.Limit, Offset: request.Offset, FilterOptions: options}
}

func flowTableValues(rows []flowTablePoint, plan flowTablePlan) []float64 {
	if plan.from.IsZero() || !plan.to.After(plan.from) || plan.step <= 0 {
		values := make([]float64, len(rows))
		for index, row := range rows {
			values[index] = row.value
		}
		return values
	}
	existing := make(map[int64]float64, len(rows))
	for _, row := range rows {
		existing[row.bucket.UnixMilli()] = row.value
	}
	values := make([]float64, 0, int(plan.to.Sub(plan.from)/plan.step))
	for bucket := plan.from; bucket.Before(plan.to); bucket = bucket.Add(plan.step) {
		values = append(values, existing[bucket.UnixMilli()])
	}
	if len(values) == 0 {
		return []float64{0}
	}
	return values
}

func flowTableTotal(rows []flowTablePoint, plan flowTablePlan, unit string) float64 {
	var total float64
	for _, row := range rows {
		if unit != "bits_per_second" && unit != "packets_per_second" {
			total += row.value
			continue
		}
		seconds := plan.step.Seconds()
		if remaining := plan.to.Sub(row.bucket).Seconds(); remaining < seconds {
			seconds = remaining
		}
		seconds = max(0, seconds)
		value := row.value * seconds
		if unit == "bits_per_second" {
			value /= 8
		}
		total += value
	}
	return total
}

func flowTableAverage(values []float64, total float64, plan flowTablePlan, unit string) float64 {
	if unit == "bits_per_second" || unit == "packets_per_second" {
		seconds := max(1, plan.to.Sub(plan.from).Seconds())
		if unit == "bits_per_second" {
			return total * 8 / seconds
		}
		return total / seconds
	}
	var totalValues float64
	for _, value := range values {
		totalValues += value
	}
	return totalValues / float64(max(1, len(values)))
}

func flowTablePercentile(values []float64, ratio float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	return copyValues[max(0, int(math.Ceil(float64(len(copyValues))*ratio))-1)]
}

func flowTableMaximum(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		result = max(result, value)
	}
	return result
}

func flowTableMinimum(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		result = min(result, value)
	}
	return result
}

func flowTableValue(row flowTableRow, field string) string {
	switch field {
	case "dimension":
		return row.Label
	case "last":
		return strconv.FormatFloat(row.Last, 'g', -1, 64)
	case "average":
		return strconv.FormatFloat(row.Average, 'g', -1, 64)
	case "p95":
		return strconv.FormatFloat(row.P95, 'g', -1, 64)
	case "maximum":
		return strconv.FormatFloat(row.Maximum, 'g', -1, 64)
	case "minimum":
		return strconv.FormatFloat(row.Minimum, 'g', -1, 64)
	case "total":
		return strconv.FormatFloat(row.Total, 'g', -1, 64)
	case "records":
		return strconv.FormatUint(row.ReceivedRecords, 10)
	case "unknown":
		return strconv.FormatFloat(row.UnknownSamplingRatio, 'g', -1, 64)
	case "quality":
		return strconv.FormatFloat(row.QualityRecordRatio, 'g', -1, 64)
	default:
		return ""
	}
}

func flowTableOptions(rows []flowTableRow) map[string][]flowTableFilterOption {
	result := make(map[string][]flowTableFilterOption, len(flowTableFields))
	for field := range flowTableFields {
		counts := make(map[string]int)
		for _, row := range rows {
			counts[flowTableValue(row, field)]++
		}
		values := make([]flowTableFilterOption, 0, len(counts))
		for value, count := range counts {
			values = append(values, flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(values, func(i, j int) bool {
			if field == "dimension" {
				return values[i].Value < values[j].Value
			}
			left, _ := strconv.ParseFloat(values[i].Value, 64)
			right, _ := strconv.ParseFloat(values[j].Value, 64)
			return left < right
		})
		result[field] = values
	}
	return result
}

func flowTableMatchesFilters(row flowTableRow, filters map[string][]string) bool {
	for field, values := range filters {
		if len(values) == 0 {
			continue
		}
		want := flowTableValue(row, field)
		matched := false
		for _, value := range values {
			matched = matched || value == want
		}
		if !matched {
			return false
		}
	}
	return true
}

func flowTableSearchText(row flowTableRow) string {
	values := []string{row.Name}
	for field := range flowTableFields {
		values = append(values, flowTableValue(row, field))
	}
	return strings.Join(values, " ")
}

func flowTableCompare(left, right flowTableRow, field string) int {
	if field == "dimension" {
		return strings.Compare(left.Label, right.Label)
	}
	leftValue, _ := strconv.ParseFloat(flowTableValue(left, field), 64)
	rightValue, _ := strconv.ParseFloat(flowTableValue(right, field), 64)
	return cmpFloat64(leftValue, rightValue)
}

func cmpFloat64(left, right float64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
