// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
	"github.com/parquet-go/parquet-go"
)

// flow_export_rows.go migrates the hub's aggregate/joint/report export row model
// (export_flow.go) to v2. A flow query result or a report's panels flatten into
// one wide, provenance-carrying tabular schema (flowExportRow) rendered to CSV or
// Parquet. A faithful de-tenanted port: the QueryValueLayer becomes the flow view,
// and the report envelope becomes the in-process panel slice + gin.H meta.

type flowExportRow struct {
	Bucket                    time.Time
	DimensionNames            []string
	DimensionValues           []string
	Other                     bool
	Metric                    string
	Unit                      string
	Value                     float64
	ValueLayer                flowquery.View
	ReceivedRecords           uint64
	UnknownSamplingRecords    uint64
	QualityRecords            uint64
	SamplingCompleteness      float64
	SamplingCompletenessKnown bool
	QualityRecordRatio        float64
	QualityRecordRatioKnown   bool
	DimensionSnapshotID       string
	GeoVersion                string
	ClassificationVersion     uint32
	ObservedAt                time.Time
	QueryCompleteRatio        float64
	QueryPartial              bool
	QuerySource               string
	QueryStepSeconds          uint32
	QueryAsOf                 time.Time
	ReportKind                string
	ReportPanel               string
	ReportPanelStatus         string
	ReportPanelReason         string
}

type flowExportDecoration struct {
	ValueLayer    flowquery.View
	CompleteRatio float64
	Partial       bool
	Source        string
	StepSeconds   uint32
	AsOf          time.Time
	ReportKind    string
	ReportPanel   string
	PanelStatus   string
	PanelReason   string
}

// flowQueryExportRows flattens a marshaled aggregate/joint query result (the
// /flow/query envelope) into export rows.
func flowQueryExportRows(data json.RawMessage, decoration flowExportDecoration) ([]flowExportRow, error) {
	var envelope struct {
		Points     json.RawMessage                 `json:"points"`
		Metric     flowquery.MetricDefinition      `json:"metric"`
		Dimension  flowquery.DimensionDefinition   `json:"dimension"`
		Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode flow export result: %w", err)
	}
	decorate := func(row *flowExportRow) {
		row.Metric = string(envelope.Metric.Name)
		row.Unit = envelope.Metric.Unit
		decorateFlowExportRow(row, decoration)
	}
	if len(envelope.Dimensions) > 0 {
		var points []flowquery.JointPoint
		if err := json.Unmarshal(envelope.Points, &points); err != nil {
			return nil, fmt.Errorf("decode flow joint export points: %w", err)
		}
		rows := make([]flowExportRow, 0, len(points))
		for _, point := range points {
			row := flowExportRow{
				Bucket: point.Bucket, DimensionNames: flowExportDimensionNames(envelope.Dimensions),
				DimensionValues: append([]string(nil), point.DimensionValues...), Other: point.Other,
				Value: point.Value, ReceivedRecords: point.ReceivedRecords,
				UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
				SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
				QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
				DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
				ClassificationVersion: point.ClassificationVersion, ObservedAt: point.ObservedAt,
			}
			decorate(&row)
			rows = append(rows, row)
		}
		return rows, nil
	}
	var points []flowquery.Point
	if err := json.Unmarshal(envelope.Points, &points); err != nil {
		return nil, fmt.Errorf("decode flow export points: %w", err)
	}
	rows := make([]flowExportRow, 0, len(points))
	for _, point := range points {
		row := flowExportRow{
			Bucket: point.Bucket, DimensionNames: []string{string(envelope.Dimension.Kind)},
			DimensionValues: []string{point.DimensionValue}, Other: point.Other,
			Value: point.Value, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
			DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
			ClassificationVersion: point.ClassificationVersion, ObservedAt: point.GeneratedAt,
		}
		decorate(&row)
		rows = append(rows, row)
	}
	return rows, nil
}

// flowReportExportRows flattens a report's panels into export rows, dispatching the
// special panels to their own extractors and emitting a status row for any panel
// that is not ready.
func flowReportExportRows(panels []flowReportPanel, kind string, view flowquery.View) ([]flowExportRow, error) {
	rows := make([]flowExportRow, 0, len(panels))
	for _, panel := range panels {
		stepSeconds, source, asOf, completeRatio, partial := flowPanelMeta(panel.Meta)
		decoration := flowExportDecoration{
			ValueLayer: view, CompleteRatio: completeRatio, Partial: partial,
			Source: source, StepSeconds: stepSeconds, AsOf: asOf,
			ReportKind: kind, ReportPanel: panel.ID, PanelStatus: panel.Status, PanelReason: panel.Reason,
		}
		if panel.Status != "ready" {
			rows = append(rows, flowExportRow{
				Bucket: asOf, Metric: "panel_status", Unit: "status", ValueLayer: view,
				QueryCompleteRatio: completeRatio, QueryPartial: true, QuerySource: source,
				QueryStepSeconds: stepSeconds, QueryAsOf: asOf, ReportKind: kind, ReportPanel: panel.ID,
				ReportPanelStatus: panel.Status, ReportPanelReason: panel.Reason,
			})
			continue
		}
		var panelRows []flowExportRow
		var err error
		switch panel.ID {
		case "observed":
			panelRows, err = flowObservedExportRows(panel.Data, decoration)
		case "vpn_share":
			panelRows, err = flowVPNShareExportRows(panel.Data, decoration)
		case "vpn_findings":
			panelRows, err = flowVPNSummaryExportRows(panel.Data, decoration)
		default:
			panelRows, err = flowQueryExportRows(panel.Data, decoration)
		}
		if err != nil {
			return nil, fmt.Errorf("export flow report panel %s: %w", panel.ID, err)
		}
		rows = append(rows, panelRows...)
	}
	return rows, nil
}

func flowVPNShareExportRows(data json.RawMessage, decoration flowExportDecoration) ([]flowExportRow, error) {
	var result struct {
		Points []flowOverseasVPNSharePoint `json:"points"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	rows := make([]flowExportRow, 0, len(result.Points)*5)
	for _, point := range result.Points {
		appendRow := func(metric, unit string, value float64) {
			row := flowExportRow{
				Bucket: point.GeneratedAt, Metric: metric, Unit: unit, Value: value,
				DimensionNames: []string{"direction"}, DimensionValues: []string{point.Direction},
			}
			decorateFlowExportRow(&row, decoration)
			rows = append(rows, row)
		}
		appendRow("vpn_bytes", "bytes", float64(point.VPNBytes))
		appendRow("overseas_bytes", "bytes", point.TotalBytes)
		appendRow("unknown_geo_bytes", "bytes", float64(point.UnknownGeo))
		appendRow("finding_rows", "findings", float64(point.FindingRows))
		if point.Ratio != nil {
			appendRow("vpn_share", "ratio", *point.Ratio)
		}
	}
	return rows, nil
}

func flowObservedExportRows(data json.RawMessage, decoration flowExportDecoration) ([]flowExportRow, error) {
	var envelope struct {
		Result flowquery.OverseasResult `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	result := envelope.Result
	rows := make([]flowExportRow, 0, len(result.Points)*3)
	for _, point := range result.Points {
		base := flowExportRow{
			Bucket: point.Bucket, DimensionNames: []string{"geo_scope", "direction", "ip_family", "geo_value"},
			DimensionValues: []string{string(point.GeoScope), string(point.Direction), string(point.IPFamily), point.GeoValue},
			Other:           point.Other, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
			DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
			ClassificationVersion: point.ClassificationVersion, ObservedAt: point.GeneratedAt,
		}
		traffic := base
		traffic.Metric, traffic.Unit, traffic.Value = string(result.Metric.Name), result.Metric.Unit, point.Value
		decorateFlowExportRow(&traffic, decoration)
		rows = append(rows, traffic)
		for _, cardinality := range []struct {
			metric string
			unit   string
			value  uint64
		}{
			{"observed_remote_ips", "addresses", point.ObservedRemoteIPs},
			{"observed_local_hosts", "hosts", point.ObservedLocalHosts},
		} {
			row := base
			row.Metric, row.Unit, row.Value = cardinality.metric, cardinality.unit, float64(cardinality.value)
			decorateFlowExportRow(&row, decoration)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func flowVPNSummaryExportRows(data json.RawMessage, decoration flowExportDecoration) ([]flowExportRow, error) {
	var summary flowVPNReportSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, err
	}
	rows := make([]flowExportRow, 0)
	appendRow := func(bucket time.Time, metric, unit string, value float64, names, values []string) {
		row := flowExportRow{Bucket: bucket, Metric: metric, Unit: unit, Value: value, DimensionNames: names, DimensionValues: values}
		decorateFlowExportRow(&row, decoration)
		rows = append(rows, row)
	}
	asOf := decoration.AsOf
	for _, item := range []struct {
		metric string
		unit   string
		value  float64
	}{
		{"finding_count", "findings", float64(summary.FindingCount)},
		{"suspected_hosts", "hosts", float64(summary.SuspectedHosts)},
		{"high_risk_hosts", "hosts", float64(summary.HighRiskHosts)},
		{"active_ports", "ports", float64(summary.ActivePorts)},
		{"inbound_bytes", "bytes", float64(summary.InboundBytes)},
		{"outbound_bytes", "bytes", float64(summary.OutboundBytes)},
		{"total_bytes", "bytes", float64(summary.TotalBytes)},
		{"minimum_complete_ratio", "ratio", summary.MinimumCompleteRatio},
	} {
		appendRow(asOf, item.metric, item.unit, item.value, nil, nil)
	}
	for _, point := range summary.Trend {
		appendRow(point.Bucket, "inbound_bytes", "bytes", float64(point.InboundBytes), []string{"series"}, []string{"trend"})
		appendRow(point.Bucket, "outbound_bytes", "bytes", float64(point.OutboundBytes), []string{"series"}, []string{"trend"})
	}
	for _, distribution := range []struct {
		name  string
		items []flowReportCount
	}{{"port", summary.PortDistribution}, {"type", summary.TypeDistribution}} {
		for _, item := range distribution.items {
			appendRow(asOf, "finding_count", "findings", float64(item.Count), []string{"distribution", "value"}, []string{distribution.name, item.Value})
			appendRow(asOf, "traffic_bytes", "bytes", float64(item.Bytes), []string{"distribution", "value"}, []string{distribution.name, item.Value})
		}
	}
	return rows, nil
}

func decorateFlowExportRow(row *flowExportRow, decoration flowExportDecoration) {
	row.ValueLayer = decoration.ValueLayer
	row.QueryCompleteRatio = decoration.CompleteRatio
	row.QueryPartial = decoration.Partial
	row.QuerySource = decoration.Source
	row.QueryStepSeconds = decoration.StepSeconds
	row.QueryAsOf = decoration.AsOf
	row.ReportKind = decoration.ReportKind
	row.ReportPanel = decoration.ReportPanel
	row.ReportPanelStatus = decoration.PanelStatus
	row.ReportPanelReason = decoration.PanelReason
}

func flowExportDimensionNames(dimensions []flowquery.DimensionDefinition) []string {
	names := make([]string, 0, len(dimensions))
	for _, dimension := range dimensions {
		names = append(names, string(dimension.Kind))
	}
	return names
}

// flowPanelMeta best-effort extracts the export decoration fields from a v2 panel's
// gin.H meta. v2 panel meta is lighter than the hub's structured flowReportPanelMeta,
// so absent fields default to zero.
func flowPanelMeta(meta gin.H) (stepSeconds uint32, source string, asOf time.Time, completeRatio float64, partial bool) {
	if meta == nil {
		return 0, "", time.Time{}, 0, false
	}
	if v, ok := meta["step_seconds"]; ok {
		switch t := v.(type) {
		case uint32:
			stepSeconds = t
		case int:
			stepSeconds = uint32(t)
		case int64:
			stepSeconds = uint32(t)
		case float64:
			stepSeconds = uint32(t)
		}
	}
	if v, ok := meta["source"]; ok && v != nil {
		source = fmt.Sprint(v)
	}
	if v, ok := meta["as_of"].(time.Time); ok {
		asOf = v
	}
	if v, ok := meta["complete_ratio"].(float64); ok {
		completeRatio = v
	}
	if v, ok := meta["partial"].(bool); ok {
		partial = v
	}
	return stepSeconds, source, asOf, completeRatio, partial
}

var flowExportHeader = []string{
	"bucket", "dimension_1_name", "dimension_1_value", "dimension_2_name", "dimension_2_value",
	"dimension_3_name", "dimension_3_value", "dimension_4_name", "dimension_4_value", "other", "metric", "unit", "value", "value_layer",
	"received_records", "unknown_sampling_records", "quality_records", "sampling_completeness", "sampling_completeness_known",
	"quality_record_ratio", "quality_record_ratio_known", "dimension_snapshot_id", "geo_version", "classification_version",
	"observed_at", "query_complete_ratio", "query_partial", "query_source", "query_step_seconds", "query_as_of",
	"report_kind", "report_panel", "report_panel_status", "report_panel_reason",
}

func renderFlowExportCSV(rows []flowExportRow) ([]byte, error) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	if err := writer.Write(flowExportHeader); err != nil {
		return nil, err
	}
	for _, row := range rows {
		dimensionNames := [4]string{}
		dimensions := [4]string{}
		for index := 0; index < len(row.DimensionNames) && index < len(dimensionNames); index++ {
			dimensionNames[index] = safeSpreadsheetCell(row.DimensionNames[index])
		}
		for index := 0; index < len(row.DimensionValues) && index < len(dimensions); index++ {
			dimensions[index] = safeSpreadsheetCell(row.DimensionValues[index])
		}
		record := []string{
			row.Bucket.UTC().Format(time.RFC3339Nano),
			dimensionNames[0], dimensions[0], dimensionNames[1], dimensions[1],
			dimensionNames[2], dimensions[2], dimensionNames[3], dimensions[3],
			strconv.FormatBool(row.Other), safeSpreadsheetCell(row.Metric), safeSpreadsheetCell(row.Unit),
			strconv.FormatFloat(row.Value, 'f', -1, 64), string(row.ValueLayer), strconv.FormatUint(row.ReceivedRecords, 10),
			strconv.FormatUint(row.UnknownSamplingRecords, 10), strconv.FormatUint(row.QualityRecords, 10),
			strconv.FormatFloat(row.SamplingCompleteness, 'f', -1, 64), strconv.FormatBool(row.SamplingCompletenessKnown),
			strconv.FormatFloat(row.QualityRecordRatio, 'f', -1, 64), strconv.FormatBool(row.QualityRecordRatioKnown),
			safeSpreadsheetCell(row.DimensionSnapshotID), safeSpreadsheetCell(row.GeoVersion), strconv.FormatUint(uint64(row.ClassificationVersion), 10),
			row.ObservedAt.UTC().Format(time.RFC3339Nano), strconv.FormatFloat(row.QueryCompleteRatio, 'f', -1, 64),
			strconv.FormatBool(row.QueryPartial), safeSpreadsheetCell(row.QuerySource), strconv.FormatUint(uint64(row.QueryStepSeconds), 10),
			row.QueryAsOf.UTC().Format(time.RFC3339Nano),
			safeSpreadsheetCell(row.ReportKind), safeSpreadsheetCell(row.ReportPanel),
			safeSpreadsheetCell(row.ReportPanelStatus), safeSpreadsheetCell(row.ReportPanelReason),
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

type flowExportParquetRow struct {
	Bucket                    int64   `parquet:"bucket,timestamp(millisecond:utc)"`
	Dimension1Name            string  `parquet:"dimension_1_name,dict"`
	Dimension1Value           string  `parquet:"dimension_1_value,dict"`
	Dimension2Name            string  `parquet:"dimension_2_name,dict"`
	Dimension2Value           string  `parquet:"dimension_2_value,dict"`
	Dimension3Name            string  `parquet:"dimension_3_name,dict"`
	Dimension3Value           string  `parquet:"dimension_3_value,dict"`
	Dimension4Name            string  `parquet:"dimension_4_name,dict"`
	Dimension4Value           string  `parquet:"dimension_4_value,dict"`
	Other                     bool    `parquet:"other"`
	Metric                    string  `parquet:"metric,dict"`
	Unit                      string  `parquet:"unit,dict"`
	Value                     float64 `parquet:"value"`
	ValueLayer                string  `parquet:"value_layer,dict"`
	ReceivedRecords           uint64  `parquet:"received_records"`
	UnknownSamplingRecords    uint64  `parquet:"unknown_sampling_records"`
	QualityRecords            uint64  `parquet:"quality_records"`
	SamplingCompleteness      float64 `parquet:"sampling_completeness"`
	SamplingCompletenessKnown bool    `parquet:"sampling_completeness_known"`
	QualityRecordRatio        float64 `parquet:"quality_record_ratio"`
	QualityRecordRatioKnown   bool    `parquet:"quality_record_ratio_known"`
	DimensionSnapshotID       string  `parquet:"dimension_snapshot_id,dict"`
	GeoVersion                string  `parquet:"geo_version,dict"`
	ClassificationVersion     uint32  `parquet:"classification_version"`
	ObservedAt                int64   `parquet:"observed_at,timestamp(millisecond:utc)"`
	QueryCompleteRatio        float64 `parquet:"query_complete_ratio"`
	QueryPartial              bool    `parquet:"query_partial"`
	QuerySource               string  `parquet:"query_source,dict"`
	QueryStepSeconds          uint32  `parquet:"query_step_seconds"`
	QueryAsOf                 int64   `parquet:"query_as_of,timestamp(millisecond:utc)"`
	ReportKind                string  `parquet:"report_kind,dict"`
	ReportPanel               string  `parquet:"report_panel,dict"`
	ReportPanelStatus         string  `parquet:"report_panel_status,dict"`
	ReportPanelReason         string  `parquet:"report_panel_reason"`
}

func renderFlowExportParquet(rows []flowExportRow) ([]byte, error) {
	items := make([]flowExportParquetRow, 0, len(rows))
	for _, row := range rows {
		dimensionNames := [4]string{}
		dimensions := [4]string{}
		copy(dimensionNames[:], row.DimensionNames)
		copy(dimensions[:], row.DimensionValues)
		items = append(items, flowExportParquetRow{
			Bucket:         row.Bucket.UnixMilli(),
			Dimension1Name: dimensionNames[0], Dimension1Value: dimensions[0], Dimension2Name: dimensionNames[1], Dimension2Value: dimensions[1],
			Dimension3Name: dimensionNames[2], Dimension3Value: dimensions[2], Dimension4Name: dimensionNames[3], Dimension4Value: dimensions[3],
			Other: row.Other, Metric: row.Metric, Unit: row.Unit, Value: row.Value, ValueLayer: string(row.ValueLayer),
			ReceivedRecords: row.ReceivedRecords, UnknownSamplingRecords: row.UnknownSamplingRecords, QualityRecords: row.QualityRecords,
			SamplingCompleteness: row.SamplingCompleteness, SamplingCompletenessKnown: row.SamplingCompletenessKnown,
			QualityRecordRatio: row.QualityRecordRatio, QualityRecordRatioKnown: row.QualityRecordRatioKnown,
			DimensionSnapshotID: row.DimensionSnapshotID, GeoVersion: row.GeoVersion, ClassificationVersion: row.ClassificationVersion,
			ObservedAt: row.ObservedAt.UnixMilli(), QueryCompleteRatio: row.QueryCompleteRatio, QueryPartial: row.QueryPartial,
			QuerySource: row.QuerySource, QueryStepSeconds: row.QueryStepSeconds, QueryAsOf: row.QueryAsOf.UnixMilli(),
			ReportKind: row.ReportKind, ReportPanel: row.ReportPanel,
			ReportPanelStatus: row.ReportPanelStatus, ReportPanelReason: row.ReportPanelReason,
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, items); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
