// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_query_envelope.go is the hub wire contract for POST /flow/query: a gateway
// envelope carrying dataset/range/value-layer plus a nested `parameters` grammar.
// v2 keeps this exact wire shape (frontend unchanged) and maps it onto the direct
// flowquery runners — the QueryGateway machinery itself is not reintroduced.

type flowQueryEnvelope struct {
	Dataset         string              `json:"dataset,omitempty"`
	From            time.Time           `json:"from"`
	To              time.Time           `json:"to"`
	StepSeconds     uint32              `json:"step_seconds,omitempty"`
	Limit           uint32              `json:"limit,omitempty"`
	Cursor          string              `json:"cursor,omitempty"`
	ValueLayer      flowquery.View      `json:"value_layer,omitempty"`
	RequireComplete bool                `json:"require_complete,omitempty"`
	Parameters      flowQueryParameters `json:"parameters"`
}

// flowQueryParameters is the Flow grammar inside the envelope's `parameters` blob.
type flowQueryParameters struct {
	Metric             flowquery.Metric            `json:"metric,omitempty"`
	Dimension          flowquery.Dimension         `json:"dimension,omitempty"`
	Dimensions         []flowquery.Dimension       `json:"dimensions,omitempty"`
	Filters            flowquery.Filters           `json:"filters,omitempty"`
	Filter             *flowquery.FilterExpression `json:"filter,omitempty"`
	TopN               uint16                      `json:"top_n,omitempty"`
	IncludeOther       bool                        `json:"include_other,omitempty"`
	Timezone           string                      `json:"timezone,omitempty"`
	TargetPoints       uint16                      `json:"target_points,omitempty"`
	TimeWindows        []flowquery.LocalTimeWindow `json:"time_windows,omitempty"`
	DirectionSplit     bool                        `json:"direction_split,omitempty"`
	AddressSetFilter   *flowAddressSetFilter       `json:"address_set_filter,omitempty"`
	AddressSetEndpoint string                      `json:"address_set_endpoint,omitempty"`
	Operator           *flowOperatorSelection      `json:"operator_selection,omitempty"`
	Table              *flowTableRequest           `json:"table,omitempty"`
}

// flowAddressSetFilter selects an address-set combination (union of include_any,
// intersection of include_all, minus exclude_any).
type flowAddressSetFilter struct {
	IncludeAny []string `json:"include_any,omitempty"`
	IncludeAll []string `json:"include_all,omitempty"`
	ExcludeAny []string `json:"exclude_any,omitempty"`
}

// flowExportQueryEnvelope is the `query` payload of the hub export create contract
// (POST /flow/exports): a QueryRequest envelope whose `parameters` may carry a
// nested `report` spec — its presence discriminates a report export from an
// aggregate/joint query export.
type flowExportQueryEnvelope struct {
	Dataset         string                    `json:"dataset,omitempty"`
	From            time.Time                 `json:"from"`
	To              time.Time                 `json:"to"`
	StepSeconds     uint32                    `json:"step_seconds,omitempty"`
	Limit           uint32                    `json:"limit,omitempty"`
	Cursor          string                    `json:"cursor,omitempty"`
	ValueLayer      flowquery.View            `json:"value_layer,omitempty"`
	RequireComplete bool                      `json:"require_complete,omitempty"`
	Parameters      flowExportQueryParameters `json:"parameters"`
}

type flowExportQueryParameters struct {
	flowQueryParameters
	Report *flowReportSpecInput `json:"report,omitempty"`
}

// toReportRequest maps a report export envelope onto the internal report request.
func (env flowExportQueryEnvelope) toReportRequest() flowReportRequest {
	p := env.Parameters
	req := flowReportRequest{
		Metric: p.Metric, View: env.ValueLayer, Filters: p.Filters, Filter: p.Filter,
		From: env.From, To: env.To, TargetPoints: p.TargetPoints, TopN: p.TopN,
		Timezone: p.Timezone, Operator: p.Operator, Table: p.Table,
	}
	if p.Report != nil {
		req.Kind, req.Side, req.GroupBy = p.Report.Kind, p.Report.Side, p.Report.GroupBy
		req.DisplayMode, req.PeakWindows = p.Report.DisplayMode, p.Report.PeakWindows
		req.PanelIDs, req.Tables = p.Report.PanelIDs, p.Report.Tables
	}
	return req
}

// toAggregateInput maps a query export envelope onto the internal aggregate input.
func (env flowExportQueryEnvelope) toAggregateInput() flowAggregateInput {
	p := env.Parameters
	return flowAggregateInput{
		From: env.From, To: env.To, StepSeconds: env.StepSeconds, TargetPoints: p.TargetPoints,
		Metric: p.Metric, View: env.ValueLayer, Dimension: p.Dimension, Dimensions: p.Dimensions,
		Filters: p.Filters, Filter: p.Filter, TopN: p.TopN, IncludeOther: p.IncludeOther, Timezone: p.Timezone,
		Operator: p.Operator,
	}
}

// flowQueryResultMeta builds the hub `meta` envelope (the QueryResultMeta subset the
// clients read: unit/source/value_layer/timezone/step + completeness).
func flowQueryResultMeta(view flowquery.View, unit, source, timezone string, stepSeconds uint32, completeRatio float64, partial bool, operator *flowOperatorSelection) gin.H {
	meta := gin.H{
		"request_id": newID(), "schema_version": "query-result-v2", "as_of": time.Now().UTC(),
		"source": source, "value_layer": view, "unit": unit, "timezone": timezone,
		"step_seconds": stepSeconds, "complete_ratio": completeRatio, "unknown_ratio": 0, "partial": partial,
	}
	if operator != nil && operator.SchemaVersion != 0 {
		meta["versions"] = gin.H{
			"publication_ids": operator.PublicationIDs, "dimension_snapshot_ids": operator.DimensionSnapshotIDs,
			"classification_versions": operator.ClassificationVersions,
		}
		meta["operator_selection"] = operator
	}
	return meta
}
