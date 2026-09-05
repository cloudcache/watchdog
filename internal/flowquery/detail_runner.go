// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

var ErrSupplierProvenanceUnavailable = errors.New("supplier provenance is unavailable for part of the requested fact range")

type DetailRunner struct {
	executor Executor
}

type DetailRow struct {
	EventTime time.Time           `json:"event_time"`
	RecordID  string              `json:"record_id"`
	Values    map[DetailField]any `json:"values"`
}

type DetailResult struct {
	View                       View          `json:"view"`
	Fields                     []DetailField `json:"fields"`
	Rows                       []DetailRow   `json:"rows"`
	HasMore                    bool          `json:"has_more"`
	NextCursor                 string        `json:"next_cursor,omitempty"`
	SupplierProvenanceComplete bool          `json:"supplier_provenance_complete,omitempty"`
	MinimumFactSchema          uint16        `json:"minimum_fact_schema,omitempty"`
}

func NewDetailRunner(executor Executor) (*DetailRunner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse Flow detail executor is required")
	}
	return &DetailRunner{executor: executor}, nil
}

func (r *DetailRunner) Run(ctx context.Context, compiled CompiledDetail) (DetailResult, error) {
	if r == nil || r.executor == nil {
		return DetailResult{}, errors.New("ClickHouse Flow detail runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || !compiled.To.After(compiled.From) || !compiled.IP.IsValid() ||
		!validDetailView(compiled.View) || !validDetailEndpoint(compiled.Endpoint) || compiled.Limit < 1 || compiled.Limit > maxDetailLimit ||
		compiled.MaxResultRows != uint64(compiled.Limit)+1 {
		return DetailResult{}, errors.New("compiled Flow detail query is invalid")
	}
	columns, err := newDetailResultColumns(compiled.View, compiled.Fields)
	if err != nil {
		return DetailResult{}, fmt.Errorf("compiled Flow detail query is invalid: %w", err)
	}
	query := compiled.Query
	query.Result = columns.results()
	state := detailResultState{
		compiled: compiled,
		seen:     make(map[detailResultKey]struct{}),
	}
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		if state.err != nil {
			return state.err
		}
		state.err = state.consume(columns)
		return state.err
	}
	if err := r.executor.Do(ctx, query); err != nil {
		if state.err != nil {
			return DetailResult{}, state.err
		}
		return DetailResult{}, fmt.Errorf("execute ClickHouse Flow detail query: %w", err)
	}
	if state.err != nil {
		return DetailResult{}, state.err
	}
	result := DetailResult{View: compiled.View, Fields: append([]DetailField(nil), compiled.Fields...), Rows: state.rows}
	if compiled.View == ViewSupplier {
		result.SupplierProvenanceComplete = true
		result.MinimumFactSchema = state.minimumFactSchema
	}
	if len(result.Rows) > int(compiled.Limit) {
		result.HasMore = true
		result.Rows = result.Rows[:compiled.Limit]
		last := result.Rows[len(result.Rows)-1]
		result.NextCursor, err = EncodeDetailCursor(last.EventTime, last.RecordID)
		if err != nil {
			return DetailResult{}, fmt.Errorf("encode Flow detail cursor: %w", err)
		}
	}
	return result, nil
}

type detailDynamicColumn struct {
	field DetailField
	data  proto.ColResult
	value func(int) any
}

type detailResultColumns struct {
	eventTime     *proto.ColDateTime64
	recordID      proto.ColStr
	sourceIP      proto.ColStr
	destinationIP proto.ColStr
	dynamic       []detailDynamicColumn
	scopeMatch    *proto.ColBool
	minimumSchema *proto.ColUInt16
}

func newDetailResultColumns(view View, fields []DetailField) (*detailResultColumns, error) {
	columns := &detailResultColumns{eventTime: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)}
	if view == ViewSupplier {
		columns.scopeMatch = new(proto.ColBool)
		columns.minimumSchema = new(proto.ColUInt16)
	}
	seen := make(map[DetailField]struct{}, len(fields))
	for _, field := range fields {
		if _, exists := seen[field]; exists {
			return nil, fmt.Errorf("duplicate field %q", field)
		}
		seen[field] = struct{}{}
		spec, exists := detailFieldRegistry[field]
		if !exists {
			return nil, fmt.Errorf("unknown field %q", field)
		}
		column, err := newDetailDynamicColumn(spec)
		if err != nil {
			return nil, err
		}
		columns.dynamic = append(columns.dynamic, column)
	}
	return columns, nil
}

func newDetailDynamicColumn(spec detailFieldSpec) (detailDynamicColumn, error) {
	switch spec.kind {
	case detailKindString:
		column := new(proto.ColStr)
		return detailDynamicColumn{field: spec.field, data: column, value: func(index int) any { return column.Row(index) }}, nil
	case detailKindUInt64:
		column := new(proto.ColUInt64)
		return detailDynamicColumn{field: spec.field, data: column, value: func(index int) any { return column.Row(index) }}, nil
	case detailKindBool:
		column := new(proto.ColBool)
		return detailDynamicColumn{field: spec.field, data: column, value: func(index int) any { return column.Row(index) }}, nil
	case detailKindTime:
		column := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
		return detailDynamicColumn{field: spec.field, data: column, value: func(index int) any { return column.Row(index).UTC() }}, nil
	default:
		return detailDynamicColumn{}, fmt.Errorf("field %q has an unsupported result type", spec.field)
	}
}

func (c *detailResultColumns) results() proto.Results {
	result := proto.Results{
		{Name: "event_time", Data: c.eventTime},
		{Name: "record_id", Data: &c.recordID},
		{Name: "_source_ip", Data: &c.sourceIP},
		{Name: "_destination_ip", Data: &c.destinationIP},
	}
	for _, column := range c.dynamic {
		result = append(result, proto.ResultColumn{Name: string(column.field), Data: column.data})
	}
	if c.scopeMatch != nil {
		result = append(result,
			proto.ResultColumn{Name: "_scope_match", Data: c.scopeMatch},
			proto.ResultColumn{Name: "_minimum_fact_schema", Data: c.minimumSchema},
		)
	}
	return result
}

func (c *detailResultColumns) rowCount() (int, error) {
	if c == nil || c.eventTime == nil {
		return 0, errors.New("detail result columns are not initialized")
	}
	want := c.eventTime.Rows()
	for name, rows := range map[string]int{
		"record_id": c.recordID.Rows(), "_source_ip": c.sourceIP.Rows(), "_destination_ip": c.destinationIP.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow detail result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	for _, column := range c.dynamic {
		if rows := column.data.Rows(); rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow detail result: column %s has %d rows, want %d", column.field, rows, want)
		}
	}
	if c.scopeMatch != nil {
		if c.minimumSchema == nil || c.scopeMatch.Rows() != want || c.minimumSchema.Rows() != want {
			return 0, errors.New("invalid ClickHouse Flow detail result: supplier metadata columns have inconsistent rows")
		}
	}
	return want, nil
}

type detailResultKey struct {
	millis   int64
	recordID [32]byte
}

type detailResultState struct {
	compiled            CompiledDetail
	rows                []DetailRow
	seen                map[detailResultKey]struct{}
	previous            *detailResultKey
	minimumFactSchema   uint16
	hasSupplierEvidence bool
	err                 error
}

func (s *detailResultState) consume(columns *detailResultColumns) error {
	count, err := columns.rowCount()
	if err != nil {
		return err
	}
	if len(s.rows)+count > int(s.compiled.MaxResultRows) {
		return fmt.Errorf("invalid ClickHouse Flow detail result: rows exceed hard limit %d", s.compiled.MaxResultRows)
	}
	for index := 0; index < count; index++ {
		if columns.scopeMatch != nil {
			minimum := columns.minimumSchema.Row(index)
			if minimum < minimumSupplierFactSchema {
				return ErrSupplierProvenanceUnavailable
			}
			if s.hasSupplierEvidence && minimum != s.minimumFactSchema {
				return errors.New("invalid ClickHouse Flow detail result: inconsistent supplier fact schema evidence")
			}
			s.minimumFactSchema = minimum
			s.hasSupplierEvidence = true
			if !columns.scopeMatch.Row(index) {
				continue
			}
		}
		row, key, err := s.row(columns, index)
		if err != nil {
			return err
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse Flow detail result: duplicate cursor key")
		}
		if s.previous != nil && !detailKeyBefore(key, *s.previous) {
			return errors.New("invalid ClickHouse Flow detail result: rows are not strictly descending by event_time and record_id")
		}
		if s.compiled.cursor != nil {
			boundary := detailResultKey{millis: s.compiled.cursor.eventTime.UnixMilli(), recordID: s.compiled.cursor.recordID}
			if !detailKeyBefore(key, boundary) {
				return errors.New("invalid ClickHouse Flow detail result: row does not follow the requested cursor")
			}
		}
		s.seen[key] = struct{}{}
		current := key
		s.previous = &current
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *detailResultState) row(columns *detailResultColumns, index int) (DetailRow, detailResultKey, error) {
	eventTime := columns.eventTime.Row(index).UTC()
	if eventTime.Before(s.compiled.From) || !eventTime.Before(s.compiled.To) || eventTime.Nanosecond()%int(time.Millisecond) != 0 {
		return DetailRow{}, detailResultKey{}, errors.New("invalid ClickHouse Flow detail result: event_time is outside or not millisecond-aligned")
	}
	recordIDText := columns.recordID.Row(index)
	recordID, err := hex.DecodeString(recordIDText)
	if err != nil || len(recordID) != 32 || hex.EncodeToString(recordID) != recordIDText {
		return DetailRow{}, detailResultKey{}, errors.New("invalid ClickHouse Flow detail result: record_id is not canonical 64-character hexadecimal")
	}
	sourceIP, err := parseResultIP(columns.sourceIP.Row(index))
	if err != nil {
		return DetailRow{}, detailResultKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: source ip: %w", err)
	}
	destinationIP, err := parseResultIP(columns.destinationIP.Row(index))
	if err != nil {
		return DetailRow{}, detailResultKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: destination ip: %w", err)
	}
	wanted := s.compiled.IP.Unmap()
	if (s.compiled.Endpoint == DetailEndpointSource && sourceIP != wanted) ||
		(s.compiled.Endpoint == DetailEndpointDestination && destinationIP != wanted) ||
		(s.compiled.Endpoint == DetailEndpointEither && sourceIP != wanted && destinationIP != wanted) {
		return DetailRow{}, detailResultKey{}, errors.New("invalid ClickHouse Flow detail result: row does not match the requested IP endpoint")
	}
	row := DetailRow{EventTime: eventTime, RecordID: recordIDText, Values: make(map[DetailField]any, len(columns.dynamic))}
	for _, column := range columns.dynamic {
		value := column.value(index)
		if isDetailIPField(column.field) {
			parsed, err := parseResultIP(value.(string))
			if err != nil {
				return DetailRow{}, detailResultKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: field %s: %w", column.field, err)
			}
			value = parsed.String()
		}
		row.Values[column.field] = value
	}
	key := detailResultKey{millis: eventTime.UnixMilli()}
	copy(key.recordID[:], recordID)
	return row, key, nil
}

func parseResultIP(value string) (netip.Addr, error) {
	ip, err := netip.ParseAddr(value)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, errors.New("not an IPv4 or IPv6 address")
	}
	return ip.Unmap(), nil
}

func detailKeyBefore(left, right detailResultKey) bool {
	return left.millis < right.millis || (left.millis == right.millis && bytes.Compare(left.recordID[:], right.recordID[:]) < 0)
}

func validDetailEndpoint(value DetailEndpoint) bool {
	return value == DetailEndpointSource || value == DetailEndpointDestination || value == DetailEndpointEither
}

func validDetailView(value View) bool {
	_, ok := detailViewRegistry[value]
	return ok
}

func isDetailIPField(field DetailField) bool {
	return field == DetailFieldSourceIP || field == DetailFieldDestinationIP || field == DetailFieldLocalIP || field == DetailFieldRemoteIP
}
