// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

var ErrSupplierProvenanceUnavailable = errors.New("supplier provenance is unavailable for part of the requested fact range")

type DetailRunner struct {
	executor Executor
}

type DetailRow struct {
	EventTime        time.Time           `json:"event_time"`
	SourceCoordinate SourceCoordinate    `json:"source_coordinate"`
	Values           map[DetailField]any `json:"values"`
}

// SourceCoordinate is the stable, traceable identity of a decoded Flow fact.
// It is also the ClickHouse replacement key suffix and pagination tie-breaker;
// no hashed surrogate identity is computed on the ingest or query path.
type SourceCoordinate struct {
	SourceStreamID string `json:"source_stream_id"`
	KafkaPartition uint32 `json:"kafka_partition"`
	KafkaOffset    uint64 `json:"kafka_offset"`
	RecordIndex    uint32 `json:"record_index"`
}

func (c SourceCoordinate) validate() error {
	if c.SourceStreamID == "" || len(c.SourceStreamID) > 128 || strings.TrimSpace(c.SourceStreamID) != c.SourceStreamID {
		return errors.New("source_stream_id must be 1..128 characters without surrounding whitespace")
	}
	for _, value := range []byte(c.SourceStreamID) {
		if !(value >= 'a' && value <= 'z') && !(value >= 'A' && value <= 'Z') &&
			!(value >= '0' && value <= '9') && value != '.' && value != '_' && value != ':' && value != '-' {
			return errors.New("source_stream_id contains unsupported characters")
		}
	}
	return nil
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
		compiled.MaxResultRows != uint64(compiled.Limit)+1 || compiled.Sort.Field == "" ||
		(compiled.Sort.Direction != "asc" && compiled.Sort.Direction != "desc") {
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
		return DetailResult{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow detail query: %w", err))
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
		result.NextCursor, err = encodeDetailCursorForRow(compiled, last)
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
	eventTime      *proto.ColDateTime64
	sourceStreamID proto.ColStr
	kafkaPartition proto.ColUInt32
	kafkaOffset    proto.ColUInt64
	recordIndex    proto.ColUInt32
	sourceIP       proto.ColStr
	destinationIP  proto.ColStr
	dynamic        []detailDynamicColumn
	scopeMatch     *proto.ColBool
	minimumSchema  *proto.ColUInt16
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
		{Name: "source_stream_id", Data: &c.sourceStreamID},
		{Name: "kafka_partition", Data: &c.kafkaPartition},
		{Name: "kafka_offset", Data: &c.kafkaOffset},
		{Name: "record_index", Data: &c.recordIndex},
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
		"source_stream_id": c.sourceStreamID.Rows(), "kafka_partition": c.kafkaPartition.Rows(),
		"kafka_offset": c.kafkaOffset.Rows(), "record_index": c.recordIndex.Rows(),
		"_source_ip": c.sourceIP.Rows(), "_destination_ip": c.destinationIP.Rows(),
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
	millis     int64
	coordinate SourceCoordinate
}

type detailSortKey struct {
	value      any
	eventTime  time.Time
	coordinate SourceCoordinate
}

type detailResultState struct {
	compiled            CompiledDetail
	rows                []DetailRow
	seen                map[detailResultKey]struct{}
	previous            *detailSortKey
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
		sortKey, err := detailSortKeyForRow(s.compiled, row, key)
		if err != nil {
			return err
		}
		if s.previous != nil && !detailSortKeyAfter(sortKey, *s.previous, s.compiled.Sort.Direction) {
			return errors.New("invalid ClickHouse Flow detail result: rows do not follow the requested stable sort")
		}
		if s.compiled.cursor != nil {
			boundary := detailSortKey{value: s.compiled.cursor.sortValue, eventTime: s.compiled.cursor.eventTime, coordinate: SourceCoordinate{
				SourceStreamID: s.compiled.cursor.sourceStreamID, KafkaPartition: s.compiled.cursor.kafkaPartition,
				KafkaOffset: s.compiled.cursor.kafkaOffset, RecordIndex: s.compiled.cursor.recordIndex,
			}}
			if !detailSortKeyAfter(sortKey, boundary, s.compiled.Sort.Direction) {
				return errors.New("invalid ClickHouse Flow detail result: row does not follow the requested cursor")
			}
		}
		s.seen[key] = struct{}{}
		current := sortKey
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
	coordinate := SourceCoordinate{
		SourceStreamID: columns.sourceStreamID.Row(index), KafkaPartition: columns.kafkaPartition.Row(index),
		KafkaOffset: columns.kafkaOffset.Row(index), RecordIndex: columns.recordIndex.Row(index),
	}
	if err := coordinate.validate(); err != nil {
		return DetailRow{}, detailResultKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: source coordinate: %w", err)
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
	row := DetailRow{EventTime: eventTime, SourceCoordinate: coordinate, Values: make(map[DetailField]any, len(columns.dynamic))}
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
	return row, detailResultKey{millis: eventTime.UnixMilli(), coordinate: coordinate}, nil
}

func parseResultIP(value string) (netip.Addr, error) {
	ip, err := netip.ParseAddr(value)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, errors.New("not an IPv4 or IPv6 address")
	}
	return ip.Unmap(), nil
}

func detailSortKeyForRow(compiled CompiledDetail, row DetailRow, key detailResultKey) (detailSortKey, error) {
	value := any(row.EventTime)
	if compiled.Sort.Field != "event_time" {
		var exists bool
		value, exists = row.Values[DetailField(compiled.Sort.Field)]
		if !exists {
			return detailSortKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: sort field %s is missing", compiled.Sort.Field)
		}
	}
	spec, _, _, err := normalizeDetailSort(compiled.View, compiled.Fields, compiled.Sort)
	if err != nil {
		return detailSortKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: %w", err)
	}
	if _, err := formatDetailCursorValue(spec, value); err != nil {
		return detailSortKey{}, fmt.Errorf("invalid ClickHouse Flow detail result: %w", err)
	}
	return detailSortKey{value: value, eventTime: row.EventTime, coordinate: key.coordinate}, nil
}

func detailSortKeyAfter(left, right detailSortKey, direction string) bool {
	comparison := compareDetailSortValue(left.value, right.value)
	if comparison == 0 {
		if left.eventTime.Before(right.eventTime) {
			comparison = -1
		} else if left.eventTime.After(right.eventTime) {
			comparison = 1
		} else {
			comparison = compareSourceCoordinate(left.coordinate, right.coordinate)
		}
	}
	if direction == "asc" {
		return comparison > 0
	}
	return comparison < 0
}

func compareSourceCoordinate(left, right SourceCoordinate) int {
	if comparison := strings.Compare(left.SourceStreamID, right.SourceStreamID); comparison != 0 {
		return comparison
	}
	if left.KafkaPartition != right.KafkaPartition {
		if left.KafkaPartition < right.KafkaPartition {
			return -1
		}
		return 1
	}
	if left.KafkaOffset != right.KafkaOffset {
		if left.KafkaOffset < right.KafkaOffset {
			return -1
		}
		return 1
	}
	if left.RecordIndex != right.RecordIndex {
		if left.RecordIndex < right.RecordIndex {
			return -1
		}
		return 1
	}
	return 0
}

func compareDetailSortValue(left, right any) int {
	switch l := left.(type) {
	case string:
		r, ok := right.(string)
		if !ok {
			return 0
		}
		return bytes.Compare([]byte(l), []byte(r))
	case uint64:
		r, ok := right.(uint64)
		if !ok || l == r {
			return 0
		}
		if l < r {
			return -1
		}
		return 1
	case bool:
		r, ok := right.(bool)
		if !ok || l == r {
			return 0
		}
		if !l && r {
			return -1
		}
		return 1
	case time.Time:
		r, ok := right.(time.Time)
		if !ok || l.Equal(r) {
			return 0
		}
		if l.Before(r) {
			return -1
		}
		return 1
	default:
		return 0
	}
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
