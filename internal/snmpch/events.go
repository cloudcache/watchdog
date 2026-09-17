package snmpch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type Event struct {
	ID, DeviceID, EntityType, EntityID   string
	Source, Severity, EventType, Message string
	Raw                                  map[string]any
	OccurredAt                           time.Time
	IngestedAt                           time.Time
}

type EventQuery struct {
	DeviceID                        string
	Search                          string
	Severities, EventTypes, Sources []string
	SortBy, SortDirection           string
	Limit, Offset                   int
}

type EventFacetQuery struct {
	EventQuery
	Field, FacetSearch string
}

type EventFacet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

var eventSortColumns = map[string]string{
	"occurred_at": "occurred_at", "severity": "severity", "event_type": "event_type",
	"source": "source", "message": "message",
}

var eventFacetColumns = map[string]string{
	"severity": "severity", "event_type": "event_type", "source": "source",
}

func (s *Store) WriteEvents(ctx context.Context, events []Event) error {
	if s == nil || s.exec == nil {
		return errors.New("SNMP ClickHouse store is not initialized")
	}
	for start := 0; start < len(events); start += writeChunkSize {
		end := min(start+writeChunkSize, len(events))
		input, err := eventInput(events[start:end])
		if err != nil {
			return err
		}
		if len(input) == 0 {
			continue
		}
		query := ch.Query{Body: input.Into("snmp_events"), Input: input, Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: fmt.Sprintf("snmp-event:%s:%s:%d", events[start].ID, events[end-1].ID, end-start), Important: true},
		}}
		delay := 20 * time.Millisecond
		for attempt := 0; ; attempt++ {
			err = s.exec.Do(ctx, query)
			if err == nil {
				break
			}
			if attempt == 4 || ctx.Err() != nil {
				return fmt.Errorf("insert SNMP events: %w", errors.Join(err, ctx.Err()))
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.Join(err, ctx.Err())
			case <-timer.C:
			}
			delay *= 2
		}
	}
	return nil
}

func eventInput(events []Event) (proto.Input, error) {
	if len(events) == 0 {
		return nil, nil
	}
	var ids, devices, entityIDs, messages, raw proto.ColStr
	entityTypes := new(proto.ColStr).LowCardinality()
	sources := new(proto.ColStr).LowCardinality()
	severities := new(proto.ColStr).LowCardinality()
	types := new(proto.ColStr).LowCardinality()
	occurred := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	ingested := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	now := time.Now().UTC()
	for _, event := range events {
		if strings.TrimSpace(event.ID) == "" || strings.TrimSpace(event.DeviceID) == "" || event.OccurredAt.IsZero() ||
			strings.TrimSpace(event.Source) == "" || strings.TrimSpace(event.Severity) == "" || strings.TrimSpace(event.EventType) == "" {
			return nil, errors.New("invalid SNMP event")
		}
		encoded, err := json.Marshal(event.Raw)
		if err != nil {
			return nil, fmt.Errorf("encode SNMP event raw data: %w", err)
		}
		ids.Append(event.ID)
		occurred.Append(event.OccurredAt.UTC())
		ingested.Append(now)
		devices.Append(event.DeviceID)
		entityTypes.Append(event.EntityType)
		entityIDs.Append(event.EntityID)
		sources.Append(event.Source)
		severities.Append(event.Severity)
		types.Append(event.EventType)
		messages.Append(event.Message)
		raw.Append(string(encoded))
	}
	return proto.Input{
		{Name: "id", Data: &ids}, {Name: "occurred_at", Data: occurred}, {Name: "ingested_at", Data: ingested},
		{Name: "device_id", Data: &devices}, {Name: "entity_type", Data: entityTypes}, {Name: "entity_id", Data: &entityIDs},
		{Name: "source", Data: sources}, {Name: "severity", Data: severities}, {Name: "event_type", Data: types},
		{Name: "message", Data: &messages}, {Name: "raw_json", Data: &raw},
	}, nil
}

func (s *Store) QueryEvents(ctx context.Context, request EventQuery) ([]Event, int64, error) {
	if s == nil || s.exec == nil {
		return nil, 0, errors.New("SNMP ClickHouse store is not initialized")
	}
	if err := validateEventQuery(request); err != nil {
		return nil, 0, err
	}
	where, params := eventWhere(request, "")
	var totalColumn proto.ColUInt64
	count := ch.Query{Body: "SELECT count() FROM snmp_events FINAL WHERE " + where,
		Parameters: ch.Parameters(params), Result: proto.Results{{Name: "count()", Data: &totalColumn}}, Settings: s.querySettings(1)}
	if err := s.exec.Do(ctx, count); err != nil {
		return nil, 0, fmt.Errorf("count SNMP events: %w", err)
	}
	total := int64(0)
	if totalColumn.Rows() > 0 {
		total = int64(totalColumn[0])
	}
	var ids, devices, entityIDs, messages, raw proto.ColStr
	entityTypes := new(proto.ColStr).LowCardinality()
	sources := new(proto.ColStr).LowCardinality()
	severities := new(proto.ColStr).LowCardinality()
	types := new(proto.ColStr).LowCardinality()
	occurred := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	ingested := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	body := "SELECT id,device_id,entity_type,entity_id,source,severity,event_type,message,raw_json,occurred_at,ingested_at FROM snmp_events FINAL WHERE " + where +
		" ORDER BY " + eventSortColumns[request.SortBy] + " " + request.SortDirection + ", occurred_at DESC, id DESC LIMIT " + strconv.Itoa(request.Limit) + " OFFSET " + strconv.Itoa(request.Offset)
	query := ch.Query{Body: body, Parameters: ch.Parameters(params), Result: proto.Results{
		{Name: "id", Data: &ids}, {Name: "device_id", Data: &devices}, {Name: "entity_type", Data: entityTypes},
		{Name: "entity_id", Data: &entityIDs}, {Name: "source", Data: sources}, {Name: "severity", Data: severities},
		{Name: "event_type", Data: types}, {Name: "message", Data: &messages}, {Name: "raw_json", Data: &raw},
		{Name: "occurred_at", Data: occurred}, {Name: "ingested_at", Data: ingested},
	}, Settings: s.querySettings(uint32(request.Limit))}
	items := make([]Event, 0, request.Limit)
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for i := 0; i < block.Rows; i++ {
			item := Event{ID: ids.Row(i), DeviceID: devices.Row(i), EntityType: entityTypes.Row(i), EntityID: entityIDs.Row(i),
				Source: sources.Row(i), Severity: severities.Row(i), EventType: types.Row(i), Message: messages.Row(i),
				OccurredAt: occurred.Row(i).UTC(), IngestedAt: ingested.Row(i).UTC()}
			if err := json.Unmarshal([]byte(raw.Row(i)), &item.Raw); err != nil {
				return fmt.Errorf("decode SNMP event %s raw JSON: %w", item.ID, err)
			}
			items = append(items, item)
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return nil, 0, fmt.Errorf("query SNMP events: %w", err)
	}
	return items, total, nil
}

func (s *Store) QueryEventFacets(ctx context.Context, request EventFacetQuery) ([]EventFacet, error) {
	if s == nil || s.exec == nil {
		return nil, errors.New("SNMP ClickHouse store is not initialized")
	}
	if err := validateEventQuery(request.EventQuery); err != nil {
		return nil, err
	}
	if len(request.FacetSearch) > 96 {
		return nil, errors.New("SNMP event facet search must not exceed 96 characters")
	}
	column, ok := eventFacetColumns[request.Field]
	if !ok {
		return nil, errors.New("event facet field must be severity, event_type or source")
	}
	where, params := eventWhere(request.EventQuery, request.Field)
	if request.FacetSearch != "" {
		where += " AND positionCaseInsensitiveUTF8(" + column + ",{facet:String})>0"
		params["facet"] = request.FacetSearch
	}
	var values proto.ColStr
	var counts proto.ColUInt64
	body := "SELECT CAST(if(" + column + "='', '_unknown', " + column + ") AS String) value,count() count FROM snmp_events FINAL WHERE " + where +
		" GROUP BY value ORDER BY count DESC,value ASC LIMIT " + strconv.Itoa(request.Limit)
	query := ch.Query{Body: body, Parameters: ch.Parameters(params), Result: proto.Results{{Name: "value", Data: &values}, {Name: "count", Data: &counts}}, Settings: s.querySettings(uint32(request.Limit))}
	items := make([]EventFacet, 0, request.Limit)
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for i := 0; i < block.Rows; i++ {
			items = append(items, EventFacet{Value: values.Row(i), Count: int64(counts[i])})
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return nil, fmt.Errorf("query SNMP event facets: %w", err)
	}
	return items, nil
}

func validateEventQuery(request EventQuery) error {
	if request.DeviceID == "" || request.Limit < 1 || request.Limit > 100 || request.Offset < 0 || request.Offset > 100000 || len(request.Search) > 256 {
		return errors.New("invalid SNMP event query")
	}
	if _, ok := eventSortColumns[request.SortBy]; !ok || (request.SortDirection != "ASC" && request.SortDirection != "DESC") {
		return errors.New("invalid SNMP event sort")
	}
	for _, values := range [][]string{request.Severities, request.EventTypes, request.Sources} {
		if len(values) > 8 {
			return errors.New("SNMP event filter accepts at most 8 values")
		}
		for _, value := range values {
			if value == "" || len(value) > 96 {
				return errors.New("invalid SNMP event filter value")
			}
		}
	}
	return nil
}

func eventWhere(request EventQuery, excludedFacet string) (string, map[string]any) {
	where := []string{"device_id={device:String}"}
	params := map[string]any{"device": request.DeviceID}
	if request.Search != "" {
		where = append(where, "positionCaseInsensitiveUTF8(concat(source,' ',severity,' ',event_type,' ',message),{search:String})>0")
		params["search"] = request.Search
	}
	for _, filter := range []struct {
		name, column string
		values       []string
	}{{"severity", "severity", request.Severities}, {"event_type", "event_type", request.EventTypes}, {"source", "source", request.Sources}} {
		if filter.name == excludedFacet || len(filter.values) == 0 {
			continue
		}
		parts := make([]string, len(filter.values))
		for i, value := range filter.values {
			key := filter.name + strconv.Itoa(i)
			parts[i] = "{" + key + ":String}"
			params[key] = value
		}
		where = append(where, filter.column+" IN ("+strings.Join(parts, ",")+")")
	}
	return strings.Join(where, " AND "), params
}
