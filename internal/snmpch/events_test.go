package snmpch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type eventExecutor struct {
	attempts int
	queries  []ch.Query
}

func (e *eventExecutor) Do(ctx context.Context, query ch.Query) error {
	e.queries = append(e.queries, query)
	if query.Input != nil {
		e.attempts++
		if e.attempts < 3 {
			return errors.New("temporary insert failure")
		}
		return nil
	}
	results := query.Result.(proto.Results)
	if strings.HasPrefix(query.Body, "SELECT count()") {
		results[0].Data.(*proto.ColUInt64).Append(1)
		return nil
	}
	if strings.Contains(query.Body, "GROUP BY value") {
		results[0].Data.(*proto.ColStr).Append("trap")
		results[1].Data.(*proto.ColUInt64).Append(1)
		return query.OnResult(ctx, proto.Block{Rows: 1})
	}
	results[0].Data.(*proto.ColStr).Append("event-a")
	results[1].Data.(*proto.ColStr).Append("device-a")
	results[2].Data.(*proto.ColLowCardinality[string]).Append("port")
	results[3].Data.(*proto.ColStr).Append("port-a")
	results[4].Data.(*proto.ColLowCardinality[string]).Append("trap")
	results[5].Data.(*proto.ColLowCardinality[string]).Append("warning")
	results[6].Data.(*proto.ColLowCardinality[string]).Append("link_down")
	results[7].Data.(*proto.ColStr).Append("Interface is down")
	results[8].Data.(*proto.ColStr).Append(`{"oid":"1.2.3"}`)
	results[9].Data.(*proto.ColDateTime64).Append(time.Date(2026, 9, 12, 1, 0, 0, 0, time.UTC))
	results[10].Data.(*proto.ColDateTime64).Append(time.Date(2026, 9, 12, 1, 0, 1, 0, time.UTC))
	return query.OnResult(ctx, proto.Block{Rows: 1})
}

func TestEventWriteRetriesWithStableDeduplicationToken(t *testing.T) {
	executor := &eventExecutor{}
	store, _ := New(executor)
	err := store.WriteEvents(context.Background(), []Event{{
		ID: "event-a", DeviceID: "device-a", EntityType: "port", EntityID: "port-a",
		Source: "trap", Severity: "warning", EventType: "link_down", Message: "Interface is down",
		OccurredAt: time.Date(2026, 9, 12, 1, 0, 0, 0, time.UTC),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if executor.attempts != 3 {
		t.Fatalf("attempts=%d; want 3", executor.attempts)
	}
	for _, query := range executor.queries {
		if query.Input == nil {
			continue
		}
		var token string
		for _, setting := range query.Settings {
			if setting.Key == "insert_deduplication_token" {
				token = setting.Value
			}
		}
		if token != "snmp-event:event-a:event-a:1" {
			t.Fatalf("deduplication token=%q", token)
		}
	}
}

func TestEventQueryAndFacetsAreBoundedAndParameterized(t *testing.T) {
	executor := &eventExecutor{}
	store, _ := New(executor)
	items, total, err := store.QueryEvents(context.Background(), EventQuery{
		DeviceID: "device-a", Search: "down", Severities: []string{"warning"},
		SortBy: "occurred_at", SortDirection: "DESC", Limit: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != "event-a" || items[0].Raw["oid"] != "1.2.3" || items[0].IngestedAt.IsZero() {
		t.Fatalf("items=%+v total=%d", items, total)
	}
	facets, err := store.QueryEventFacets(context.Background(), EventFacetQuery{
		EventQuery: EventQuery{DeviceID: "device-a", SortBy: "occurred_at", SortDirection: "DESC", Limit: 25},
		Field:      "source", FacetSearch: "tr",
	})
	if err != nil || len(facets) != 1 || facets[0].Value != "trap" {
		t.Fatalf("facets=%+v err=%v", facets, err)
	}
	last := executor.queries[len(executor.queries)-1]
	if !strings.Contains(last.Body, "{facet:String}") || strings.Contains(last.Body, "device-a") {
		t.Fatalf("facet SQL is not parameterized: %s", last.Body)
	}
}

func TestEventQueryRejectsUnboundedFilters(t *testing.T) {
	store, _ := New(&eventExecutor{})
	_, _, err := store.QueryEvents(context.Background(), EventQuery{DeviceID: "d", SortBy: "occurred_at", SortDirection: "DESC", Limit: 101})
	if err == nil {
		t.Fatal("oversized event page was accepted")
	}
}
