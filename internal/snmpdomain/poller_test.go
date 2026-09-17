package snmpdomain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
)

type fixedQuery struct{ value uint64 }

func (q fixedQuery) Get(_ context.Context, request GetRequest) (QueryResponse, error) {
	return QueryResponse{VarBinds: []VarBind{{OID: request.OIDs[0], Value: q.value, ValueType: ValueCounter64}}}, nil
}
func (fixedQuery) Walk(context.Context, WalkRequest) (QueryResponse, error) {
	return QueryResponse{}, errors.New("unexpected walk")
}

type sampleStore struct{ rows []snmpch.Sample }

func (s *sampleStore) WriteSamples(_ context.Context, rows []snmpch.Sample) error {
	s.rows = append(s.rows, rows...)
	return nil
}

func TestPollerPreservesCounter64ForClickHouse(t *testing.T) {
	store := &sampleStore{}
	poller := Poller{Query: fixedQuery{value: 1<<53 + 33}, Writer: ClickHouseRawWriter{Store: store, AgentID: "agent-a"}}
	result, err := poller.Poll(context.Background(), PollJob{
		TargetID: "device-a", DeviceID: "device-a", Target: QueryTarget{Host: "192.0.2.1"},
		Profile: Profile{Version: Version2c}, SampledAt: time.Unix(100, 0),
		Recipes: []Recipe{{ID: "recipe-a", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: "ifHCInOctets", ValueType: ValueCounter64, NumericOID: "1.3.6.1.2.1.31.1.1.1.6.1", SampleIntervalSeconds: 60, Enabled: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SampleCount != 1 || len(store.rows) != 1 {
		t.Fatalf("result=%+v rows=%d", result, len(store.rows))
	}
	row := store.rows[0]
	if row.CounterValue != 1<<53+33 || row.CounterWidth != 64 || row.ValueKind != "counter" {
		t.Fatalf("row=%+v", row)
	}
	if row.DeviceID != "device-a" || row.AgentID != "agent-a" || row.SourceRunID == "" {
		t.Fatalf("identity=%+v", row)
	}
}

type missingHCInQuery struct {
	calls [][]string
}

func (q *missingHCInQuery) Get(_ context.Context, request GetRequest) (QueryResponse, error) {
	q.calls = append(q.calls, append([]string(nil), request.OIDs...))
	if len(q.calls) == 1 {
		return QueryResponse{VarBinds: []VarBind{{OID: "1.3.6.1.2.1.31.1.1.1.10.79", Value: uint64(900), ValueType: ValueCounter64}}}, nil
	}
	if len(q.calls) == 2 {
		return QueryResponse{VarBinds: []VarBind{{OID: "1.3.6.1.2.1.31.1.1.1.6.79", Value: nil, ValueType: ValueString}}}, nil
	}
	return QueryResponse{VarBinds: []VarBind{{OID: "1.3.6.1.2.1.2.2.1.10.79", Value: uint32(450), ValueType: ValueCounter32}}}, nil
}

func (*missingHCInQuery) Walk(context.Context, WalkRequest) (QueryResponse, error) {
	return QueryResponse{}, errors.New("unexpected walk")
}

func TestPollerFallsBackPerDirectionWhenHCCounterIsMissing(t *testing.T) {
	query := &missingHCInQuery{}
	store := &sampleStore{}
	poller := Poller{Query: query, Writer: ClickHouseRawWriter{Store: store}}
	result, err := poller.Poll(context.Background(), PollJob{
		TargetID: "device-a", DeviceID: "device-a", Target: QueryTarget{Host: "192.0.2.1"},
		Profile: Profile{Version: Version2c}, SampledAt: time.Unix(100, 0),
		Recipes: []Recipe{
			{ID: "in", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: MetricSNMPIfInOctetsTotal, ValueType: ValueCounter64, NumericOID: "1.3.6.1.2.1.31.1.1.1.6.79", OIDIndex: "79", SampleIntervalSeconds: 60, Enabled: true},
			{ID: "out", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: MetricSNMPIfOutOctetsTotal, ValueType: ValueCounter64, NumericOID: "1.3.6.1.2.1.31.1.1.1.10.79", OIDIndex: "79", SampleIntervalSeconds: 60, Enabled: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SampleCount != 2 || len(result.MissingRecipeIDs) != 0 || len(store.rows) != 2 {
		t.Fatalf("result=%+v rows=%+v", result, store.rows)
	}
	if len(query.calls) != 3 || len(query.calls[1]) != 1 || query.calls[1][0] != "1.3.6.1.2.1.31.1.1.1.6.79" || len(query.calls[2]) != 1 || query.calls[2][0] != "1.3.6.1.2.1.2.2.1.10.79" {
		t.Fatalf("fallback calls=%v", query.calls)
	}
	byMetric := map[string]snmpch.Sample{}
	for _, row := range store.rows {
		byMetric[row.Metric] = row
	}
	if row := byMetric[MetricSNMPIfInOctetsTotal]; row.CounterValue != 450 || row.CounterWidth != 32 {
		t.Fatalf("inbound fallback row=%+v", row)
	}
	if row := byMetric[MetricSNMPIfOutOctetsTotal]; row.CounterValue != 900 || row.CounterWidth != 64 {
		t.Fatalf("outbound primary row=%+v", row)
	}
}

type transientMissingHCInQuery struct {
	calls [][]string
}

func (q *transientMissingHCInQuery) Get(_ context.Context, request GetRequest) (QueryResponse, error) {
	q.calls = append(q.calls, append([]string(nil), request.OIDs...))
	if len(q.calls) == 1 {
		return QueryResponse{VarBinds: []VarBind{{OID: "1.3.6.1.2.1.31.1.1.1.10.79", Value: uint64(900), ValueType: ValueCounter64}}}, nil
	}
	return QueryResponse{VarBinds: []VarBind{{OID: "1.3.6.1.2.1.31.1.1.1.6.79", Value: uint64(450), ValueType: ValueCounter64}}}, nil
}

func (*transientMissingHCInQuery) Walk(context.Context, WalkRequest) (QueryResponse, error) {
	return QueryResponse{}, errors.New("unexpected walk")
}

func TestPollerRetriesPrimaryHCCounterBeforeFallback(t *testing.T) {
	query := &transientMissingHCInQuery{}
	store := &sampleStore{}
	poller := Poller{Query: query, Writer: ClickHouseRawWriter{Store: store}}
	result, err := poller.Poll(context.Background(), PollJob{
		TargetID: "device-a", DeviceID: "device-a", Target: QueryTarget{Host: "192.0.2.1"},
		Profile: Profile{Version: Version2c}, SampledAt: time.Unix(100, 0),
		Recipes: []Recipe{
			{ID: "in", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: MetricSNMPIfInOctetsTotal, ValueType: ValueCounter64, NumericOID: "1.3.6.1.2.1.31.1.1.1.6.79", OIDIndex: "79", SampleIntervalSeconds: 60, Enabled: true},
			{ID: "out", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: MetricSNMPIfOutOctetsTotal, ValueType: ValueCounter64, NumericOID: "1.3.6.1.2.1.31.1.1.1.10.79", OIDIndex: "79", SampleIntervalSeconds: 60, Enabled: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SampleCount != 2 || len(result.MissingRecipeIDs) != 0 || len(query.calls) != 2 {
		t.Fatalf("result=%+v calls=%v", result, query.calls)
	}
	for _, row := range store.rows {
		if row.CounterWidth != 64 {
			t.Fatalf("transient miss changed counter width: %+v", row)
		}
	}
}

func TestPortRecipesPublishStandardCounterFallbacks(t *testing.T) {
	recipes := portRecipes(DiscoveryContext{TargetID: "target-a", Device: Device{ID: "device-a"}}, Port{ID: "port-a", IfIndex: 79}, true, true)
	byMetric := make(map[string]Recipe, len(recipes))
	for _, recipe := range recipes {
		byMetric[recipe.MetricName] = recipe
	}
	if got := byMetric[MetricSNMPIfInOctetsTotal].Options["fallback_numeric_oid"]; got != "1.3.6.1.2.1.2.2.1.10.79" {
		t.Fatalf("inbound fallback OID=%q", got)
	}
	if got := byMetric[MetricSNMPIfOutOctetsTotal].Options["fallback_numeric_oid"]; got != "1.3.6.1.2.1.2.2.1.16.79" {
		t.Fatalf("outbound fallback OID=%q", got)
	}
}

type pollRepository struct {
	device     Device
	target     Target
	profile    Profile
	recipes    []Recipe
	marked     map[string]string
	deviceMark string
}

func (r *pollRepository) ListDueDevices(context.Context, int, time.Time) ([]string, error) {
	return []string{r.device.ID}, nil
}
func (r *pollRepository) ListRecipesByDevice(context.Context, string) ([]Recipe, error) {
	return r.recipes, nil
}
func (r *pollRepository) MarkRecipePollResult(_ context.Context, id string, _ time.Time, lastError string) error {
	r.marked[id] = lastError
	return nil
}
func (r *pollRepository) MarkDevicePollResult(_ context.Context, _ string, _ time.Time, lastError string) error {
	r.deviceMark = lastError
	return nil
}
func (r *pollRepository) GetDevice(context.Context, string) (Device, error)   { return r.device, nil }
func (r *pollRepository) GetTarget(context.Context, string) (Target, error)   { return r.target, nil }
func (r *pollRepository) GetProfile(context.Context, string) (Profile, error) { return r.profile, nil }

func TestPollRunnerUsesSingleDomainRepositories(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	store := &sampleStore{}
	repo := &pollRepository{
		device: Device{ID: "device-a", TargetID: "device-a", SNMPProfileID: "profile-a"},
		target: Target{ID: "device-a", Host: "192.0.2.1"}, profile: Profile{ID: "profile-a", Version: Version2c},
		recipes: []Recipe{{ID: "recipe-a", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", MetricName: "ifHCInOctets", ValueType: ValueCounter64, NumericOID: "1.3.6.1", SampleIntervalSeconds: 60, Enabled: true}},
		marked:  map[string]string{},
	}
	runner := PollRunner{Recipes: repo, Devices: repo, Targets: repo, Profiles: repo, Now: func() time.Time { return now }, GlobalConcurrency: 1, Poller: Poller{Query: fixedQuery{value: 42}, Writer: ClickHouseRawWriter{Store: store}}}
	result, err := runner.RunDue(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result != (PollRunnerResult{RecipeCount: 1, DeviceCount: 1, SampleCount: 1}) {
		t.Fatalf("result=%+v", result)
	}
	if repo.marked["recipe-a"] != "" || repo.deviceMark != "" {
		t.Fatalf("recipe marks=%v device mark=%q", repo.marked, repo.deviceMark)
	}
}

func TestSplitSNMPHostPort(t *testing.T) {
	for _, test := range []struct {
		input string
		host  string
		port  uint16
	}{{"192.0.2.1", "192.0.2.1", 161}, {"udp://[2001:db8::1]:1161", "2001:db8::1", 1161}, {"[2001:db8::2]", "2001:db8::2", 161}} {
		host, port := splitSNMPHostPort(test.input)
		if host != test.host || port != test.port {
			t.Fatalf("splitSNMPHostPort(%q)=(%q,%d), want (%q,%d)", test.input, host, port, test.host, test.port)
		}
	}
}
