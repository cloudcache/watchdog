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
