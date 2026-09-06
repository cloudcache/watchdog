package watchdog

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type collectorPlanRolloutRepositoryStub struct {
	created        CollectorPlanRollout
	preview        CollectorPlanRolloutPreview
	createRequest  CollectorPlanRolloutCreateRequest
	previewRequest CollectorPlanRolloutPreviewRequest
	createCalls    int
	previewCalls   int
}

func (r *collectorPlanRolloutRepositoryStub) CreateCollectorPlanRollout(_ context.Context, request CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error) {
	r.createRequest = request
	r.createCalls++
	return r.created, nil
}

func (r *collectorPlanRolloutRepositoryStub) PreviewCollectorPlanRollout(_ context.Context, request CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error) {
	r.previewRequest = request
	r.previewCalls++
	return r.preview, nil
}

func TestCollectorPlanRolloutServiceNormalizesBeforeRepository(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	repository := &collectorPlanRolloutRepositoryStub{}
	service, err := NewCollectorPlanRolloutService(repository)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.CreateCollectorPlanRollout(context.Background(), CollectorPlanRolloutCreateRequest{
		TenantID: "tenant-a", ActorID: "user-a",
		Selector: CollectorPlanRolloutSelector{
			ModuleKey: " FLOW ", AgentType: " FLOW-COLLECT ",
			CollectorIDs: []string{"collector-z", "collector-a"},
		},
		SpecJSON:          []byte(` { "schema_version": 1, "kafka": {"topic":"flows"} } `),
		PlanSchemaVersion: 1,
		Strategy: CollectorPlanRolloutStrategy{
			CanaryCount: 2, WaveSize: 10, MinSoakSeconds: 600, FailureBudget: 1,
		},
		ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := repository.createRequest
	if request.Selector.ModuleKey != "flow" || request.Selector.AgentType != "flow-collect" || request.Selector.Status != "active" || !reflect.DeepEqual(request.Selector.CollectorIDs, []string{"collector-a", "collector-z"}) || string(request.SpecJSON) != `{"kafka":{"topic":"flows"},"schema_version":1}` {
		t.Fatalf("normalized request=%+v spec=%s", request, request.SpecJSON)
	}
}

func TestCollectorPlanRolloutValidationRejectsAmbiguousOrUnsafeInput(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	base := CollectorPlanRolloutCreateRequest{
		TenantID: "tenant-a", ActorID: "user-a",
		Selector: CollectorPlanRolloutSelector{ModuleKey: "flow"},
		SpecJSON: []byte(`{"schema_version":1}`), PlanSchemaVersion: 1,
		Strategy:  CollectorPlanRolloutStrategy{CanaryCount: 1, WaveSize: 10},
		ExpiresAt: now.Add(time.Hour),
	}
	tests := []CollectorPlanRolloutCreateRequest{
		func() CollectorPlanRolloutCreateRequest {
			value := base
			value.Selector.CollectorIDs = []string{"collector-a", "collector-a"}
			return value
		}(),
		func() CollectorPlanRolloutCreateRequest {
			value := base
			value.Selector.Status = "deleted"
			return value
		}(),
		func() CollectorPlanRolloutCreateRequest { value := base; value.Strategy.CanaryCount = 0; return value }(),
		func() CollectorPlanRolloutCreateRequest {
			value := base
			value.Strategy.FailureBudget = 11
			return value
		}(),
		func() CollectorPlanRolloutCreateRequest { value := base; value.ExpiresAt = now; return value }(),
		func() CollectorPlanRolloutCreateRequest {
			value := base
			value.ExpiresAt = now.Add(time.Hour).Add(time.Nanosecond)
			return value
		}(),
		func() CollectorPlanRolloutCreateRequest { value := base; value.SpecJSON = []byte(`[]`); return value }(),
	}
	for index, request := range tests {
		if _, err := normalizeCollectorPlanRolloutCreateRequest(request, now); !errors.Is(err, ErrCollectorPlanRolloutInvalid) {
			t.Fatalf("case %d error=%v", index, err)
		}
	}
}

func TestCollectorPlanRolloutPreviewRequiresIdentityAndVersion(t *testing.T) {
	repository := &collectorPlanRolloutRepositoryStub{}
	service, err := NewCollectorPlanRolloutService(repository)
	if err != nil {
		t.Fatal(err)
	}
	request := CollectorPlanRolloutPreviewRequest{
		TenantID: "tenant-a", RolloutID: "rollout-a", ActorID: "user-a", ExpectedRowVersion: 1,
	}
	if _, err := service.PreviewCollectorPlanRollout(context.Background(), request); err != nil || repository.previewCalls != 1 {
		t.Fatalf("preview err=%v calls=%d", err, repository.previewCalls)
	}
	request.ExpectedRowVersion = 0
	if _, err := service.PreviewCollectorPlanRollout(context.Background(), request); !errors.Is(err, ErrCollectorPlanRolloutInvalid) || repository.previewCalls != 1 {
		t.Fatalf("invalid preview err=%v calls=%d", err, repository.previewCalls)
	}
}
