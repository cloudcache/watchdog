package watchdog

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

type fakeAddressDimensionPublisher struct {
	preview  AddressDimensionPreview
	request  AddressDimensionPublishRequest
	tenantID ID
	actorID  ID
	filter   AddressDimensionListFilter
}

func (p *fakeAddressDimensionPublisher) PreviewAddressDimension(context.Context, ID, time.Time) (AddressDimensionPreview, error) {
	return p.preview, nil
}

func (p *fakeAddressDimensionPublisher) PublishAddressDimension(_ context.Context, tenantID, actorID ID, request AddressDimensionPublishRequest) (AddressDimensionSnapshot, error) {
	p.tenantID, p.actorID, p.request = tenantID, actorID, request
	return AddressDimensionSnapshot{ID: "snapshot-a"}, nil
}

func (p *fakeAddressDimensionPublisher) ListAddressDimensionSnapshots(_ context.Context, tenantID ID, filter AddressDimensionListFilter) ([]AddressDimensionSnapshot, string, int, error) {
	p.tenantID, p.filter = tenantID, filter
	return []AddressDimensionSnapshot{{ID: "snapshot-a", TenantID: tenantID, Version: 7, Status: AddressDimensionStatusActive}}, "6", 3, nil
}

func (*fakeAddressDimensionPublisher) GetAddressDimensionSnapshot(context.Context, ID, ID) (AddressDimensionSnapshot, error) {
	return AddressDimensionSnapshot{}, sql.ErrNoRows
}

func TestAddressDimensionPublishJobUsesVersionedPayload(t *testing.T) {
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	payload, err := EncodeAddressDimensionPublishJobPayload(AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakeAddressDimensionPublisher{}
	result, err := NewAddressDimensionPublishJobHandler(publisher)(context.Background(), OperationJob{
		TenantID: "tenant-a", CreatedBy: "user-a", CheckpointJSON: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "dimension-snapshot:snapshot-a" || publisher.tenantID != "tenant-a" || publisher.actorID != "user-a" || publisher.request.PreviewDigest != digest || !publisher.request.EffectiveFrom.Equal(effective) {
		t.Fatalf("unexpected publish call: %q %#v", result, publisher)
	}
}

func TestAddressDimensionPublishJobRejectsInvalidPayload(t *testing.T) {
	_, err := NewAddressDimensionPublishJobHandler(&fakeAddressDimensionPublisher{})(context.Background(), OperationJob{
		TenantID: "tenant-a", CreatedBy: "user-a", CheckpointJSON: []byte(`{"schema_version":99,"payload":{}}`),
	})
	if err == nil || !IsTerminalJobError(err) {
		t.Fatalf("expected terminal payload error, got %v", err)
	}
}
