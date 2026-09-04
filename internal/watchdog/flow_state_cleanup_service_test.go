package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

type flowStateCleanupServiceRepository struct {
	authority FlowStateCleanupAuthority
	existing  FlowStateCleanupJob
	hasJob    bool
	created   FlowStateCleanupJob
	retried   FlowStateCleanupJob
}

func (r *flowStateCleanupServiceRepository) CreateFlowStateCleanupJob(_ context.Context, job FlowStateCleanupJob) (FlowStateCleanupJob, error) {
	job.Status = OperationJobQueued
	job.RowVersion = 1
	job.CreatedAt = job.Snapshot.CreatedAt
	job.UpdatedAt = job.Snapshot.UpdatedAt
	r.created = job
	r.existing = job
	r.hasJob = true
	return job, nil
}

func (r *flowStateCleanupServiceRepository) GetFlowStateCleanupJob(_ context.Context, tenantID, jobID ID) (FlowStateCleanupJob, error) {
	if !r.hasJob || r.existing.TenantID != tenantID || r.existing.ID != jobID {
		return FlowStateCleanupJob{}, errors.New("not found")
	}
	return r.existing, nil
}

func (r *flowStateCleanupServiceRepository) GetFlowStateCleanupJobByIdempotency(_ context.Context, tenantID ID, key string) (FlowStateCleanupJob, bool, error) {
	if r.hasJob && r.existing.TenantID == tenantID && r.existing.IdempotencyKey == key {
		return r.existing, true, nil
	}
	return FlowStateCleanupJob{}, false, nil
}

func (r *flowStateCleanupServiceRepository) GetFlowStateCleanupAuthority(_ context.Context, tenantID, transferID ID) (FlowStateCleanupAuthority, error) {
	if r.authority.TenantID != tenantID || r.authority.TransferID != transferID {
		return FlowStateCleanupAuthority{}, errors.New("not found")
	}
	return r.authority, nil
}

func (r *flowStateCleanupServiceRepository) RetryFlowStateCleanupJob(_ context.Context, _, _ ID, _ uint64, _ ID) (FlowStateCleanupJob, error) {
	return r.retried, nil
}

type flowStateCleanupServiceScanner struct {
	snapshot flowcollect.KafkaStateKeySnapshot
	key      []byte
	calls    int
	err      error
	entered  chan struct{}
}

func (s *flowStateCleanupServiceScanner) Scan(ctx context.Context, key []byte) (flowcollect.KafkaStateKeySnapshot, error) {
	s.calls++
	s.key = append([]byte(nil), key...)
	if s.entered != nil {
		close(s.entered)
		<-ctx.Done()
		return flowcollect.KafkaStateKeySnapshot{}, ctx.Err()
	}
	return s.snapshot, s.err
}

func TestFlowStateCleanupJobServiceCreatesFromFrozenKafkaCheckpoint(t *testing.T) {
	base := time.Unix(1_000_000, 0).UTC()
	state, payload := flowStateCleanupServiceCollectState(t, base)
	repository := &flowStateCleanupServiceRepository{authority: flowStateCleanupServiceAuthority()}
	scanner := &flowStateCleanupServiceScanner{snapshot: flowcollect.KafkaStateKeySnapshot{
		CapturedAt: base.Add(time.Second), Present: true, Value: payload,
		LastRecord: &flowcollect.KafkaRecordPosition{Partition: 0, Offset: 7},
		Boundaries: []flowcollect.KafkaStatePartitionBoundary{{Partition: 0, OldestOffset: 0, HighWatermark: 8}},
	}}
	service, err := NewFlowStateCleanupJobService(repository, scanner)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return base.Add(2 * time.Second) }
	service.newID = func() (ID, error) { return "flowclean_0000000000000001", nil }
	request := FlowStateCleanupCreateRequest{
		TransferID: "transfer_cleanup_0000001", Kind: flowcollect.StateCheckpointDecoder,
		IdentityKey: state.StateIdentityKey, IdempotencyKey: "cleanup-request-1",
	}
	job, err := service.Create(context.Background(), "tenant_cleanup_000000001", "user_cleanup_00000000001", request)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "flowclean_0000000000000001" || job.Snapshot.ApprovalID != "approval_cleanup_00000001" || job.Snapshot.Old.StateGeneration != 9 || job.Snapshot.Old.RegistryVersion != 20 || scanner.calls != 1 {
		t.Fatalf("job=%+v scanner calls=%d", job, scanner.calls)
	}
	if string(scanner.key) != string(state.StateKey) || job.RequestHash == "" {
		t.Fatalf("scanner key=%x state key=%x request hash=%q", scanner.key, state.StateKey, job.RequestHash)
	}

	replayed, err := service.Create(context.Background(), "tenant_cleanup_000000001", "user_cleanup_00000000001", request)
	if err != nil || replayed.ID != job.ID || scanner.calls != 1 {
		t.Fatalf("idempotent replay=%+v err=%v scanner calls=%d", replayed, err, scanner.calls)
	}
	request.IdentityKey = append([]byte(nil), request.IdentityKey...)
	request.IdentityKey[0] ^= 0xff
	if _, err := service.Create(context.Background(), "tenant_cleanup_000000001", "user_cleanup_00000000001", request); !errors.Is(err, ErrFlowStateCleanupIdempotencyConflict) {
		t.Fatalf("idempotency conflict error=%v", err)
	}
}

func TestFlowStateCleanupJobServiceRejectsMissingOrMismatchedCheckpoint(t *testing.T) {
	base := time.Unix(1_000_000, 0).UTC()
	state, payload := flowStateCleanupServiceCollectState(t, base)
	request := FlowStateCleanupCreateRequest{
		TransferID: "transfer_cleanup_0000001", Kind: flowcollect.StateCheckpointDecoder,
		IdentityKey: state.StateIdentityKey, IdempotencyKey: "cleanup-request-1",
	}
	for _, test := range []struct {
		name     string
		snapshot flowcollect.KafkaStateKeySnapshot
		want     error
	}{
		{name: "missing", snapshot: flowcollect.KafkaStateKeySnapshot{CapturedAt: base}, want: ErrFlowStateCleanupCheckpointMissing},
		{name: "outside frozen boundary", snapshot: flowcollect.KafkaStateKeySnapshot{
			CapturedAt: base, Present: true, Value: payload,
			LastRecord: &flowcollect.KafkaRecordPosition{Partition: 0, Offset: 7},
			Boundaries: []flowcollect.KafkaStatePartitionBoundary{{Partition: 0, HighWatermark: 7}},
		}, want: ErrFlowStateCleanupCheckpointInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := &flowStateCleanupServiceRepository{authority: flowStateCleanupServiceAuthority()}
			service, err := NewFlowStateCleanupJobService(repository, &flowStateCleanupServiceScanner{snapshot: test.snapshot})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Create(context.Background(), repository.authority.TenantID, "user_cleanup_00000000001", request); !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}

	state.ExporterId = "different-exporter"
	digest := flowStateCleanupServiceProtoDigest(t, state)
	state.PayloadSha256 = digest[:]
	mismatched, err := proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	repository := &flowStateCleanupServiceRepository{authority: flowStateCleanupServiceAuthority()}
	service, err := NewFlowStateCleanupJobService(repository, &flowStateCleanupServiceScanner{snapshot: flowcollect.KafkaStateKeySnapshot{
		CapturedAt: base.Add(time.Second), Present: true, Value: mismatched,
		LastRecord: &flowcollect.KafkaRecordPosition{Partition: 0, Offset: 7},
		Boundaries: []flowcollect.KafkaStatePartitionBoundary{{Partition: 0, HighWatermark: 8}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Create(context.Background(), repository.authority.TenantID, "user_cleanup_00000000001", request); !errors.Is(err, ErrFlowStateCleanupCheckpointInvalid) {
		t.Fatalf("mismatched checkpoint error=%v", err)
	}
}

func TestFlowStateCleanupJobServiceCloseCancelsAndDrainsRequests(t *testing.T) {
	repository := &flowStateCleanupServiceRepository{authority: flowStateCleanupServiceAuthority()}
	scanner := &flowStateCleanupServiceScanner{entered: make(chan struct{})}
	service, err := NewFlowStateCleanupJobService(repository, scanner)
	if err != nil {
		t.Fatal(err)
	}
	request := FlowStateCleanupCreateRequest{
		TransferID: "transfer_cleanup_0000001", Kind: flowcollect.StateCheckpointDecoder,
		IdentityKey: make([]byte, sha256.Size), IdempotencyKey: "cleanup-request-1",
	}
	createResult := make(chan error, 1)
	go func() {
		_, createErr := service.Create(context.Background(), repository.authority.TenantID, "user_cleanup_00000000001", request)
		createResult <- createErr
	}()
	select {
	case <-scanner.entered:
	case <-time.After(time.Second):
		t.Fatal("scanner did not start")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-createResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("create error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("create did not drain")
	}
	if _, err := service.Get(context.Background(), repository.authority.TenantID, "flowclean_0000000000000001"); err == nil {
		t.Fatal("request after close succeeded")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func flowStateCleanupServiceAuthority() FlowStateCleanupAuthority {
	return FlowStateCleanupAuthority{
		TransferID: "transfer_cleanup_0000001", TenantID: "tenant_cleanup_000000001",
		ExporterID: "exporter-a", OldCollectorID: "collector-a",
		OldPlanRevision: 20, OldOwnershipEpoch: 5, ApprovalID: "approval_cleanup_00000001",
	}
}

func flowStateCleanupServiceCollectState(t *testing.T, at time.Time) (*flowpb.CollectState, []byte) {
	t.Helper()
	source := netip.MustParseAddr("192.0.2.1").As16()
	domain := uint64(42)
	identityHash := sha256.New()
	flowStateCleanupServiceWriteHashField(identityHash, []byte("watchdog.flow.collect-state.identity.v2"))
	flowStateCleanupServiceWriteHashField(identityHash, []byte("tenant_cleanup_000000001"))
	flowStateCleanupServiceWriteHashField(identityHash, []byte("exporter-a"))
	flowStateCleanupServiceWriteHashField(identityHash, []byte{byte(flowcollect.ProtocolNetFlow9)})
	flowStateCleanupServiceWriteHashField(identityHash, source[:])
	var domainBytes [8]byte
	binary.BigEndian.PutUint64(domainBytes[:], domain)
	flowStateCleanupServiceWriteHashField(identityHash, domainBytes[:])
	identity := identityHash.Sum(nil)
	stateKey, err := flowcollect.StateCleanupKafkaKey(flowcollect.StateCheckpointDecoder, identity, 5)
	if err != nil {
		t.Fatal(err)
	}
	datagramID := sha256.Sum256([]byte("cleanup-service-datagram"))
	stateID := sha256.Sum256(append(append([]byte(nil), datagramID[:]...), stateKey...))
	state := &flowpb.CollectState{
		StateSchemaVersion: 2, StateId: stateID[:], StateKey: stateKey,
		DatagramId: datagramID[:], TenantId: "tenant_cleanup_000000001",
		CollectorId: "collector-a", ExporterId: "exporter-a", RegistryVersion: 20,
		ReceivedAtUnixMs: at.UnixMilli(), Protocol: uint32(flowcollect.ProtocolNetFlow9),
		SourceIp: source[:], ObservationDomainId: domain, StateIdentityKey: identity,
		OwnershipEpoch: 5, StateGeneration: 9,
	}
	digest := flowStateCleanupServiceProtoDigest(t, state)
	state.PayloadSha256 = digest[:]
	payload, err := proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return state, payload
}

func flowStateCleanupServiceProtoDigest(t *testing.T, state *flowpb.CollectState) [sha256.Size]byte {
	t.Helper()
	cloned := proto.Clone(state).(*flowpb.CollectState)
	cloned.PayloadSha256 = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(payload)
}

func flowStateCleanupServiceWriteHashField(writer io.Writer, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}
