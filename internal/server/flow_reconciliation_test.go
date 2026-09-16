package server

import (
	"testing"
	"time"
)

func TestFlowReconciliationOperationJobIsStableWithinScheduleBucket(t *testing.T) {
	cfg := defaultConfig()
	cfg.Flow.Reconciliation.Enabled = true
	cfg.Flow.Reconciliation.SourceStreamID = "source-1"
	cfg.Flow.Reconciliation.BootstrapOffsets = map[uint32]uint64{0: 100}
	first, err := flowReconciliationOperationJob(cfg, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	same, err := flowReconciliationOperationJob(cfg, time.Unix(1_800_000_299, 0))
	if err != nil {
		t.Fatal(err)
	}
	next, err := flowReconciliationOperationJob(cfg, time.Unix(1_800_000_300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if first.IdempotencyKey != same.IdempotencyKey || first.RequestHash != same.RequestHash {
		t.Fatalf("same bucket produced different jobs: %+v %+v", first, same)
	}
	if next.IdempotencyKey == first.IdempotencyKey || next.RequestHash != first.RequestHash {
		t.Fatalf("next bucket did not rotate only the idempotency key: %+v %+v", first, next)
	}
}

func TestFlowReconciliationOperationJobChangesWithFrozenRequest(t *testing.T) {
	cfg := defaultConfig()
	cfg.Flow.Reconciliation.Enabled = true
	cfg.Flow.Reconciliation.SourceStreamID = "source-1"
	cfg.Flow.Reconciliation.BootstrapOffsets = map[uint32]uint64{0: 100}
	at := time.Unix(1_800_000_000, 0)
	first, err := flowReconciliationOperationJob(cfg, at)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Flow.Reconciliation.MaxBatches++
	changed, err := flowReconciliationOperationJob(cfg, at)
	if err != nil {
		t.Fatal(err)
	}
	if changed.RequestHash == first.RequestHash || changed.IdempotencyKey == first.IdempotencyKey {
		t.Fatalf("changed request reused identity: %+v %+v", first, changed)
	}
}
