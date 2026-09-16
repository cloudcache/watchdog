// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowlifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

type rawDeleteStoreFake struct {
	execution      RawDeleteExecution
	readiness      RawDeleteReadiness
	loadErr        error
	readinessErr   error
	completeErr    error
	readinessCalls int
	completeCalls  int
	barrierErr     error
	reclassErr     error
	reclassCalls   int
}

func (store *rawDeleteStoreFake) RawDeleteBarrierReadyForDay(context.Context, time.Time) (DeleteBarrierStatus, error) {
	return DeleteBarrierStatus{Ready: store.barrierErr == nil}, store.barrierErr
}

func (store *rawDeleteStoreFake) EnsureNoActiveReclassificationForDay(context.Context, time.Time) error {
	store.reclassCalls++
	return store.reclassErr
}

func (store *rawDeleteStoreFake) LoadRawDeleteExecution(context.Context, string) (RawDeleteExecution, error) {
	return store.execution, store.loadErr
}

func (store *rawDeleteStoreFake) RawDayDeleteReadiness(context.Context, time.Time, time.Time, RawDayEvidenceReader) (RawDeleteReadiness, error) {
	store.readinessCalls++
	return store.readiness, store.readinessErr
}

func (store *rawDeleteStoreFake) CompleteRawDayDelete(context.Context, string, time.Time) error {
	store.completeCalls++
	if store.completeErr == nil {
		store.execution.Receipt.Status = "succeeded"
		store.execution.State.State = PartitionRawDeleted
	}
	return store.completeErr
}

type rawDeleteRunnerFake struct {
	beforeRaw      flowch.StorageCounters
	beforeArchive  flowch.StorageCounters
	afterRaw       flowch.StorageCounters
	afterArchive   flowch.StorageCounters
	beforePhysical uint64
	afterPhysical  uint64
	dropErr        error
	counterCalls   int
	physicalCalls  int
	dropCalls      int
	queryID        string
}

func (runner *rawDeleteRunnerFake) DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error) {
	runner.counterCalls++
	if runner.counterCalls == 1 {
		return runner.beforeRaw, runner.beforeArchive, nil
	}
	return runner.afterRaw, runner.afterArchive, nil
}

func (runner *rawDeleteRunnerFake) RawDayPhysicalRecords(context.Context, time.Time) (uint64, error) {
	runner.physicalCalls++
	if runner.physicalCalls == 1 {
		return runner.beforePhysical, nil
	}
	return runner.afterPhysical, nil
}

func (*rawDeleteRunnerFake) DayOffsetCoverage(context.Context, time.Time) ([]flowch.DayOffsetCoverage, error) {
	return nil, nil
}

func (runner *rawDeleteRunnerFake) DropRawDay(_ context.Context, _ time.Time, queryID string) error {
	runner.dropCalls++
	runner.queryID = queryID
	return runner.dropErr
}

func rawDeleteFixture(t *testing.T) (opjob.Job, RawDeleteExecution, RawDeleteReadiness, flowch.StorageCounters) {
	t.Helper()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	counters := Counters{RecordCount: 3, RawBytes: 300, RawPackets: 30, EstimatedBytes: 3000, EstimatedPackets: 300, EstimatedValidRecords: 3}
	coverage := []OffsetCoverage{{
		SourceStreamID: "site-a:boot-1", KafkaTopic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker", KafkaPartition: 1,
		BootstrapOffset: 0, FirstOffset: 10, LastOffsetExclusive: 14, CommittedNextOffset: 20, ReconciledNextOffset: 20,
		CommittedSnapshotAt: day.Add(72 * time.Hour), VerifiedAt: day.Add(73 * time.Hour),
	}}
	approval := DeletionApproval{
		ID: "approval-a", StorageKind: "raw", PartitionGranularity: "day", PartitionStart: day, PartitionEnd: day.Add(24 * time.Hour),
		PolicyID: "policy-a", PolicyVersion: 2, Generation: uint64(2)<<32 | 1, BackupEvidenceID: "backup-a",
		KafkaCoverage: coverage, Source: counters, PhysicalRecords: 4, Archive: counters, Status: "approved",
	}
	job, err := NewRawDeleteOperationJob(approval, "admin-a")
	if err != nil {
		t.Fatal(err)
	}
	job.ID = "job-a"
	job.AttemptCount = 1
	execution := RawDeleteExecution{
		Approval: approval,
		Receipt: DeletionReceipt{
			ID: "receipt-a", StorageKind: "raw", PartitionGranularity: "day", PartitionStart: day, PartitionEnd: day.Add(24 * time.Hour),
			PolicyID: approval.PolicyID, PolicyVersion: approval.PolicyVersion, Generation: approval.Generation,
			OperationJobID: job.ID, DeletionApprovalID: approval.ID, BackupEvidenceID: approval.BackupEvidenceID,
			KafkaCoverage: coverage, Source: counters, PhysicalRecords: approval.PhysicalRecords,
			ClickHouseQueryID: "flow-raw-delete-job-a", Status: "requested",
		},
		State: PartitionState{
			SourceDate: day, PolicyID: approval.PolicyID, PolicyVersion: approval.PolicyVersion,
			State: PartitionDeleteEligible, Generation: approval.Generation, DeleteApprovalID: approval.ID, DeleteJobID: job.ID,
			Source: counters, Archive: counters,
		},
		Policy: Policy{ID: approval.PolicyID, Version: approval.PolicyVersion, Status: PolicyPublished, RawDeleteEnabled: true},
	}
	readiness := RawDeleteReadiness{
		SourceDate: day, PolicyID: approval.PolicyID, PolicyVersion: approval.PolicyVersion, PartitionState: PartitionDeleteEligible,
		DeleteApprovalID: approval.ID, Generation: approval.Generation, EvidenceReady: true, DeletionReady: true,
		Source: counters, Archive: counters, PhysicalRecords: approval.PhysicalRecords,
		Coverage: coverage, BackupEvidenceID: approval.BackupEvidenceID,
	}
	return job, execution, readiness, flowch.StorageCounters{
		RecordCount: counters.RecordCount, RawBytes: counters.RawBytes, RawPackets: counters.RawPackets,
		EstimatedBytes: counters.EstimatedBytes, EstimatedPackets: counters.EstimatedPackets,
		EstimatedValidRecords: counters.EstimatedValidRecords,
	}
}

func TestRawDeleteHandlerCompletesOnlyAfterLiveRevalidationAndPostCheck(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{
		beforeRaw: counters, beforeArchive: counters, afterArchive: counters,
		beforePhysical: execution.Approval.PhysicalRecords,
	}
	result, err := NewRawDeleteHandler(store, runner)(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if result != "clickhouse:flow:raw-deleted:2026-09-01:g8589934593" || runner.dropCalls != 1 ||
		runner.queryID != execution.Receipt.ClickHouseQueryID || store.readinessCalls != 1 || store.completeCalls != 1 {
		t.Fatalf("result=%q runner=%+v store=%+v", result, runner, store)
	}
}

func TestRawDeleteHandlerRejectsChangedFrozenEvidenceBeforeDDL(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{beforeRaw: counters, beforeArchive: counters, beforePhysical: execution.Approval.PhysicalRecords + 1}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) || !errors.Is(err, ErrDeleteLocked) {
		t.Fatalf("changed evidence error=%v", err)
	}
	if runner.dropCalls != 0 || store.completeCalls != 0 {
		t.Fatalf("changed evidence reached deletion: runner=%+v store=%+v", runner, store)
	}
}

func TestRawDeleteHandlerRejectsActiveHistoricalReclassification(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness, reclassErr: ErrDeleteLocked}
	runner := &rawDeleteRunnerFake{beforeRaw: counters, beforeArchive: counters, beforePhysical: execution.Approval.PhysicalRecords}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); !errors.Is(err, ErrDeleteLocked) {
		t.Fatalf("active reclassification error=%v", err)
	}
	if store.reclassCalls != 1 || runner.counterCalls != 0 || runner.dropCalls != 0 {
		t.Fatalf("active reclassification reached ClickHouse: store=%+v runner=%+v", store, runner)
	}
}

func TestRawDeleteHandlerRecoversAmbiguousAcknowledgementOnTakeover(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	job.AttemptCount = 2
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{beforeArchive: counters, afterArchive: counters}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if runner.dropCalls != 0 || store.readinessCalls != 0 || store.completeCalls != 1 {
		t.Fatalf("takeover did not reconcile already-dropped partition: runner=%+v store=%+v", runner, store)
	}
}

func TestRawDeleteHandlerDoesNotAcceptMissingPartitionOnFirstAttempt(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{beforeArchive: counters, afterArchive: counters}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) || !errors.Is(err, ErrDeleteLocked) {
		t.Fatalf("first-attempt missing partition error=%v", err)
	}
	if runner.dropCalls != 0 || store.completeCalls != 0 {
		t.Fatalf("first-attempt missing partition was accepted: runner=%+v store=%+v", runner, store)
	}
}

func TestRawDeleteHandlerTreatsDroppedPartitionAsSuccessAfterLostAck(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{
		beforeRaw: counters, beforeArchive: counters, afterArchive: counters, beforePhysical: execution.Approval.PhysicalRecords,
		dropErr: errors.New("connection closed before acknowledgement"),
	}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if runner.dropCalls != 1 || store.completeCalls != 1 {
		t.Fatalf("ambiguous acknowledgement did not converge: runner=%+v store=%+v", runner, store)
	}
}

func TestRawDeleteHandlerClassifiesPermanentClickHouseFailure(t *testing.T) {
	job, execution, readiness, counters := rawDeleteFixture(t)
	store := &rawDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &rawDeleteRunnerFake{
		beforeRaw: counters, beforeArchive: counters, afterRaw: counters, afterArchive: counters,
		beforePhysical: execution.Approval.PhysicalRecords, afterPhysical: execution.Approval.PhysicalRecords,
		dropErr: flowch.Permanent(errors.New("unknown table")),
	}
	if _, err := NewRawDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) {
		t.Fatalf("permanent ClickHouse failure was retryable: %v", err)
	}
	if store.completeCalls != 0 {
		t.Fatal("permanent failed deletion was completed")
	}
}
