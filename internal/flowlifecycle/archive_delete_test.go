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

type archiveDeleteStoreFake struct {
	execution      ArchiveDeleteExecution
	readiness      ArchiveMonthDeleteReadiness
	loadErr        error
	readinessErr   error
	completeErr    error
	readinessCalls int
	completeCalls  int
}

func (store *archiveDeleteStoreFake) LoadArchiveDeleteExecution(context.Context, string) (ArchiveDeleteExecution, error) {
	return store.execution, store.loadErr
}

func (store *archiveDeleteStoreFake) ArchiveMonthDeleteReadiness(context.Context, time.Time, time.Time, ArchiveMonthEvidenceReader) (ArchiveMonthDeleteReadiness, error) {
	store.readinessCalls++
	return store.readiness, store.readinessErr
}

func (store *archiveDeleteStoreFake) CompleteArchiveMonthDelete(context.Context, string, time.Time) error {
	store.completeCalls++
	if store.completeErr == nil {
		store.execution.Receipt.Status = "succeeded"
	}
	return store.completeErr
}

type archiveDeleteRunnerFake struct {
	before        flowch.StorageCounters
	after         flowch.StorageCounters
	beforeRows    uint64
	afterRows     uint64
	dropErr       error
	counterCalls  int
	physicalCalls int
	dropCalls     int
	queryID       string
}

func (runner *archiveDeleteRunnerFake) ArchiveMonthStorageCounters(context.Context, time.Time) (flowch.StorageCounters, error) {
	runner.counterCalls++
	if runner.counterCalls == 1 {
		return runner.before, nil
	}
	return runner.after, nil
}

func (runner *archiveDeleteRunnerFake) ArchiveMonthPhysicalRecords(context.Context, time.Time) (uint64, error) {
	runner.physicalCalls++
	if runner.physicalCalls == 1 {
		return runner.beforeRows, nil
	}
	return runner.afterRows, nil
}

func (runner *archiveDeleteRunnerFake) DropArchiveMonth(_ context.Context, _ time.Time, queryID string) error {
	runner.dropCalls++
	runner.queryID = queryID
	return runner.dropErr
}

func archiveDeleteFixture(t *testing.T) (opjob.Job, ArchiveDeleteExecution, ArchiveMonthDeleteReadiness, flowch.StorageCounters) {
	t.Helper()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	counters := Counters{RecordCount: 31, RawBytes: 3_100, RawPackets: 310, EstimatedBytes: 31_000, EstimatedPackets: 3_100, EstimatedValidRecords: 30}
	approval := DeletionApproval{
		ID: "approval-archive-a", StorageKind: "archive", PartitionGranularity: "month",
		PartitionStart: month, PartitionEnd: month.AddDate(0, 1, 0), PolicyID: "policy-a", PolicyVersion: 3,
		Generation: uint64(3)<<32 | 1, BackupEvidenceID: "backup-a", KafkaCoverage: []OffsetCoverage{},
		Source: counters, Archive: counters, PhysicalRecords: 744, Status: "approved",
	}
	job, err := NewArchiveDeleteOperationJob(approval, "admin-a")
	if err != nil {
		t.Fatal(err)
	}
	job.ID = "job-archive-a"
	job.AttemptCount = 1
	execution := ArchiveDeleteExecution{
		Approval: approval,
		Receipt: DeletionReceipt{
			ID: "receipt-archive-a", StorageKind: "archive", PartitionGranularity: "month",
			PartitionStart: month, PartitionEnd: month.AddDate(0, 1, 0), PolicyID: approval.PolicyID,
			PolicyVersion: approval.PolicyVersion, Generation: approval.Generation, OperationJobID: job.ID,
			DeletionApprovalID: approval.ID, BackupEvidenceID: approval.BackupEvidenceID, KafkaCoverage: []OffsetCoverage{},
			Source: counters, PhysicalRecords: approval.PhysicalRecords, ClickHouseQueryID: "flow-archive-delete-job-a", Status: "requested",
		},
		Policy: Policy{
			ID: approval.PolicyID, Version: approval.PolicyVersion, Status: PolicyPublished,
			RawRetentionSeconds: 86400, ArchiveRetentionSeconds: 2 * 86400, ArchiveResolutionSeconds: 3600,
			DeleteGraceSeconds: 3600, MaxPartitionsPerRun: 7, BootstrapFrom: month.AddDate(0, -1, 0), ArchiveDeleteEnabled: true,
		},
	}
	readiness := ArchiveMonthDeleteReadiness{
		MonthStart: month, MonthEnd: month.AddDate(0, 1, 0), PolicyID: approval.PolicyID, PolicyVersion: approval.PolicyVersion,
		Generation: approval.Generation, DeleteApprovalID: approval.ID, EvidenceReady: true, DeletionReady: true,
		Source: counters, Archive: counters, PhysicalRecords: approval.PhysicalRecords, BackupEvidenceID: approval.BackupEvidenceID,
	}
	return job, execution, readiness, flowch.StorageCounters{
		RecordCount: counters.RecordCount, RawBytes: counters.RawBytes, RawPackets: counters.RawPackets,
		EstimatedBytes: counters.EstimatedBytes, EstimatedPackets: counters.EstimatedPackets,
		EstimatedValidRecords: counters.EstimatedValidRecords,
	}
}

func TestArchiveDeleteHandlerCompletesAfterLiveEvidenceAndPostCheck(t *testing.T) {
	job, execution, readiness, counters := archiveDeleteFixture(t)
	store := &archiveDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &archiveDeleteRunnerFake{before: counters, beforeRows: execution.Approval.PhysicalRecords}
	result, err := NewArchiveDeleteHandler(store, runner)(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if result != "clickhouse:flow:archive-deleted:2026-08:g12884901889" || runner.dropCalls != 1 ||
		runner.queryID != execution.Receipt.ClickHouseQueryID || store.readinessCalls != 1 || store.completeCalls != 1 {
		t.Fatalf("result=%q runner=%+v store=%+v", result, runner, store)
	}
}

func TestArchiveDeleteHandlerRejectsChangedFrozenEvidence(t *testing.T) {
	job, execution, readiness, counters := archiveDeleteFixture(t)
	store := &archiveDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &archiveDeleteRunnerFake{before: counters, beforeRows: execution.Approval.PhysicalRecords + 1}
	if _, err := NewArchiveDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) || !errors.Is(err, ErrDeleteLocked) {
		t.Fatalf("changed evidence error=%v", err)
	}
	if runner.dropCalls != 0 || store.completeCalls != 0 {
		t.Fatalf("changed evidence reached deletion: runner=%+v store=%+v", runner, store)
	}
}

func TestArchiveDeleteHandlerRecoversAmbiguousAcknowledgement(t *testing.T) {
	job, execution, readiness, _ := archiveDeleteFixture(t)
	job.AttemptCount = 2
	store := &archiveDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &archiveDeleteRunnerFake{}
	if _, err := NewArchiveDeleteHandler(store, runner)(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if runner.dropCalls != 0 || store.readinessCalls != 0 || store.completeCalls != 1 {
		t.Fatalf("takeover did not converge: runner=%+v store=%+v", runner, store)
	}
}

func TestArchiveDeleteHandlerRejectsMissingPartitionOnFirstAttempt(t *testing.T) {
	job, execution, readiness, _ := archiveDeleteFixture(t)
	store := &archiveDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &archiveDeleteRunnerFake{}
	if _, err := NewArchiveDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) || !errors.Is(err, ErrDeleteLocked) {
		t.Fatalf("first-attempt missing partition error=%v", err)
	}
	if runner.dropCalls != 0 || store.completeCalls != 0 {
		t.Fatalf("first-attempt missing partition was accepted: runner=%+v store=%+v", runner, store)
	}
}

func TestArchiveDeleteHandlerClassifiesPermanentClickHouseFailure(t *testing.T) {
	job, execution, readiness, counters := archiveDeleteFixture(t)
	store := &archiveDeleteStoreFake{execution: execution, readiness: readiness}
	runner := &archiveDeleteRunnerFake{
		before: counters, after: counters, beforeRows: execution.Approval.PhysicalRecords, afterRows: execution.Approval.PhysicalRecords,
		dropErr: flowch.Permanent(errors.New("unknown table")),
	}
	if _, err := NewArchiveDeleteHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) {
		t.Fatalf("permanent ClickHouse failure was retryable: %v", err)
	}
	if store.completeCalls != 0 {
		t.Fatal("permanent failed deletion was completed")
	}
}
