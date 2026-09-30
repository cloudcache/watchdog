package server

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/opjob"
)

func TestFlowLifecycleConfigAutoDeletesRawDayIntegration(t *testing.T) {
	db, err := sql.Open("mysql", isolatedMySQLDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := day.Add(5 * 24 * time.Hour)
	s := &Server{db: db, cfg: Config{Flow: FlowConfig{Lifecycle: FlowLifecycleConfig{
		Enabled: true, BootstrapFrom: "2026-01-01", RawRetention: 24 * time.Hour, LateArrival: 6 * time.Hour,
		DeleteGrace: time.Hour, MaxPartitionsPerRun: 1, RawDelete: true, AutoDelete: true,
	}}}}
	store := flowlifecycle.NewStore(db)

	actor, err := s.syncFlowLifecycleConfig(ctx, store, now)
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var password sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status,password_hash FROM users WHERE id=? AND username=?`, actor, flowLifecycleActorUsername).Scan(&status, &password); err != nil {
		t.Fatal(err)
	}
	if status != "disabled" || password.Valid {
		t.Fatalf("system actor can sign in: status=%q password=%v", status, password.Valid)
	}
	policy, err := store.GetPublishedPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !policy.AutoDelete || !policy.RawDeleteEnabled || !policy.WaiveKafkaCoverage || policy.RequireBackupBeforeDelete ||
		policy.RawRetentionSeconds != 86400 || policy.CreatedBy != actor || policy.PublishedBy != actor {
		t.Fatalf("published policy does not reflect flow.lifecycle: %+v", policy)
	}
	if again, err := s.syncFlowLifecycleConfig(ctx, store, now); err != nil || again != actor {
		t.Fatalf("resync actor=%q err=%v", again, err)
	}
	if unchanged, err := store.GetPublishedPolicy(ctx); err != nil || unchanged.Version != policy.Version {
		t.Fatalf("unchanged config published a new revision: %+v err=%v", unchanged, err)
	}

	generation, err := flowlifecycle.Generation(policy.Version, 1)
	if err != nil {
		t.Fatal(err)
	}
	counters := flowch.StorageCounters{RecordCount: 7, RawBytes: 100, RawPackets: 10, EstimatedBytes: 900, EstimatedPackets: 90, EstimatedValidRecords: 6}
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_partition_states
		(source_date,policy_id,policy_version,state,generation,repair_attempt,
		 source_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
		 archived_at,reconciled_at,late_checked_at,delete_eligible_at)
		VALUES (?,?,?,'reconciled',?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, day, policy.ID, policy.Version, generation,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		day.Add(48*time.Hour), day.Add(48*time.Hour), day.Add(48*time.Hour), day.Add(49*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// No Kafka coverage and no backup evidence: both gates are off in config.
	reader := &testRawDeleteEvidence{counters: counters, physical: 9}
	deleter := &flowlifecycle.AutoDeleter{Store: store, Evidence: reader, Actor: actor, Interval: time.Minute, MaxDays: 7}
	approved, scheduled, err := deleter.ScanOnce(ctx, now)
	if err != nil || approved != 1 || scheduled != 1 {
		t.Fatalf("auto delete approved=%d scheduled=%d err=%v", approved, scheduled, err)
	}
	partition, err := store.GetPartition(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	var approvedBy, requestedBy string
	if err := db.QueryRowContext(ctx, `SELECT a.approved_by,r.requested_by FROM flow_deletion_approvals a
		JOIN flow_deletion_receipts r ON r.deletion_approval_id=a.id WHERE a.id=?`, partition.DeleteApprovalID).Scan(&approvedBy, &requestedBy); err != nil {
		t.Fatal(err)
	}
	if partition.DeleteJobID == "" || approvedBy != actor || requestedBy != actor {
		t.Fatalf("deletion not attributed to the system actor: partition=%+v approved_by=%q requested_by=%q", partition, approvedBy, requestedBy)
	}
	if again, scheduledAgain, err := deleter.ScanOnce(ctx, now); err != nil || again != 0 || scheduledAgain != 0 {
		t.Fatalf("scheduled day was advanced twice: approved=%d scheduled=%d err=%v", again, scheduledAgain, err)
	}

	jobs := opjob.NewStore(db)
	workerContext, stopWorker := context.WithCancel(ctx)
	worker := &opjob.Worker{
		Repo: jobs, JobType: flowlifecycle.RawDeleteJobType, Owner: "auto-delete-integration",
		Handler: flowlifecycle.NewRawDeleteHandler(store, reader), PollInterval: time.Millisecond,
		LeaseFor: time.Second, RetryBase: time.Millisecond,
	}
	go worker.Run(workerContext)
	deadline := time.Now().Add(5 * time.Second)
	var finished opjob.Job
	for time.Now().Before(deadline) {
		if finished, err = jobs.Get(ctx, partition.DeleteJobID); err != nil {
			t.Fatal(err)
		}
		if finished.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopWorker()
	if partition, err = store.GetPartition(ctx, day); err != nil {
		t.Fatal(err)
	}
	if finished.Status != opjob.StatusSucceeded || partition.State != flowlifecycle.PartitionRawDeleted {
		t.Fatalf("auto-scheduled deletion did not converge: job=%+v partition=%+v", finished, partition)
	}

	// A changed flow.lifecycle publishes the next revision and retires this one;
	// a sub-day retention must pass the MySQL CHECK as well as NormalizePolicy.
	s.cfg.Flow.Lifecycle.RawRetention = 6 * time.Hour
	if _, err := s.syncFlowLifecycleConfig(ctx, store, now); err != nil {
		t.Fatal(err)
	}
	next, err := store.GetPublishedPolicy(ctx)
	if err != nil || next.Version != policy.Version+1 || next.RawRetentionSeconds != 6*3600 {
		t.Fatalf("changed config was not published: %+v err=%v", next, err)
	}
	if retired, err := store.GetPolicy(ctx, policy.ID); err != nil || retired.Status != flowlifecycle.PolicyRetired {
		t.Fatalf("previous revision status=%q err=%v", retired.Status, err)
	}
}

func TestFlowLifecycleHoldAndSupersedeIntegration(t *testing.T) {
	db, err := sql.Open("mysql", isolatedMySQLDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	held, settled := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	now := held.Add(5 * 24 * time.Hour)
	s := &Server{db: db, cfg: Config{Flow: FlowConfig{Lifecycle: FlowLifecycleConfig{
		Enabled: true, BootstrapFrom: "2026-01-01", RawRetention: 24 * time.Hour, LateArrival: 6 * time.Hour,
		DeleteGrace: time.Hour, MaxPartitionsPerRun: 1,
	}}}}
	store := flowlifecycle.NewStore(db)
	jobs := opjob.NewStore(db)
	if _, err := s.syncFlowLifecycleConfig(ctx, store, now); err != nil {
		t.Fatal(err)
	}
	first, err := store.GetPublishedPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	begin := func(policy flowlifecycle.Policy, day time.Time) (uint64, string) {
		t.Helper()
		job, err := flowlifecycle.NewArchiveOperationJob(policy, day, 1)
		if err != nil {
			t.Fatal(err)
		}
		queued, err := jobs.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		generation, _ := flowlifecycle.Generation(policy.Version, 1)
		if _, err := store.BeginArchive(ctx, policy, day, generation, queued.ID); err != nil {
			t.Fatal(err)
		}
		return generation, queued.ID
	}

	// Raw lost after the hot rollup published more: the day is held, and the
	// repair scan never re-archives it from the incomplete raw.
	generation, jobID := begin(first, held)
	if err := store.HoldArchive(ctx, first, held, generation, jobID,
		flowlifecycle.Counters{RecordCount: 2}, flowlifecycle.Counters{RecordCount: 9}, now); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetPartition(ctx, held)
	if err != nil || state.State != flowlifecycle.PartitionFailed || state.LastErrorCode != flowlifecycle.ArchiveHoldRawIncomplete {
		t.Fatalf("held partition=%+v err=%v", state, err)
	}
	repairs, err := store.ListArchiveRepairCandidates(ctx, 10)
	if err != nil || len(repairs) != 0 {
		t.Fatalf("held day offered for repair: %+v err=%v", repairs, err)
	}

	// A day reconciled under a retired revision is re-archived under the
	// published one; only then can the deletion gate accept it.
	generation, _ = flowlifecycle.Generation(first.Version, 1)
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_partition_states
		(source_date,policy_id,policy_version,state,generation,repair_attempt,archived_at,reconciled_at,late_checked_at,delete_eligible_at)
		VALUES (?,?,?,'reconciled',?,1,?,?,?,?)`, settled, first.ID, first.Version, generation,
		now, now, now, now); err != nil {
		t.Fatal(err)
	}
	s.cfg.Flow.Lifecycle.DeleteGrace = 2 * time.Hour
	if _, err := s.syncFlowLifecycleConfig(ctx, store, now); err != nil {
		t.Fatal(err)
	}
	second, err := store.GetPublishedPolicy(ctx)
	if err != nil || second.Version != first.Version+1 {
		t.Fatalf("second revision=%+v err=%v", second, err)
	}
	superseded, err := store.ListSupersededPartitions(ctx, second, 10)
	if err != nil || len(superseded) != 1 || !superseded[0].SourceDate.Equal(settled) {
		t.Fatalf("superseded=%+v err=%v", superseded, err)
	}
	begin(second, settled)
	if state, err := store.GetPartition(ctx, settled); err != nil || state.State != flowlifecycle.PartitionSealed || state.PolicyVersion != second.Version {
		t.Fatalf("re-archive under the published revision: %+v err=%v", state, err)
	}
}
