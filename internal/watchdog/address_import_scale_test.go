package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

const scaleIPDBPrefixCount = 1 << 20

type failOnceAddressImportRepository struct {
	*MySQLStore
	failAfter uint64
	written   uint64
	failed    bool
}

func (r *failOnceAddressImportRepository) InsertAddressImportBatch(ctx context.Context, tenantID, importID ID, records []AddressImportRecord) error {
	if !r.failed && r.written >= r.failAfter {
		r.failed = true
		return errors.New("injected transient batch failure")
	}
	if err := r.MySQLStore.InsertAddressImportBatch(ctx, tenantID, importID, records); err != nil {
		return err
	}
	r.written += uint64(len(records))
	return nil
}

// TestAddressImportMillionIPDBMySQLEndToEnd generates a valid IPDB trie with
// 1,048,576 distinct IPv4 /20 leaves. The official ipip reader validates the
// artifact before the platform enumerates it, so this cannot pass by replaying
// a small fixture or faking callback counts.
func TestAddressImportMillionIPDBMySQLEndToEnd(t *testing.T) {
	if os.Getenv("WATCHDOG_ADDRESS_IMPORT_SCALE") != "1" {
		t.Skip("set WATCHDOG_ADDRESS_IMPORT_SCALE=1 to run")
	}
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Fatal("WATCHDOG_MYSQL_TEST_DSN is required")
	}

	artifactPath := filepath.Join(t.TempDir(), "million.ipdb")
	artifactSize, checksum := writeMillionIPv4IPDB(t, artifactPath)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}

	const tenantID = ID("tenant_c3b_ipdb_scale")
	const actorID = ID("user_c3b_ipdb_scale")
	const importID = ID("import_c3b_ipdb_scale")
	cleanupAddressImportFixture(t, db, tenantID)
	defer cleanupAddressImportFixture(t, db, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'C3b IPDB scale', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'c3b-ipdb-scale@test.invalid', 'C3b IPDB scale', 'active', 'test', 'c3b-ipdb-scale')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	item, err := store.CreateAddressImport(ctx, AddressImport{
		ID: importID, TenantID: tenantID, SourceSlot: AddressImportSlotCombined,
		Format: AddressImportFormatIPDB, OriginalName: "million.ipdb", ArtifactRef: "scale/million.ipdb",
		ChecksumSHA256: checksum, SizeBytes: uint64(artifactSize), Status: AddressImportStatusQueued, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := EncodeAddressImportJobPayload(item.ID, "CN", 0)
	if err != nil {
		t.Fatal(err)
	}
	jobHash := sha256.Sum256(checkpoint)
	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenantID, JobType: AddressImportJobType, IdempotencyKey: "c3b-ipdb-million",
		RequestHash: hex.EncodeToString(jobHash[:]), CheckpointJSON: checkpoint, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}

	repository := &failOnceAddressImportRepository{MySQLStore: store, failAfter: 100_000}
	worker := OperationJobWorker{
		Repo: store, JobType: AddressImportJobType, Owner: "c3b-ipdb-worker",
		Handler:  NewAddressImportJobHandler(repository, fixedAddressArtifactStore{path: artifactPath}, maxAddressImportBatch),
		LeaseFor: 2 * time.Minute, RetryBase: time.Millisecond, MaxAttempts: 3,
	}
	stopHeapSampler := startAddressImportHeapSampler()
	heapSamplerStopped := false
	defer func() {
		if !heapSamplerStopped {
			_ = stopHeapSampler()
		}
	}()
	started := time.Now()

	leased, err := store.LeaseNextOperationJob(ctx, AddressImportJobType, worker.Owner, worker.LeaseFor)
	if err != nil {
		t.Fatal(err)
	}
	worker.runAttempt(ctx, leased)
	afterFailure, err := store.GetOperationJob(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.Status != OperationJobStatusQueued || afterFailure.ProgressDone != repository.failAfter || !repository.failed {
		t.Fatalf("failed attempt = status:%s progress:%d injected:%v", afterFailure.Status, afterFailure.ProgressDone, repository.failed)
	}

	leased = waitForAddressImportScaleLease(t, ctx, store, worker.Owner)
	if leased.ProgressDone != repository.failAfter {
		t.Fatalf("resumed lease progress=%d, want %d", leased.ProgressDone, repository.failAfter)
	}
	worker.runAttempt(ctx, leased)
	elapsed := time.Since(started)
	peakHeap := stopHeapSampler()
	heapSamplerStopped = true

	job, err = store.GetOperationJob(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	item, err = store.GetAddressImport(ctx, tenantID, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != OperationJobStatusSucceeded || job.ProgressDone != scaleIPDBPrefixCount || item.Status != AddressImportStatusReady || item.RowCountV4 != scaleIPDBPrefixCount || item.RowCountV6 != 0 {
		t.Fatalf("job=%s progress=%d import=%s rows=%d/%d", job.Status, job.ProgressDone, item.Status, item.RowCountV4, item.RowCountV6)
	}
	var rows uint64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_base_prefixes WHERE tenant_id = ? AND import_id = ?`, tenantID, importID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != scaleIPDBPrefixCount {
		t.Fatalf("stored rows=%d, want %d", rows, scaleIPDBPrefixCount)
	}

	rate := float64(rows) / elapsed.Seconds()
	t.Logf("valid IPDB=%d bytes rows=%d elapsed=%s throughput=%.0f rows/s peak_heap=%.1f MiB retry_from=%d",
		artifactSize, rows, elapsed.Round(time.Millisecond), rate, float64(peakHeap)/(1<<20), afterFailure.ProgressDone)
	if elapsed > 4*time.Minute || rate < 4_000 {
		t.Fatalf("IPDB import throughput regression: elapsed=%s rate=%.0f rows/s", elapsed, rate)
	}
	if peakHeap > 512<<20 {
		t.Fatalf("IPDB import peak heap exceeds 512 MiB: %.1f MiB", float64(peakHeap)/(1<<20))
	}
}

func writeMillionIPv4IPDB(t testing.TB, path string) (int, string) {
	t.Helper()
	const prefixBits = 20
	const v4PathNodes = 96
	const subtreeNodes = (1 << prefixBits) - 1
	const nodeCount = v4PathNodes + subtreeNodes
	tree := make([]byte, nodeCount*8)
	for node := 0; node < v4PathNodes; node++ {
		chosenBit := 0
		if node >= 80 {
			chosenBit = 1
		}
		binary.BigEndian.PutUint32(tree[node*8+(1-chosenBit)*4:], nodeCount)
		binary.BigEndian.PutUint32(tree[node*8+chosenBit*4:], uint32(node+1))
	}
	leafPointer := uint32(nodeCount + 1)
	for relative := 0; relative < subtreeNodes; relative++ {
		for bit := 0; bit < 2; bit++ {
			child := relative*2 + 1 + bit
			pointer := leafPointer
			if child < subtreeNodes {
				pointer = uint32(v4PathNodes + child)
			}
			binary.BigEndian.PutUint32(tree[(v4PathNodes+relative)*8+bit*4:], pointer)
		}
	}
	data := append(tree, 0)
	data = appendIPDBTestRecord(data, []byte("CN\tAS\t4134"))
	metadata := ipdbMetadata{
		Build: 1_700_000_000, IPVersion: 1, Languages: map[string]int{"CN": 0},
		NodeCount: nodeCount, TotalSize: len(data), Fields: []string{"country_code", "continent_code", "asn"},
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	file := make([]byte, 4, 4+len(encoded)+len(data))
	binary.BigEndian.PutUint32(file, uint32(len(encoded)))
	file = append(file, encoded...)
	file = append(file, data...)
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256(file)
	return len(file), hex.EncodeToString(checksum[:])
}

func waitForAddressImportScaleLease(t testing.TB, ctx context.Context, store *MySQLStore, owner string) OperationJob {
	t.Helper()
	for {
		job, err := store.LeaseNextOperationJob(ctx, AddressImportJobType, owner, 2*time.Minute)
		if err == nil {
			return job
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func startAddressImportHeapSampler() func() uint64 {
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	var mu sync.Mutex
	peak := baseline.HeapAlloc
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var sample runtime.MemStats
				runtime.ReadMemStats(&sample)
				mu.Lock()
				if sample.HeapAlloc > peak {
					peak = sample.HeapAlloc
				}
				mu.Unlock()
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-finished
		mu.Lock()
		defer mu.Unlock()
		if peak <= baseline.HeapAlloc {
			return 0
		}
		return peak - baseline.HeapAlloc
	}
}

var _ addressImportJobRepository = (*failOnceAddressImportRepository)(nil)
