package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
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

// TestAddressImportProductionMMDBToWADS exercises the real control-plane
// boundary: a production-sized MMDB is streamed into MySQL by a resumable
// operation job, then an independent operation job emits and reloads the
// immutable WADS object consumed by flow workers. It is opt-in because it
// deliberately writes more than a million rows and records a machine-specific
// performance baseline.
func TestAddressImportProductionMMDBToWADS(t *testing.T) {
	if os.Getenv("WATCHDOG_ADDRESS_IMPORT_SCALE") != "1" {
		t.Skip("set WATCHDOG_ADDRESS_IMPORT_SCALE=1 to run")
	}
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	path := os.Getenv("WATCHDOG_ADDRESS_IMPORT_MMDB")
	if dsn == "" || path == "" {
		t.Fatal("WATCHDOG_MYSQL_TEST_DSN and WATCHDOG_ADDRESS_IMPORT_MMDB are required")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}

	const tenantID = ID("tenant_c3b_mmdb_scale")
	const actorID = ID("user_c3b_mmdb_scale")
	const importID = ID("import_c3b_mmdb_scale")
	cleanupAddressImportFixture(t, db, tenantID)
	defer cleanupAddressImportFixture(t, db, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'C3b MMDB scale', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'c3b-mmdb-scale@test.invalid', 'C3b MMDB scale', 'active', 'test', 'c3b-mmdb-scale')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	item, err := store.CreateAddressImport(ctx, AddressImport{
		ID: importID, TenantID: tenantID, SourceSlot: AddressImportSlotASN,
		Format: AddressImportFormatMMDB, OriginalName: filepath.Base(path), ArtifactRef: "scale/production.mmdb",
		ChecksumSHA256: checksum, SizeBytes: uint64(info.Size()), Status: AddressImportStatusQueued, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := EncodeAddressImportJobPayload(item.ID, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	jobHash := sha256.Sum256(checkpoint)
	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenantID, JobType: AddressImportJobType, IdempotencyKey: "c3b-mmdb-production",
		RequestHash: hex.EncodeToString(jobHash[:]), CheckpointJSON: checkpoint, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}

	repository := &failOnceAddressImportRepository{MySQLStore: store, failAfter: 100_000}
	importWorker := OperationJobWorker{
		Repo: store, JobType: AddressImportJobType, Owner: "c3b-mmdb-import",
		Handler:  NewAddressImportJobHandler(repository, fixedAddressArtifactStore{path: path}, maxAddressImportBatch),
		LeaseFor: 5 * time.Minute, RetryBase: time.Millisecond, MaxAttempts: 3,
	}
	stopImportHeap := startAddressImportHeapSampler()
	importStarted := time.Now()
	leased, err := store.LeaseNextOperationJob(ctx, AddressImportJobType, importWorker.Owner, importWorker.LeaseFor)
	if err != nil {
		t.Fatal(err)
	}
	importWorker.runAttempt(ctx, leased)
	afterFailure, err := store.GetOperationJob(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.Status != OperationJobStatusQueued || afterFailure.ProgressDone != repository.failAfter || !repository.failed {
		t.Fatalf("failed attempt = status:%s progress:%d injected:%v", afterFailure.Status, afterFailure.ProgressDone, repository.failed)
	}
	leased = waitForAddressImportScaleLease(t, ctx, store, importWorker.Owner)
	importWorker.runAttempt(ctx, leased)
	importElapsed := time.Since(importStarted)
	importPeakHeap := stopImportHeap()

	job, err = store.GetOperationJob(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	item, err = store.GetAddressImport(ctx, tenantID, importID)
	if err != nil {
		t.Fatal(err)
	}
	rows := item.RowCountV4 + item.RowCountV6
	if job.Status != OperationJobStatusSucceeded || item.Status != AddressImportStatusReady || rows < 1_000_000 || job.ProgressDone != rows {
		t.Fatalf("job=%s progress=%d import=%s rows=%d/%d", job.Status, job.ProgressDone, item.Status, item.RowCountV4, item.RowCountV6)
	}
	var storedRows, distinctASNs, supplierNames uint64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(DISTINCT NULLIF(asn, 0)),
		       SUM(CASE WHEN COALESCE(operator_name, '') <> '' THEN 1 ELSE 0 END)
		FROM address_base_prefixes WHERE tenant_id = ? AND import_id = ?
	`, tenantID, importID).Scan(&storedRows, &distinctASNs, &supplierNames); err != nil {
		t.Fatal(err)
	}
	asnOnly := strings.Contains(strings.ToLower(item.DatabaseType), "asn")
	if storedRows != rows || (asnOnly && (distinctASNs == 0 || supplierNames != 0)) {
		t.Fatalf("stored=%d rows=%d distinct_asns=%d supplier_names=%d", storedRows, rows, distinctASNs, supplierNames)
	}
	if _, err := store.ActivateAddressImport(ctx, tenantID, importID, actorID, 0); err != nil {
		t.Fatal(err)
	}

	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 512 << 20}
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects)
	if err != nil {
		t.Fatal(err)
	}
	effective := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	preview, err := publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeAddressDimensionPublishJobPayload(AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	buildHash := sha256.Sum256(payload)
	buildJob, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenantID, JobType: AddressSnapshotBuildJob, IdempotencyKey: "c3b-mmdb-wads",
		RequestHash: hex.EncodeToString(buildHash[:]), CheckpointJSON: payload, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	buildWorker := OperationJobWorker{
		Repo: store, JobType: AddressSnapshotBuildJob, Owner: "c3b-mmdb-builder",
		Handler: NewAddressSnapshotBuildJobHandler(publisher), LeaseFor: 10 * time.Minute, RetryBase: time.Millisecond, MaxAttempts: 1,
	}
	stopBuildHeap := startAddressImportHeapSampler()
	buildStarted := time.Now()
	leased, err = store.LeaseNextOperationJob(ctx, AddressSnapshotBuildJob, buildWorker.Owner, buildWorker.LeaseFor)
	if err != nil {
		t.Fatal(err)
	}
	buildWorker.runAttempt(ctx, leased)
	buildElapsed := time.Since(buildStarted)
	buildPeakHeap := stopBuildHeap()
	buildJob, err = store.GetOperationJob(ctx, tenantID, buildJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if buildJob.Status != OperationJobStatusSucceeded {
		t.Fatalf("WADS build job=%s code=%s detail=%s", buildJob.Status, buildJob.LastErrorCode, buildJob.LastErrorDetail)
	}
	snapshot, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, buildJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	objectPath, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}
	stopCompileHeap := startAddressImportHeapSampler()
	compileStarted := time.Now()
	index, err := flowdimension.DecodeAndCompileAddressSnapshot(data, snapshot.Checksum, flowdimension.AddressSnapshotLimits{})
	compileElapsed := time.Since(compileStarted)
	compilePeakHeap := stopCompileHeap()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.SnapshotID != string(snapshot.ID) || len(artifact.IPv4Ranges)+len(artifact.IPv6Ranges) == 0 || (asnOnly && len(artifact.Operators) != 0) {
		t.Fatalf("WADS identity/ranges/operators = %q %d/%d %d", artifact.SnapshotID, len(artifact.IPv4Ranges), len(artifact.IPv6Ranges), len(artifact.Operators))
	}

	samples := sampleMMDBAddresses(t, path, rows, 20_000)
	latencies := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		started := time.Now()
		resolution, found := index.ResolveAddress(sample.address)
		latencies = append(latencies, time.Since(started))
		if !found || resolution.SupplierASN != sample.asn {
			t.Fatalf("lookup %s = found:%v asn:%d, want %d", sample.address, found, resolution.SupplierASN, sample.asn)
		}
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	lookupStarted := time.Now()
	const lookupCount = 1_000_000
	for indexAt := 0; indexAt < lookupCount; indexAt++ {
		index.ResolveAddress(samples[indexAt%len(samples)].address)
	}
	lookupElapsed := time.Since(lookupStarted)
	lookupRate := float64(lookupCount) / lookupElapsed.Seconds()
	p95, p99 := percentileDuration(latencies, 95), percentileDuration(latencies, 99)
	if lookupRate < 100_000 || p99 > time.Millisecond {
		t.Fatalf("WADS lookup regression: rate=%.0f/s p99=%s", lookupRate, p99)
	}
	// The catalog measurement owns its compiled index. Release the earlier
	// correctness index so it is not counted as a second published address
	// library: the platform has one shared AddressSnap, not one per tenant.
	index = nil
	catalogScale := measureAddressSnapshotCatalogScale(t, artifact, samples)

	t.Logf("production MMDB=%d bytes sha256=%s rows=%d (v4=%d v6=%d) distinct_asns=%d import=%s %.0f rows/s import_peak_heap=%.1f MiB retry_from=%d",
		info.Size(), checksum, rows, item.RowCountV4, item.RowCountV6, distinctASNs, importElapsed.Round(time.Millisecond), float64(rows)/importElapsed.Seconds(), float64(importPeakHeap)/(1<<20), afterFailure.ProgressDone)
	t.Logf("WADS=%d bytes ranges=%d/%d values=%d build=%s build_peak_heap=%.1f MiB compile=%s compile_peak_heap=%.1f MiB lookup=%.0f/s p95=%s p99=%s",
		len(data), len(artifact.IPv4Ranges), len(artifact.IPv6Ranges), len(artifact.Values), buildElapsed.Round(time.Millisecond), float64(buildPeakHeap)/(1<<20), compileElapsed.Round(time.Millisecond), float64(compilePeakHeap)/(1<<20), lookupRate, p95, p99)
	t.Logf("shared catalog retained_heap=%0.1f MiB swap=%s concurrent_lookup_max=%s",
		float64(catalogScale.retainedHeap)/(1<<20), catalogScale.swapPause, catalogScale.concurrentLookupMax)
}

type addressSnapshotCatalogScale struct {
	retainedHeap        uint64
	swapPause           time.Duration
	concurrentLookupMax time.Duration
}

func measureAddressSnapshotCatalogScale(t testing.TB, source flowdimension.AddressSnapshotArtifact, samples []mmdbLookupSample) addressSnapshotCatalogScale {
	t.Helper()
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	catalog, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	result := addressSnapshotCatalogScale{}
	data, err := flowdimension.EncodeAddressSnapshot(source, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	dimension, err := flowdimension.DecodeAndCompileAddressSnapshot(data, "sha256:"+hex.EncodeToString(digest[:]), flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classification, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		Version: 1, EffectiveFrom: source.EffectiveFrom, DimensionSnapshotID: source.SnapshotID,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Install(flowworker.EnrichmentVersion{Dimension: dimension, Classification: classification}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var current runtime.MemStats
	runtime.ReadMemStats(&current)
	if current.HeapAlloc > baseline.HeapAlloc {
		result.retainedHeap = current.HeapAlloc - baseline.HeapAlloc
	}
	if result.retainedHeap > 256<<20 {
		t.Fatalf("shared catalog retained heap = %d", result.retainedHeap)
	}

	updateArtifact := source
	updateArtifact.SnapshotID = "snapshot_c4b2_shared_v2"
	updateArtifact.Version = 2
	updateArtifact.EffectiveFrom = source.EffectiveFrom.Add(time.Minute)
	updateData, err := flowdimension.EncodeAddressSnapshot(updateArtifact, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	updateDigest := sha256.Sum256(updateData)
	updateDimension, err := flowdimension.DecodeAndCompileAddressSnapshot(updateData, "sha256:"+hex.EncodeToString(updateDigest[:]), flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	updateClassification, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		Version: 2, EffectiveFrom: updateArtifact.EffectiveFrom, DimensionSnapshotID: updateArtifact.SnapshotID,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}

	var stopReaders atomic.Bool
	var readerError atomic.Bool
	var readerOperations atomic.Uint64
	var maximumLookupNS atomic.Int64
	var readers sync.WaitGroup
	for readerIndex := 0; readerIndex < 4; readerIndex++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for !stopReaders.Load() {
				started := time.Now()
				version, err := catalog.Select(updateArtifact.EffectiveFrom.Add(time.Minute))
				if err != nil {
					readerError.Store(true)
					return
				}
				version.Dimension.ClassifyEndpoints(samples[0].address, samples[len(samples)-1].address)
				elapsed := time.Since(started).Nanoseconds()
				for current := maximumLookupNS.Load(); elapsed > current && !maximumLookupNS.CompareAndSwap(current, elapsed); current = maximumLookupNS.Load() {
				}
				readerOperations.Add(1)
			}
		}()
	}
	deadline := time.Now().Add(time.Second)
	for readerOperations.Load() < 10_000 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	swapStarted := time.Now()
	if err := catalog.Install(flowworker.EnrichmentVersion{Dimension: updateDimension, Classification: updateClassification}); err != nil {
		stopReaders.Store(true)
		readers.Wait()
		t.Fatal(err)
	}
	result.swapPause = time.Since(swapStarted)
	deadline = time.Now().Add(time.Second)
	for readerOperations.Load() < 50_000 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	stopReaders.Store(true)
	readers.Wait()
	if readerError.Load() || readerOperations.Load() < 10_000 {
		t.Fatalf("catalog readers failed or made insufficient progress: failed=%v operations=%d", readerError.Load(), readerOperations.Load())
	}
	result.concurrentLookupMax = time.Duration(maximumLookupNS.Load())
	if result.swapPause > 10*time.Millisecond || result.concurrentLookupMax > 100*time.Millisecond {
		t.Fatalf("catalog swap regression: swap=%s max_lookup=%s", result.swapPause, result.concurrentLookupMax)
	}
	return result
}

type mmdbLookupSample struct {
	address netip.Addr
	asn     uint32
}

func sampleMMDBAddresses(t testing.TB, path string, rows uint64, maximum int) []mmdbLookupSample {
	t.Helper()
	stride := rows / uint64(maximum)
	if stride == 0 {
		stride = 1
	}
	ordinal := uint64(0)
	samples := make([]mmdbLookupSample, 0, maximum)
	_, err := StreamMMDB(path, func(record AddressImportRecord) error {
		if ordinal%stride == 0 && len(samples) < maximum {
			prefix, err := netip.ParsePrefix(record.Prefix)
			if err != nil {
				return err
			}
			samples = append(samples, mmdbLookupSample{address: prefix.Addr(), asn: record.ASN})
		}
		ordinal++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) == 0 {
		t.Fatal("production MMDB yielded no lookup samples")
	}
	return samples
}

func percentileDuration(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percentile + 99) / 100
	if index > 0 {
		index--
	}
	return values[index]
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
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
