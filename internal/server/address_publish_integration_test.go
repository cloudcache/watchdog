package server

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestAddressPublishWADSIntegration drives the de-tenanted geo/AddressSnap chain
// end to end against a real, empty MySQL: import a real MMDB -> activate its slot
// -> preview -> build the WADS publication -> decode the stored object. Opt-in via
// WATCHDOG_TEST_MYSQL_DSN so ordinary unit tests do not require MySQL.
func TestAddressPublishWADSIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	// Point at a throwaway database so the run starts from an empty schema.
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_addr_wads_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	cfg := Config{
		MySQL:   MySQLConfig{DSN: dsn},
		Admin:   AdminConfig{Username: "addr-admin", Password: "addr-admin-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir()},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	var adminID string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM users WHERE username = ?`, "addr-admin").Scan(&adminID); err != nil {
		t.Fatalf("load admin id: %v", err)
	}

	store := address.NewStore(s.db)

	// 1) Create a queued import and stream a real MMDB fixture into base prefixes,
	//    exactly as the import job handler would (Begin -> InsertBatch -> Complete).
	imp, err := store.CreateAddressImport(ctx, address.AddressImport{
		SourceSlot: address.AddressImportSlotGeo, Format: address.AddressImportFormatMMDB,
		OriginalName: "GeoLite2-City-Test.mmdb", ArtifactRef: "address-imports/it/source.mmdb",
		ChecksumSHA256: strings.Repeat("a", 64), SizeBytes: 20809, Status: address.AddressImportStatusQueued,
		CreatedBy: adminID,
	})
	if err != nil {
		t.Fatalf("create import: %v", err)
	}
	if err := store.BeginAddressImport(ctx, imp.ID); err != nil {
		t.Fatalf("begin import: %v", err)
	}
	fixture := filepath.Join("..", "..", "akvorado", "orchestrator", "geoip", "testdata", "GeoLite2-City-Test.mmdb")
	batch := make([]address.AddressImportRecord, 0, 512)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := store.InsertAddressImportBatch(ctx, imp.ID, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	metadata, err := address.StreamMMDB(fixture, func(rec address.AddressImportRecord) error {
		batch = append(batch, rec)
		if len(batch) == 512 {
			return flush()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream mmdb: %v", err)
	}
	if err := flush(); err != nil {
		t.Fatalf("flush batch: %v", err)
	}
	ready, err := store.CompleteAddressImport(ctx, imp.ID, metadata, "")
	if err != nil {
		t.Fatalf("complete import: %v", err)
	}
	if ready.Status != address.AddressImportStatusReady || ready.RowCountV4+ready.RowCountV6 == 0 {
		t.Fatalf("import not ready or empty: status=%s v4=%d v6=%d", ready.Status, ready.RowCountV4, ready.RowCountV6)
	}

	// 2) Activate the ready generation into its slot.
	if _, err := store.ActivateAddressImport(ctx, imp.ID, adminID, 0); err != nil {
		t.Fatalf("activate import: %v", err)
	}

	// 3) Preview and build the WADS publication.
	objects := address.DiskDimensionObjectStore{Dir: t.TempDir()}
	publisher, err := address.NewPublisher(store, objects)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	effectiveFrom := time.Now().UTC().Truncate(time.Minute)
	preview, err := publisher.PreviewAddressDimension(ctx, effectiveFrom)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if preview.DraftDigest == "" || preview.SourcePrefixCount == 0 {
		t.Fatalf("empty preview: %+v", preview)
	}
	buildJobID := "01JADDRWADS0000000000000IT"
	snapshot, err := publisher.BuildAddressSnapshotPublication(ctx, adminID, buildJobID, address.AddressDimensionPublishRequest{
		EffectiveFrom: effectiveFrom, PreviewDigest: preview.DraftDigest,
	})
	if err != nil {
		t.Fatalf("build publication: %v", err)
	}
	if snapshot.ObjectFormat != address.AddressSnapshotObjectFormat || snapshot.Version != 1 || snapshot.Status != address.AddressDimensionStatusActive {
		t.Fatalf("unexpected snapshot: format=%s version=%d status=%s", snapshot.ObjectFormat, snapshot.Version, snapshot.Status)
	}

	// 4) The stored object is a valid WADS that decodes to non-empty ranges.
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatalf("resolve object: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if len(data) < 4 || string(data[:4]) != "WADS" {
		t.Fatalf("object is not a WADS artifact (magic=%q)", data[:min(4, len(data))])
	}
	artifact, err := flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatalf("decode WADS: %v", err)
	}
	if len(artifact.IPv4Ranges)+len(artifact.IPv6Ranges) == 0 {
		t.Fatalf("decoded WADS has no ranges")
	}

	// 5) Idempotent re-build with the same build job id returns the same snapshot
	//    and identical stored bytes (content-addressed object).
	again, err := publisher.BuildAddressSnapshotPublication(ctx, adminID, buildJobID, address.AddressDimensionPublishRequest{
		EffectiveFrom: effectiveFrom, PreviewDigest: preview.DraftDigest,
	})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if again.Checksum != snapshot.Checksum {
		t.Fatalf("rebuild checksum drift: %s vs %s", again.Checksum, snapshot.Checksum)
	}
}

func dropTestDatabase(t *testing.T, baseDSN, name string) {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + name + "`"); err != nil {
		t.Fatalf("drop database: %v", err)
	}
}
