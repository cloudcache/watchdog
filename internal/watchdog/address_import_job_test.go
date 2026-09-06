package watchdog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type fakeAddressImportJobRepository struct {
	item      AddressImport
	records   []AddressImportRecord
	begin     int
	completed int
	failed    int
	metadata  AddressImportMetadata
}

func (r *fakeAddressImportJobRepository) GetAddressImport(context.Context, ID, ID) (AddressImport, error) {
	return r.item, nil
}

func (r *fakeAddressImportJobRepository) BeginAddressImport(context.Context, ID, ID) error {
	r.begin++
	r.item.Status = AddressImportStatusImporting
	return nil
}

func (r *fakeAddressImportJobRepository) InsertAddressImportBatch(_ context.Context, _, _ ID, records []AddressImportRecord) error {
	r.records = append(r.records, records...)
	return nil
}

func (r *fakeAddressImportJobRepository) CompleteAddressImport(_ context.Context, _, _ ID, metadata AddressImportMetadata, _ string) (AddressImport, error) {
	r.completed++
	r.metadata = metadata
	r.item.Status = AddressImportStatusReady
	return r.item, nil
}

func (r *fakeAddressImportJobRepository) FailAddressImport(context.Context, ID, ID, string, string) error {
	r.failed++
	r.item.Status = AddressImportStatusFailed
	return nil
}

type fixedAddressArtifactStore struct{ path string }

func (s fixedAddressArtifactStore) SaveAddressArtifact(context.Context, ID, ID, string, io.Reader) (AddressArtifact, error) {
	return AddressArtifact{}, errors.New("not implemented")
}
func (s fixedAddressArtifactStore) ResolveAddressArtifact(string) (string, error) { return s.path, nil }
func (s fixedAddressArtifactStore) RemoveAddressArtifact(string) error            { return nil }

func TestAddressImportJobStreamsBatchesAndResumesByOrdinal(t *testing.T) {
	path := filepath.Join("..", "..", "akvorado", "orchestrator", "geoip", "testdata", "GeoLite2-City-Test.mmdb")
	var total uint64
	if _, err := StreamMMDB(path, func(AddressImportRecord) error {
		total++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if total < 2 {
		t.Fatalf("fixture contains only %d records", total)
	}
	repo := &fakeAddressImportJobRepository{item: AddressImport{
		ID: "import-a", TenantID: "tenant-a", Format: AddressImportFormatMMDB,
		ArtifactRef: "address-imports/tenant-a/import-a/source.mmdb", Status: AddressImportStatusImporting,
	}}
	checkpoint, err := EncodeAddressImportJobPayload(repo.item.ID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewAddressImportJobHandler(repo, fixedAddressArtifactStore{path: path}, 2)(context.Background(), OperationJob{
		ID: "job-a", TenantID: repo.item.TenantID, CheckpointJSON: checkpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "address-import:import-a" || repo.begin != 1 || repo.completed != 1 || uint64(len(repo.records)) != total-1 {
		t.Fatalf("result=%q begin=%d completed=%d records=%d total=%d", result, repo.begin, repo.completed, len(repo.records), total)
	}
	if repo.metadata.Format != AddressImportFormatMMDB || repo.metadata.DatabaseType == "" {
		t.Fatalf("metadata = %#v", repo.metadata)
	}
}

func TestAddressImportJobFailsInvalidArtifactTerminally(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.mmdb")
	if err := os.WriteFile(path, []byte("not an mmdb"), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &fakeAddressImportJobRepository{item: AddressImport{
		ID: "import-b", TenantID: "tenant-a", Format: AddressImportFormatMMDB,
		ArtifactRef: "address-imports/tenant-a/import-b/source.mmdb", Status: AddressImportStatusQueued,
	}}
	checkpoint, err := EncodeAddressImportJobPayload(repo.item.ID, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewAddressImportJobHandler(repo, fixedAddressArtifactStore{path: path}, 100)(context.Background(), OperationJob{
		ID: "job-b", TenantID: repo.item.TenantID, CheckpointJSON: checkpoint,
	})
	if err == nil || !IsTerminalJobError(err) || repo.failed != 1 || repo.completed != 0 {
		t.Fatalf("err=%v failed=%d completed=%d", err, repo.failed, repo.completed)
	}
}
