package address

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// Faithful de-tenant port of internal/watchdog TestDiskAddressArtifactStoreBoundsAndResolvesSafely:
// the tenant arg and the <tenant> path segment are removed; every bound/safety assertion is preserved.
func TestDiskArtifactStoreBoundsAndResolvesSafely(t *testing.T) {
	store := DiskArtifactStore{Dir: t.TempDir(), MaxBytes: 8}
	artifact, err := store.SaveArtifact(context.Background(), "import-a", "Geo.MMDB", strings.NewReader("12345678"))
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256([]byte("12345678"))
	if artifact.Format != AddressImportFormatMMDB || artifact.SizeBytes != 8 || artifact.ChecksumSHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("artifact = %#v", artifact)
	}
	path, err := store.ResolveArtifact(artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "12345678" {
		t.Fatalf("read artifact = %q, err=%v", data, err)
	}
	if _, err := store.SaveArtifact(context.Background(), "import-b", "Geo.MMDB", strings.NewReader("123456789")); err == nil {
		t.Fatal("oversized artifact must be rejected")
	}
	if _, err := store.SaveArtifact(context.Background(), "import-c", "Geo.csv", strings.NewReader("value")); err == nil {
		t.Fatal("unknown extension must be rejected")
	}
	if _, err := store.ResolveArtifact("../source.mmdb"); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	if err := store.RemoveArtifact(artifact.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveArtifact(artifact.Ref); !os.IsNotExist(err) {
		t.Fatalf("resolve removed artifact error = %v", err)
	}
}
