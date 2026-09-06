package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func TestDiskAddressArtifactStoreBoundsAndResolvesSafely(t *testing.T) {
	store := DiskAddressArtifactStore{Dir: t.TempDir(), MaxBytes: 8}
	artifact, err := store.SaveAddressArtifact(context.Background(), "tenant-a", "import-a", "Geo.MMDB", strings.NewReader("12345678"))
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256([]byte("12345678"))
	if artifact.Format != AddressImportFormatMMDB || artifact.SizeBytes != 8 || artifact.ChecksumSHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("artifact = %#v", artifact)
	}
	path, err := store.ResolveAddressArtifact(artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "12345678" {
		t.Fatalf("read artifact = %q, err=%v", data, err)
	}
	if _, err := store.SaveAddressArtifact(context.Background(), "tenant-a", "import-b", "Geo.MMDB", strings.NewReader("123456789")); err == nil {
		t.Fatal("oversized artifact must be rejected")
	}
	if _, err := store.SaveAddressArtifact(context.Background(), "tenant-a", "import-c", "Geo.csv", strings.NewReader("value")); err == nil {
		t.Fatal("unknown extension must be rejected")
	}
	if _, err := store.ResolveAddressArtifact("../source.mmdb"); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	if err := store.RemoveAddressArtifact(artifact.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAddressArtifact(artifact.Ref); !os.IsNotExist(err) {
		t.Fatalf("resolve removed artifact error = %v", err)
	}
}
