package watchdog

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestDiskDimensionObjectStoreWritesChecksummedImmutableBundle(t *testing.T) {
	store := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1024}
	object, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-a", []byte(`{"schema_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if object.Ref != "dimension-snapshots/tenant-a/snapshot-a/bundle.json" || !strings.HasPrefix(object.Checksum, "sha256:") {
		t.Fatalf("unexpected object: %#v", object)
	}
	path, err := store.ResolveDimensionObject(object.Ref)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != `{"schema_version":1}` {
		t.Fatalf("read object: %q, %v", data, err)
	}
	if _, err := store.ResolveDimensionObject("../bundle.json"); err == nil {
		t.Fatal("expected escaping object reference to fail")
	}
	if _, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-a", []byte(`{"schema_version":2}`)); err == nil {
		t.Fatal("expected immutable object overwrite to fail")
	}
	data, err = os.ReadFile(path)
	if err != nil || string(data) != `{"schema_version":1}` {
		t.Fatalf("immutable object was changed: %q, %v", data, err)
	}
}

func TestDiskDimensionObjectStoreEnforcesSizeLimit(t *testing.T) {
	store := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 3}
	if _, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-a", []byte("four")); err == nil {
		t.Fatal("expected oversized dimension object to fail")
	}
}

func TestDiskDimensionObjectStoreWritesAndRetriesAddressSnapshot(t *testing.T) {
	store := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1024}
	data := []byte("WADS-binary-fixture")
	first, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-wads", data)
	if err != nil {
		t.Fatal(err)
	}
	if first.Ref != "dimension-snapshots/tenant-a/snapshot-wads/address-snapshot.wads" {
		t.Fatalf("WADS object ref = %q", first.Ref)
	}
	second, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-wads", data)
	if err != nil || second != first {
		t.Fatalf("idempotent WADS save = %#v, %v", second, err)
	}
	if _, err := store.SaveDimensionObject(context.Background(), "tenant-a", "snapshot-wads", []byte("WADS-different")); err == nil {
		t.Fatal("different retry content must not replace immutable WADS object")
	}
}
