package flowtombstone

import (
	"path/filepath"
	"testing"
	"time"
)

func TestBarrierLKGRejectsRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "barrier.json")
	now := time.Now().UTC().Truncate(time.Millisecond)
	one, _ := Advance(nil, day("2026-08-01"), "barrier_a", 1, now)
	two, _ := Advance(&one, day("2026-08-02"), "barrier_b", 2, now.Add(time.Second))
	if err := SaveFile(path, two); err != nil {
		t.Fatal(err)
	}
	if err := SaveFile(path, one); err == nil {
		t.Fatal("rollback LKG accepted")
	}
	loaded, err := LoadFile(path)
	if err != nil || loaded.Revision != 2 {
		t.Fatalf("unexpected LKG: %#v err=%v", loaded, err)
	}
}
