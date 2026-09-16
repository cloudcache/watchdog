package flowtombstone

import (
	"testing"
	"time"
)

func TestBarrierAdvanceCompactsExceptionDays(t *testing.T) {
	now := time.Date(2026, 9, 16, 1, 2, 3, 0, time.UTC)
	first, err := Advance(nil, day("2026-08-01"), "barrier_a", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	third, err := Advance(&first, day("2026-08-03"), "barrier_b", 2, now.Add(time.Second))
	if err != nil || len(third.ExceptionDays) != 1 || third.ExceptionDays[0] != "2026-08-03" {
		t.Fatalf("unexpected gap publication: %#v err=%v", third, err)
	}
	compacted, err := Advance(&third, day("2026-08-02"), "barrier_c", 3, now.Add(2*time.Second))
	if err != nil || compacted.DeletedThrough != "2026-08-03" || len(compacted.ExceptionDays) != 0 {
		t.Fatalf("unexpected compacted publication: %#v err=%v", compacted, err)
	}
	for _, value := range []string{"2020-01-01", "2026-08-01", "2026-08-03"} {
		if _, ok := compacted.Covers(day(value).Add(12 * time.Hour)); !ok {
			t.Fatalf("expected %s to be covered", value)
		}
	}
	if _, ok := compacted.Covers(day("2026-08-04")); ok {
		t.Fatal("future day unexpectedly covered")
	}
}

func TestGuardRejectsRollbackAndChangedSameRevision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	one, _ := Advance(nil, day("2026-08-01"), "barrier_a", 1, now)
	guard, err := NewGuard(&one)
	if err != nil {
		t.Fatal(err)
	}
	changed := one
	changed.ID = "barrier_changed"
	if err := guard.Install(changed); err == nil {
		t.Fatal("changed same revision accepted")
	}
	two, _ := Advance(&one, day("2026-08-02"), "barrier_b", 2, now.Add(time.Second))
	if err := guard.Install(two); err != nil {
		t.Fatal(err)
	}
	if err := guard.Install(one); err == nil {
		t.Fatal("rollback accepted")
	}
}

func TestBarrierRejectsRevisionReservedForReceiptLifecycleGeneration(t *testing.T) {
	barrier := Barrier{
		SchemaVersion: SchemaVersion, ID: "barrier_reserved", Revision: MaxRevision + 1,
		DeletedThrough: "2026-08-01", PublishedAt: time.Now().UTC(),
	}
	if err := barrier.Validate(); err == nil {
		t.Fatal("reserved revision was accepted")
	}
	if _, err := Advance(nil, day("2026-08-01"), "barrier_reserved", MaxRevision+1, time.Now().UTC()); err == nil {
		t.Fatal("reserved revision was published")
	}
}

func day(value string) time.Time {
	result, _ := time.Parse(time.DateOnly, value)
	return result
}
