package alerts

import (
	"context"
	"testing"
	"time"
)

// fakeQuietHoursReader stands in for the MySQL-backed source and records the
// arguments it is called with so the test can assert the PocketBase id and
// system are threaded through unchanged.
type fakeQuietHoursReader struct {
	windows                            []QuietHourWindow
	gotProvider, gotSubject, gotSystem string
}

func (f *fakeQuietHoursReader) QuietHoursForExternalSubject(_ context.Context, provider, subject, systemID string) ([]QuietHourWindow, error) {
	f.gotProvider, f.gotSubject, f.gotSystem = provider, subject, systemID
	return f.windows, nil
}

// TestIsNotificationSilencedUsesReader covers the MySQL path: when a reader is
// wired, silencing is decided from its windows (the legacy PocketBase read is
// bypassed) and the resolved windows still flow through quietHourWindowActive.
func TestIsNotificationSilencedUsesReader(t *testing.T) {
	now := time.Now().UTC()

	active := &fakeQuietHoursReader{windows: []QuietHourWindow{
		{Type: "one-time", Start: now.Add(-time.Hour), End: now.Add(time.Hour)},
	}}
	am := &AlertManager{quietHoursReader: active}
	if !am.IsNotificationSilenced("pb_user_1", "sys_a") {
		t.Fatal("an active window from the reader should silence")
	}
	if active.gotProvider != "pocketbase" || active.gotSubject != "pb_user_1" || active.gotSystem != "sys_a" {
		t.Fatalf("reader called with (%q, %q, %q)", active.gotProvider, active.gotSubject, active.gotSystem)
	}

	future := &fakeQuietHoursReader{windows: []QuietHourWindow{
		{Type: "one-time", Start: now.Add(time.Hour), End: now.Add(2 * time.Hour)},
	}}
	if (&AlertManager{quietHoursReader: future}).IsNotificationSilenced("pb_user_1", "sys_a") {
		t.Fatal("a window that has not started should not silence")
	}

	if (&AlertManager{quietHoursReader: &fakeQuietHoursReader{}}).IsNotificationSilenced("pb_user_1", "sys_a") {
		t.Fatal("no windows should not silence")
	}
}
