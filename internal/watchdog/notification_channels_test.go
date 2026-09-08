package watchdog

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeNotificationChannelRepo struct {
	channels NotificationChannels
	lastPut  NotificationChannels
}

func (f *fakeNotificationChannelRepo) GetNotificationChannels(context.Context, ID, ID) (NotificationChannels, error) {
	return f.channels, nil
}
func (f *fakeNotificationChannelRepo) ReplaceNotificationChannels(_ context.Context, _, _ ID, c NotificationChannels) error {
	f.lastPut = c
	f.channels = c
	return nil
}
func TestNotificationChannelsValidation(t *testing.T) {
	ok, err := ValidateNotificationChannels(NotificationChannels{
		Emails:   []string{" ops@example.com ", ""},
		Webhooks: []string{"https://hooks.example.com/x", ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ok.Emails) != 1 || ok.Emails[0] != "ops@example.com" || len(ok.Webhooks) != 1 {
		t.Fatalf("normalized = %+v", ok)
	}
	if _, err := ValidateNotificationChannels(NotificationChannels{Emails: []string{"not-an-email"}}); err == nil {
		t.Fatal("invalid email must be rejected")
	}
	if _, err := ValidateNotificationChannels(NotificationChannels{Webhooks: []string{"ftp://x/y"}}); err == nil {
		t.Fatal("non-http webhook must be rejected")
	}
}

func TestNotificationChannelsAPI(t *testing.T) {
	repo := &fakeNotificationChannelRepo{channels: NotificationChannels{Emails: []string{"a@b.com"}, Webhooks: []string{}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), NotificationChannels: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/notification-channels", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "a@b.com") {
		t.Fatalf("get status = %d body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/me/notification-channels",
		strings.NewReader(`{"emails":["ops@example.com"],"webhooks":["https://h.example.com/x"]}`))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.lastPut.Emails) != 1 || repo.lastPut.Webhooks[0] != "https://h.example.com/x" {
		t.Fatalf("replaced = %+v", repo.lastPut)
	}

	// Invalid webhook is rejected.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/api/v1/me/notification-channels", strings.NewReader(`{"webhooks":["nope"]}`))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid webhook status = %d", rec.Code)
	}
}

// TestMySQLNotificationChannelsLifecycle proves per-user wholesale replace.
func TestMySQLNotificationChannelsLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_nc_1', ?, 'nc@test.local', 'NC', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	// Replace with a set, then read back.
	if err := store.ReplaceNotificationChannels(ctx, tenant, "user_nc_1", NotificationChannels{
		Emails:   []string{"ops@example.com", "sre@example.com"},
		Webhooks: []string{"https://hooks.example.com/a"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetNotificationChannels(ctx, tenant, "user_nc_1")
	if err != nil || len(got.Emails) != 2 || len(got.Webhooks) != 1 {
		t.Fatalf("get = %+v err=%v", got, err)
	}

	// Replace is wholesale: fewer channels overwrites, not merges.
	if err := store.ReplaceNotificationChannels(ctx, tenant, "user_nc_1", NotificationChannels{
		Emails: []string{"only@example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	after, _ := store.GetNotificationChannels(ctx, tenant, "user_nc_1")
	if len(after.Emails) != 1 || len(after.Webhooks) != 0 {
		t.Fatalf("wholesale replace = %+v", after)
	}

}

// TestMySQLNotificationChannelsWebhookEncryption proves webhook URLs are
// encrypted at rest (the address column is ciphertext), decrypt transparently
// on read, and that legacy plaintext rows keep working.
func TestMySQLNotificationChannelsWebhookEncryption(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	store.encryptionKey = bytes.Repeat([]byte("k"), 32)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_enc_1', ?, 'enc@test.local', 'Enc', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	const webhook = "https://hooks.example.com/T/B/supersecret"
	if err := store.ReplaceNotificationChannels(ctx, tenant, "user_enc_1", NotificationChannels{
		Webhooks: []string{webhook},
	}); err != nil {
		t.Fatal(err)
	}

	// At rest the address is ciphertext (prefixed, no plaintext token).
	var stored string
	if err := db.QueryRowContext(ctx, `
		SELECT address FROM notification_channels WHERE user_id = 'user_enc_1' AND channel_type = 'webhook'
	`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stored, encryptedSecretPrefix) || strings.Contains(stored, "supersecret") {
		t.Fatalf("webhook not encrypted at rest: %s", stored)
	}

	// The user-scoped read path decrypts transparently.
	got, err := store.GetNotificationChannels(ctx, tenant, "user_enc_1")
	if err != nil || len(got.Webhooks) != 1 || got.Webhooks[0] != webhook {
		t.Fatalf("get decrypted = %+v err=%v", got, err)
	}
	// A legacy plaintext row (written before encryption) reads back unchanged.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO notification_channels (id, tenant_id, user_id, channel_type, address)
		VALUES ('ch_legacy', ?, 'user_enc_1', 'webhook', 'https://legacy.example.com/plain')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetNotificationChannels(ctx, tenant, "user_enc_1")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range after.Webhooks {
		if w == "https://legacy.example.com/plain" {
			found = true
		}
	}
	if !found {
		t.Fatalf("legacy plaintext webhook not read back: %+v", after.Webhooks)
	}
}
