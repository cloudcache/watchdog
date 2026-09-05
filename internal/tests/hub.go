//go:build testing

// Package tests provides helpers for testing the application.
package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cloudcache/watchdog/internal/alerts"
	"github.com/cloudcache/watchdog/internal/hub"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/stretchr/testify/assert"

	_ "github.com/pocketbase/pocketbase/migrations"
)

// mockChannelReader stands in for the MySQL notification-channel source in hub
// integration tests, so alert delivery no longer depends on the retired
// PocketBase user_settings collection. Tests populate it per user.
type mockChannelReader struct {
	mu       sync.Mutex
	channels map[string]struct {
		emails   []string
		webhooks []string
	}
}

func (m *mockChannelReader) ChannelsForExternalSubject(_ context.Context, _, externalSubject string) ([]string, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.channels[externalSubject]
	return c.emails, c.webhooks, nil
}

// NotificationChannelReader lets alert managers created against this test hub
// adopt the mock reader at construction (channelReaderProvider).
func (t *TestHub) NotificationChannelReader() alerts.NotificationChannelReader {
	return t.channels
}

// TestHub is a wrapper hub instance used for testing.
type TestHub struct {
	core.App
	*tests.TestApp
	*hub.Hub
	channels *mockChannelReader
}

// SetNotificationChannels configures the emails/webhooks alert delivery will
// see for a user (keyed by PocketBase id), replacing the old user_settings row.
func (t *TestHub) SetNotificationChannels(userID string, emails, webhooks []string) {
	t.channels.mu.Lock()
	defer t.channels.mu.Unlock()
	t.channels.channels[userID] = struct {
		emails   []string
		webhooks []string
	}{emails: emails, webhooks: webhooks}
}

// NewTestHub creates and initializes a test application instance.
//
// It is the caller's responsibility to call app.Cleanup() when the app is no longer needed.
func NewTestHub(optTestDataDir ...string) (*TestHub, error) {
	var testDataDir string
	if len(optTestDataDir) > 0 {
		testDataDir = optTestDataDir[0]
	}

	return NewTestHubWithConfig(core.BaseAppConfig{
		DataDir:       testDataDir,
		EncryptionEnv: "pb_test_env",
	})
}

// NewTestHubWithConfig creates and initializes a test application instance
// from the provided config.
//
// If config.DataDir is not set it fallbacks to the default internal test data directory.
//
// config.DataDir is cloned for each new test application instance.
//
// It is the caller's responsibility to call app.Cleanup() when the app is no longer needed.
func NewTestHubWithConfig(config core.BaseAppConfig) (*TestHub, error) {
	testApp, err := tests.NewTestAppWithConfig(config)
	if err != nil {
		return nil, err
	}

	h := hub.NewHub(testApp)

	channels := &mockChannelReader{channels: map[string]struct {
		emails   []string
		webhooks []string
	}{}}
	h.SetNotificationChannelReader(channels)

	t := &TestHub{
		App:      testApp,
		TestApp:  testApp,
		Hub:      h,
		channels: channels,
	}

	return t, nil
}

// Helper function to create a test user for config tests
func CreateUser(app core.App, email string, password string) (*core.Record, error) {
	userCollection, err := app.FindCachedCollectionByNameOrId("users")
	if err != nil {
		return nil, err
	}

	user := core.NewRecord(userCollection)
	user.Set("email", email)
	user.Set("password", password)

	return user, app.Save(user)
}

// Helper function to create a test superuser for config tests
func CreateSuperuser(app core.App, email string, password string) (*core.Record, error) {
	superusersCollection, _ := app.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
	superuser := core.NewRecord(superusersCollection)
	superuser.Set("email", email)
	superuser.Set("password", password)

	return superuser, app.Save(superuser)
}

func CreateUserWithRole(app core.App, email string, password string, roleName string) (*core.Record, error) {
	user, err := CreateUser(app, email, password)
	if err != nil {
		return nil, err
	}

	user.Set("role", roleName)
	return user, app.Save(user)
}

// Helper function to create a test record
func CreateRecord(app core.App, collectionName string, fields map[string]any) (*core.Record, error) {
	collection, err := app.FindCachedCollectionByNameOrId(collectionName)
	if err != nil {
		return nil, err
	}

	record := core.NewRecord(collection)
	record.Load(fields)

	return record, app.Save(record)
}

func ClearCollection(t testing.TB, app core.App, collectionName string) error {
	_, err := app.DB().NewQuery(fmt.Sprintf("DELETE from %s", collectionName)).Execute()
	recordCount, err := app.CountRecords(collectionName)
	assert.EqualValues(t, recordCount, 0, "should have 0 records after clearing")
	return err
}

func (h *TestHub) Cleanup() {
	h.GetAlertManager().Stop()
	h.GetSystemManager().RemoveAllSystems()
	h.TestApp.Cleanup()
}

func CreateSystems(app core.App, count int, userId string, status string) ([]*core.Record, error) {
	systems := make([]*core.Record, 0, count)
	for i := range count {
		system, err := CreateRecord(app, "systems", map[string]any{
			"name":  fmt.Sprintf("test-system-%d", i),
			"host":  fmt.Sprintf("127.0.0.%d", i),
			"port":  "33914",
			"users": []string{userId},
		})
		if err != nil {
			return nil, err
		}
		system.Set("status", status)
		err = app.SaveNoValidate(system)
		if err != nil {
			return nil, err
		}
		systems = append(systems, system)
	}
	return systems, nil
}

// GetHubWithUser creates a test hub with a test user and user settings
func GetHubWithUser(t *testing.T) (*TestHub, *core.Record) {
	hub, err := NewTestHub(t.TempDir())
	assert.NoError(t, err)
	hub.StartHub()

	// Manually initialize the system manager to bind event hooks
	err = hub.GetSystemManager().Initialize()
	assert.NoError(t, err)

	// Create a test user
	user, err := CreateUser(hub, "test@example.com", "password")
	assert.NoError(t, err)

	// Configure the user's notification channel (required for alert delivery).
	hub.SetNotificationChannels(user.Id, []string{"test@example.com"}, nil)

	return hub, user
}
