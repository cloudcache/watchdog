package hub_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	hubpkg "github.com/cloudcache/watchdog/internal/hub"
	watchdogTests "github.com/cloudcache/watchdog/internal/tests"
)

func TestPocketBaseIdentityAuthenticatorAcceptsOnlyUserAuthTokens(t *testing.T) {
	app, user := watchdogTests.GetHubWithUser(t)
	defer app.Cleanup()
	authenticate := hubpkg.NewPocketBaseIdentityAuthenticator(app)

	userToken, err := user.NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+userToken)
	identity, err := authenticate(req)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Provider != "pocketbase" || identity.Subject != user.Id {
		t.Fatalf("identity = %#v", identity)
	}

	superuser, err := watchdogTests.CreateSuperuser(app, "platform-admin@example.com", "password123")
	if err != nil {
		t.Fatal(err)
	}
	superuserToken, err := superuser.NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", superuserToken)
	if _, err := authenticate(req); err == nil {
		t.Fatal("expected superuser token to be rejected by platform identity adapter")
	}
}
