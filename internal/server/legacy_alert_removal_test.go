package server

import (
	"strings"
	"testing"
)

func TestLegacyPBAlertRoutesAreNotRegistered(t *testing.T) {
	router := (&Server{}).newRouter()
	for _, route := range router.Routes() {
		path := strings.ToLower(route.Path)
		if strings.Contains(path, "notification-channels") ||
			strings.Contains(path, "quiet-hours") ||
			strings.Contains(path, "alerts-history") ||
			path == "/api/v1/alerts" {
			t.Fatalf("legacy PB route is still registered: %s %s", route.Method, route.Path)
		}
	}
}
