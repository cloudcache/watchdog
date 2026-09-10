package server

import (
	"net/http"
	"testing"

	"github.com/cloudcache/watchdog/internal/opjob"
)

func TestPlatformOperationRoutesAreRegistered(t *testing.T) {
	router := (&Server{}).newRouter()
	want := map[string]bool{
		http.MethodGet + " /api/v1/audit-logs":                         false,
		http.MethodGet + " /api/v1/operation-jobs":                     false,
		http.MethodGet + " /api/v1/operation-jobs/:id":                 false,
		http.MethodPost + " /api/v1/operation-jobs/:id/actions/cancel": false,
	}
	for _, route := range router.Routes() {
		key := route.Method + " " + route.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for route, found := range want {
		if !found {
			t.Fatalf("platform operation route is not registered: %s", route)
		}
	}
}

func TestValidOperationJobStatus(t *testing.T) {
	for _, status := range []string{
		opjob.StatusQueued,
		opjob.StatusRunning,
		opjob.StatusCancelRequested,
		opjob.StatusSucceeded,
		opjob.StatusFailed,
		opjob.StatusCanceled,
	} {
		if !validOperationJobStatus(status) {
			t.Fatalf("valid status %q rejected", status)
		}
	}
	for _, status := range []string{"", "pending", "done", "RUNNING"} {
		if validOperationJobStatus(status) {
			t.Fatalf("invalid status %q accepted", status)
		}
	}
}
