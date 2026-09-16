package server

import (
	"net/http"
	"testing"

	"github.com/cloudcache/watchdog/internal/opjob"
)

func TestPlatformOperationRoutesAreRegistered(t *testing.T) {
	router := (&Server{}).newRouter()
	want := map[string]bool{
		http.MethodGet + " /api/v1/audit-logs":                                       false,
		http.MethodGet + " /api/v1/operation-jobs":                                   false,
		http.MethodGet + " /api/v1/operation-jobs/:id":                               false,
		http.MethodPost + " /api/v1/operation-jobs/:id/actions/cancel":               false,
		http.MethodGet + " /api/v1/flow/storage/policies":                            false,
		http.MethodPost + " /api/v1/flow/storage/policies":                           false,
		http.MethodGet + " /api/v1/flow/storage/policies/:id":                        false,
		http.MethodPatch + " /api/v1/flow/storage/policies/:id":                      false,
		http.MethodDelete + " /api/v1/flow/storage/policies/:id":                     false,
		http.MethodPost + " /api/v1/flow/storage/policies/:id/actions/publish":       false,
		http.MethodPost + " /api/v1/flow/storage/policies/:id/actions/retire":        false,
		http.MethodGet + " /api/v1/flow/storage/partitions":                          false,
		http.MethodGet + " /api/v1/flow/storage/partitions/:date/delete-readiness":   false,
		http.MethodGet + " /api/v1/flow/storage/watermarks":                          false,
		http.MethodGet + " /api/v1/flow/storage/deletion-receipts":                   false,
		http.MethodGet + " /api/v1/flow/storage/backup-evidence":                     false,
		http.MethodPost + " /api/v1/flow/storage/backup-evidence":                    false,
		http.MethodGet + " /api/v1/flow/storage/backup-evidence/:id":                 false,
		http.MethodPost + " /api/v1/flow/storage/backup-evidence/:id/actions/revoke": false,
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
