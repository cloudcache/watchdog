package watchdog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type schedulerJobRepository struct {
	jobs           []DiscoveryJob
	listedTenantID ID
	runningJobID   ID
	completedJobID ID
	completedError string
	cancel         context.CancelFunc
}

func (r *schedulerJobRepository) EnqueueDiscoveryJob(context.Context, ID, ID, string) error {
	return nil
}

func (r *schedulerJobRepository) ListDueDiscoveryJobs(_ context.Context, tenantID ID, _ int) ([]DiscoveryJob, error) {
	r.listedTenantID = tenantID
	return r.jobs, nil
}

func (r *schedulerJobRepository) MarkDiscoveryJobRunning(_ context.Context, jobID ID) (bool, error) {
	r.runningJobID = jobID
	return true, nil
}

func (r *schedulerJobRepository) MarkDiscoveryJobCompleted(_ context.Context, jobID ID, lastError string) error {
	r.completedJobID = jobID
	r.completedError = lastError
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func TestDiscoverySchedulerRunsImmediatelyAndRecordsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := &schedulerJobRepository{
		jobs:   []DiscoveryJob{{ID: "job-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		cancel: cancel,
	}
	network := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
	}}}
	targets := &fakeTargetRepository{targets: []Target{{
		ID: "target-a", TenantID: "tenant-a", Kind: TargetKindNetwork, Host: "10.0.0.1", Status: "up",
	}}}
	collector := &fakeSNMPCollectorRepository{}
	scheduler := DiscoveryScheduler{
		Jobs:      jobs,
		Network:   network,
		Targets:   targets,
		SNMP:      &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: SNMPVersion2c}}},
		Discovery: &fakeSNMPInterfaceDiscoverer{err: errors.New("request timeout")},
		Collector: collector,
	}

	err := scheduler.RunLoop(ctx, "tenant-a", time.Hour, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunLoop error = %v", err)
	}
	if jobs.listedTenantID != "tenant-a" || jobs.runningJobID != "job-a" || jobs.completedJobID != "job-a" {
		t.Fatalf("job lifecycle = %#v", jobs)
	}
	if !strings.Contains(jobs.completedError, "request timeout") {
		t.Fatalf("completed error = %q", jobs.completedError)
	}
	if targets.updated.Status != "down" {
		t.Fatalf("target status = %q", targets.updated.Status)
	}
	if len(collector.events) != 1 || collector.events[0].EventType != "discovery_failed" {
		t.Fatalf("events = %#v", collector.events)
	}
}
