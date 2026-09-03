package watchdog

import (
	"context"
	"log"
	"time"
)

type DiscoveryJob struct {
	ID          ID
	TenantID    ID
	DeviceID    ID
	Reason      string
	Status      string
	DueAt       time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type DiscoveryJobRepository interface {
	EnqueueDiscoveryJob(ctx context.Context, tenantID, deviceID ID, reason string) error
	ListDueDiscoveryJobs(ctx context.Context, limit int, now time.Time) ([]DiscoveryJob, error)
	MarkDiscoveryJobRunning(ctx context.Context, jobID ID) error
	MarkDiscoveryJobCompleted(ctx context.Context, jobID ID, lastError string) error
}

const (
	discoveryJobPending   = "pending"
	discoveryJobRunning   = "running"
	discoveryJobCompleted = "completed"
	discoveryJobFailed    = "failed"
)

type DiscoveryScheduler struct {
	Jobs      DiscoveryJobRepository
	Network   NetworkRepository
	Targets   TargetRepository
	SNMP      SNMPRepository
	Discovery SNMPDeviceDiscoverer
	Collector SNMPCollectorRepository
}

func (s DiscoveryScheduler) RunLoop(ctx context.Context, interval time.Duration, limit int) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if limit <= 0 {
		limit = 10
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.runOnce(ctx, limit)
		}
	}
}

func (s DiscoveryScheduler) runOnce(ctx context.Context, limit int) {
	jobs, err := s.Jobs.ListDueDiscoveryJobs(ctx, limit, time.Now().UTC())
	if err != nil {
		log.Printf("discovery scheduler: list due jobs failed: %v", err)
		return
	}
	for _, job := range jobs {
		if err := s.Jobs.MarkDiscoveryJobRunning(ctx, job.ID); err != nil {
			log.Printf("discovery scheduler: mark running job %s failed: %v", job.ID, err)
			continue
		}
		err := s.runDiscovery(ctx, job)
		lastError := ""
		if err != nil {
			lastError = err.Error()
			log.Printf("discovery scheduler: job %s device %s failed: %v", job.ID, job.DeviceID, err)
		}
		if err := s.Jobs.MarkDiscoveryJobCompleted(ctx, job.ID, lastError); err != nil {
			log.Printf("discovery scheduler: mark completed job %s failed: %v", job.ID, err)
		}
	}
}

func (s DiscoveryScheduler) runDiscovery(ctx context.Context, job DiscoveryJob) error {
	device, err := s.Network.GetDevice(ctx, job.TenantID, job.DeviceID)
	if err != nil {
		return err
	}
	target, err := s.Targets.GetTarget(ctx, job.TenantID, device.TargetID)
	if err != nil {
		return err
	}
	profile, err := s.SNMP.GetSNMPProfile(ctx, job.TenantID, device.SNMPProfileID)
	if err != nil {
		return err
	}
	profile = ApplyDeviceSNMPOverrides(profile, device)
	result, err := s.Discovery.Discover(ctx, SNMPDiscoveryEngineRequest{
		TenantID: job.TenantID,
		TargetID: device.TargetID,
		Target:   SNMPCollectorTarget{Host: target.Host, Port: normalizeSNMPPort(device.SNMPPort)},
		Device:   device,
		Profile:  profile,
	})
	if err != nil {
		return err
	}
	report, err := ImportSNMPCollectorDiscoveryResult(ctx, s.Network, s.Collector, job.TenantID, device, result)
	if err != nil {
		return err
	}
	return promoteDiscoveredTargetName(ctx, s.Targets, job.TenantID, device.TargetID, report.Device.SysName)
}
