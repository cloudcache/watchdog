package watchdog

import (
	"context"
	"fmt"
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
	ListDueDiscoveryJobs(ctx context.Context, tenantID ID, limit int) ([]DiscoveryJob, error)
	MarkDiscoveryJobRunning(ctx context.Context, jobID ID) (bool, error)
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

func (s DiscoveryScheduler) RunLoop(ctx context.Context, tenantID ID, interval time.Duration, limit int) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if limit <= 0 {
		limit = 10
	}
	s.runOnce(ctx, tenantID, limit)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.runOnce(ctx, tenantID, limit)
		}
	}
}

func (s DiscoveryScheduler) runOnce(ctx context.Context, tenantID ID, limit int) {
	jobs, err := s.Jobs.ListDueDiscoveryJobs(ctx, tenantID, limit)
	if err != nil {
		log.Printf("discovery scheduler: list due jobs failed: %v", err)
		return
	}
	for _, job := range jobs {
		claimed, err := s.Jobs.MarkDiscoveryJobRunning(ctx, job.ID)
		if err != nil {
			log.Printf("discovery scheduler: mark running job %s failed: %v", job.ID, err)
			continue
		}
		if !claimed {
			continue
		}
		err = s.runDiscovery(ctx, job)
		status := "up"
		if err != nil {
			status = "down"
		}
		if statusErr := s.updateTargetStatus(ctx, job, status); statusErr != nil {
			if err == nil {
				err = fmt.Errorf("update target discovery status: %w", statusErr)
			} else {
				log.Printf("discovery scheduler: update target status for device %s failed: %v", job.DeviceID, statusErr)
			}
		}
		lastError := ""
		if err != nil {
			lastError = err.Error()
			log.Printf("discovery scheduler: job %s device %s failed: %v", job.ID, job.DeviceID, err)
			if eventErr := recordSNMPDiscoveryFailure(ctx, s.Collector, job.TenantID, job.DeviceID, lastError); eventErr != nil {
				log.Printf("discovery scheduler: record failure for device %s failed: %v", job.DeviceID, eventErr)
			}
		}
		if err := s.Jobs.MarkDiscoveryJobCompleted(ctx, job.ID, lastError); err != nil {
			log.Printf("discovery scheduler: mark completed job %s failed: %v", job.ID, err)
		}
	}
}

func (s DiscoveryScheduler) updateTargetStatus(ctx context.Context, job DiscoveryJob, status string) error {
	device, err := s.Network.GetDevice(ctx, job.TenantID, job.DeviceID)
	if err != nil {
		return err
	}
	return updateNetworkTargetStatus(ctx, s.Targets, job.TenantID, device.TargetID, status)
}

func updateNetworkTargetStatus(ctx context.Context, targets TargetRepository, tenantID, targetID ID, status string) error {
	if targets == nil || targetID == "" {
		return nil
	}
	target, err := targets.GetTarget(ctx, tenantID, targetID)
	if err != nil {
		return err
	}
	if target.Status == "paused" || target.Status == status {
		return nil
	}
	target.Status = status
	_, err = targets.UpdateTarget(ctx, target)
	return err
}

func recordSNMPDiscoveryFailure(ctx context.Context, collector SNMPCollectorRepository, tenantID, deviceID ID, message string) error {
	if collector == nil {
		return nil
	}
	return collector.CreateSNMPEvent(ctx, SNMPEvent{
		TenantID:   tenantID,
		DeviceID:   deviceID,
		EntityType: SNMPCollectorEntityDevice,
		EntityID:   deviceID,
		Source:     "discovery",
		Severity:   "warning",
		EventType:  "discovery_failed",
		Message:    message,
		OccurredAt: time.Now().UTC(),
	})
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
