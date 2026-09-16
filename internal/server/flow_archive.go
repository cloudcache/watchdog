package server

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	flowArchiveScanInterval = 5 * time.Minute
	flowLateCheckInterval   = 6 * time.Hour
	flowArchiveScanBudget   = 7
)

// startFlowArchive runs the installation-wide cold lifecycle on the shared
// operation_jobs engine. Retention values come only from the published MySQL
// policy; these constants bound scheduler work and never define retention.
func (s *Server) startFlowArchive() error {
	if s.clickHouse == nil {
		return nil
	}
	if s.db == nil || s.jobs == nil {
		return errors.New("Flow archive management dependencies are not initialized")
	}
	runner, err := flowch.NewRollupRunner(s.clickHouse)
	if err != nil {
		return err
	}
	store := flowlifecycle.NewStore(s.db)
	s.flowLifecycle = store
	workerContext, cancel := context.WithCancel(context.Background())
	s.flowArchiveCancel = cancel
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.ArchiveJobType, Owner: "watchdog-server/flow-archive",
		Handler: flowlifecycle.NewArchiveHandler(store, runner), Logf: log.Printf,
	}
	scheduler := &flowlifecycle.ArchiveScheduler{
		Store: store, Jobs: s.jobs, Runner: runner,
		Interval: flowArchiveScanInterval, LateCheckEvery: flowLateCheckInterval,
		MaxPartitions: flowArchiveScanBudget, MaxLateChecks: flowArchiveScanBudget, Logf: log.Printf,
	}
	go worker.Run(workerContext)
	go scheduler.Run(workerContext)
	return nil
}

// applyFlowStorageBoundary makes raw facts authoritative after the largest
// contiguous reconciled UTC-day prefix. One-minute queries always use raw facts
// because Storage V2 intentionally has no one-minute archive.
func (s *Server) applyFlowStorageBoundary(ctx context.Context, request *flowquery.Request) error {
	if request == nil || s.flowLifecycle == nil {
		return nil
	}
	request.StorageV2 = true
	request.ArchiveThrough = request.From.UTC()
	if request.Bucket == flowquery.BucketOneMinute {
		return nil
	}
	boundary, err := s.flowLifecycle.ArchiveThrough(ctx, request.From, request.To)
	if err != nil {
		return err
	}
	request.ArchiveThrough = boundary
	return nil
}

func (s *Server) applyFlowOverseasStorageBoundary(ctx context.Context, request *flowquery.OverseasRequest) error {
	if request == nil || s.flowLifecycle == nil {
		return nil
	}
	request.StorageV2 = true
	request.ArchiveThrough = request.From.UTC()
	if request.Bucket == flowquery.BucketOneMinute {
		return nil
	}
	boundary, err := s.flowLifecycle.ArchiveThrough(ctx, request.From, request.To)
	if err != nil {
		return err
	}
	request.ArchiveThrough = boundary
	return nil
}
