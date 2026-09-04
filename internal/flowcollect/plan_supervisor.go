package flowcollect

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrActivePlanExpired = errors.New("active flow plan expired")

type planRefreshFailure struct {
	stage  string
	code   string
	detail string
	err    error
}

func (f planRefreshFailure) Error() string { return f.err.Error() }
func (f planRefreshFailure) Unwrap() error { return f.err }

func newPlanRefreshFailure(stage, code, detail string, err error) error {
	if err == nil {
		return nil
	}
	var existing planRefreshFailure
	if errors.As(err, &existing) {
		return err
	}
	return planRefreshFailure{stage: stage, code: code, detail: detail, err: err}
}

type PlanDependencyValidator func([]*Registry) error

type PlanSupervisor struct {
	RefreshInterval time.Duration
	Plans           *PlanHistory
	WAL             *WAL
	Runner          *Runner
	Metrics         *Metrics
	Runtime         *RuntimeState
	Validate        PlanDependencyValidator
	Wake            <-chan struct{}
	OnActivated     func(*Registry)
	OnError         func(error)
	now             func() time.Time
}

func NewPlanSupervisor(refreshInterval time.Duration, plans *PlanHistory, wal *WAL, runner *Runner, metrics *Metrics, runtime *RuntimeState, validate PlanDependencyValidator) (*PlanSupervisor, error) {
	if refreshInterval <= 0 || plans == nil || wal == nil || runner == nil || metrics == nil || runtime == nil || validate == nil {
		return nil, errors.New("plan refresh interval, history, WAL, runner, metrics, runtime, and validator are required")
	}
	return &PlanSupervisor{RefreshInterval: refreshInterval, Plans: plans, WAL: wal, Runner: runner, Metrics: metrics, Runtime: runtime, Validate: validate, now: time.Now}, nil
}

func (s *PlanSupervisor) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	for {
		now := s.now()
		if err := s.Refresh(now); err != nil {
			active := s.Runner.ActiveRegistry()
			if active == nil || !registryValidAt(active, now) {
				return fmt.Errorf("%w: %v", ErrActivePlanExpired, err)
			}
			if s.OnError != nil {
				s.OnError(err)
			}
		}
		active := s.Runner.ActiveRegistry()
		if active == nil {
			return ErrActivePlanExpired
		}
		delay := s.RefreshInterval
		if untilExpiry := active.plan.ExpiresAt.Sub(s.now()); untilExpiry < delay {
			delay = untilExpiry
		}
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil
		case <-s.Wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (s *PlanSupervisor) Refresh(now time.Time) error {
	result, err := s.Plans.Refresh(now, s.pendingRegistryVersions, s.Validate)
	if err == nil {
		active := s.Plans.Active()
		if activateErr := s.Runner.ActivateRegistry(active); activateErr != nil {
			err = newPlanRefreshFailure("activate", "DATA_PLANE_ACTIVATE_FAILED", "plan could not be activated in the data plane", fmt.Errorf("publish refreshed flow plan to data plane: %w", activateErr))
		} else {
			s.Metrics.PlanRefreshSuccesses.Add(1)
			if result.Changed {
				s.Metrics.PlanRefreshChanges.Add(1)
			}
			s.Metrics.PlanHistoryPruned.Add(uint64(result.Pruned))
			s.Metrics.PlanHistoryEntries.Store(int64(len(s.Plans.Revisions())))
			s.Runtime.observePlan(nil, active, s.Plans.UsedLKG(), now)
			if s.OnActivated != nil {
				s.OnActivated(active)
			}
			return nil
		}
	}
	s.Metrics.PlanRefreshFailures.Add(1)
	s.Runtime.observePlan(err, s.Runner.ActiveRegistry(), s.Plans.UsedLKG(), now)
	return err
}

func (s *PlanSupervisor) pendingRegistryVersions() (map[uint64]struct{}, error) {
	referenced, err := s.WAL.PendingRegistryVersions()
	if err != nil {
		return nil, err
	}
	for _, revision := range s.Runner.AdmissibleRegistryVersions() {
		referenced[revision] = struct{}{}
	}
	return referenced, nil
}
