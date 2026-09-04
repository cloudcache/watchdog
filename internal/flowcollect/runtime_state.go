package flowcollect

import (
	"sync/atomic"
	"time"
)

type componentRuntimeState struct {
	healthy       atomic.Bool
	lastSuccessAt atomic.Int64
	lastFailureAt atomic.Int64
}

type RuntimeState struct {
	running           atomic.Bool
	startedAt         atomic.Int64
	plan              planRuntimeState
	controlPlane      controlPlaneRuntimeState
	kafka             [kafkaTopicSlots]componentRuntimeState
	collect           componentRuntimeState
	attemptJournal    componentRuntimeState
	attemptCheckpoint componentRuntimeState
	qualityJournal    componentRuntimeState
	qualityCheckpoint componentRuntimeState
}

type planRuntimeState struct {
	componentRuntimeState
	accepting      atomic.Bool
	activeRevision atomic.Uint64
	expiresAt      atomic.Int64
	usedLKG        atomic.Bool
}

type controlPlaneRuntimeState struct {
	componentRuntimeState
	enabled atomic.Bool
}

type ComponentRuntimeSnapshot struct {
	Healthy       bool
	LastSuccessAt int64
	LastFailureAt int64
}

type RuntimeSnapshot struct {
	Running           bool
	StartedAt         int64
	Plan              PlanRuntimeSnapshot
	ControlPlane      ControlPlaneRuntimeSnapshot
	Kafka             [kafkaTopicSlots]ComponentRuntimeSnapshot
	Collect           ComponentRuntimeSnapshot
	AttemptJournal    ComponentRuntimeSnapshot
	AttemptCheckpoint ComponentRuntimeSnapshot
	QualityJournal    ComponentRuntimeSnapshot
	QualityCheckpoint ComponentRuntimeSnapshot
}

type PlanRuntimeSnapshot struct {
	Healthy        bool
	Accepting      bool
	ActiveRevision uint64
	ExpiresAt      int64
	UsedLKG        bool
	LastSuccessAt  int64
	LastFailureAt  int64
}

type ControlPlaneRuntimeSnapshot struct {
	Enabled       bool
	Healthy       bool
	LastSuccessAt int64
	LastFailureAt int64
}

func NewRuntimeState() *RuntimeState {
	state := &RuntimeState{}
	for index := range state.kafka {
		state.kafka[index].healthy.Store(true)
	}
	state.collect.healthy.Store(true)
	state.attemptJournal.healthy.Store(true)
	state.attemptCheckpoint.healthy.Store(true)
	state.qualityJournal.healthy.Store(true)
	state.qualityCheckpoint.healthy.Store(true)
	return state
}

func (s *RuntimeState) enableControlPlane() {
	s.controlPlane.enabled.Store(true)
	s.controlPlane.healthy.Store(false)
}

func (s *RuntimeState) observeControlPlane(err error, now time.Time) {
	s.controlPlane.observe(err, now)
}

func (s *RuntimeState) start(now time.Time) {
	s.startedAt.Store(now.Unix())
	s.running.Store(true)
}

func (s *RuntimeState) stop() {
	s.running.Store(false)
}

func (s *RuntimeState) observeKafka(topic kafkaTopic, err error, now time.Time) {
	s.kafka[int(topic)].observe(err, now)
}

func (s *RuntimeState) observeCollect(err error, now time.Time) {
	s.collect.observe(err, now)
}

func (s *RuntimeState) observeAttemptJournal(err error, now time.Time) {
	s.attemptJournal.observe(err, now)
}

func (s *RuntimeState) observeAttemptCheckpoint(err error, now time.Time) {
	s.attemptCheckpoint.observe(err, now)
}

func (s *RuntimeState) observeQualityJournal(err error, now time.Time) {
	s.qualityJournal.observe(err, now)
}

func (s *RuntimeState) observeQualityCheckpoint(err error, now time.Time) {
	s.qualityCheckpoint.observe(err, now)
}

func (s *RuntimeState) observePlan(err error, registry *Registry, usedLKG bool, now time.Time) {
	s.plan.observe(err, now)
	s.plan.usedLKG.Store(usedLKG)
	if registry == nil {
		s.plan.accepting.Store(false)
		s.plan.activeRevision.Store(0)
		s.plan.expiresAt.Store(0)
		return
	}
	s.plan.activeRevision.Store(registry.plan.Revision)
	s.plan.expiresAt.Store(registry.plan.ExpiresAt.Unix())
	s.plan.accepting.Store(registryValidAt(registry, now))
}

func (s *RuntimeState) Snapshot() RuntimeSnapshot {
	snapshot := RuntimeSnapshot{Running: s.running.Load(), StartedAt: s.startedAt.Load()}
	planComponent := s.plan.componentRuntimeState.snapshot()
	snapshot.Plan = PlanRuntimeSnapshot{Healthy: planComponent.Healthy, Accepting: s.plan.accepting.Load(), ActiveRevision: s.plan.activeRevision.Load(), ExpiresAt: s.plan.expiresAt.Load(), UsedLKG: s.plan.usedLKG.Load(), LastSuccessAt: planComponent.LastSuccessAt, LastFailureAt: planComponent.LastFailureAt}
	controlPlaneComponent := s.controlPlane.componentRuntimeState.snapshot()
	snapshot.ControlPlane = ControlPlaneRuntimeSnapshot{Enabled: s.controlPlane.enabled.Load(), Healthy: controlPlaneComponent.Healthy, LastSuccessAt: controlPlaneComponent.LastSuccessAt, LastFailureAt: controlPlaneComponent.LastFailureAt}
	for index := range s.kafka {
		snapshot.Kafka[index] = s.kafka[index].snapshot()
	}
	snapshot.Collect = s.collect.snapshot()
	snapshot.AttemptJournal = s.attemptJournal.snapshot()
	snapshot.AttemptCheckpoint = s.attemptCheckpoint.snapshot()
	snapshot.QualityJournal = s.qualityJournal.snapshot()
	snapshot.QualityCheckpoint = s.qualityCheckpoint.snapshot()
	return snapshot
}

func (s *componentRuntimeState) observe(err error, now time.Time) {
	if err == nil {
		s.lastSuccessAt.Store(now.Unix())
		s.healthy.Store(true)
		return
	}
	s.lastFailureAt.Store(now.Unix())
	s.healthy.Store(false)
}

func (s *componentRuntimeState) snapshot() ComponentRuntimeSnapshot {
	return ComponentRuntimeSnapshot{Healthy: s.healthy.Load(), LastSuccessAt: s.lastSuccessAt.Load(), LastFailureAt: s.lastFailureAt.Load()}
}
