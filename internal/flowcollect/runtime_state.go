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
	kafka             [kafkaTopicSlots]componentRuntimeState
	collect           componentRuntimeState
	qualityJournal    componentRuntimeState
	qualityCheckpoint componentRuntimeState
}

type ComponentRuntimeSnapshot struct {
	Healthy       bool
	LastSuccessAt int64
	LastFailureAt int64
}

type RuntimeSnapshot struct {
	Running           bool
	StartedAt         int64
	Kafka             [kafkaTopicSlots]ComponentRuntimeSnapshot
	Collect           ComponentRuntimeSnapshot
	QualityJournal    ComponentRuntimeSnapshot
	QualityCheckpoint ComponentRuntimeSnapshot
}

func NewRuntimeState() *RuntimeState {
	state := &RuntimeState{}
	for index := range state.kafka {
		state.kafka[index].healthy.Store(true)
	}
	state.collect.healthy.Store(true)
	state.qualityJournal.healthy.Store(true)
	state.qualityCheckpoint.healthy.Store(true)
	return state
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

func (s *RuntimeState) observeQualityJournal(err error, now time.Time) {
	s.qualityJournal.observe(err, now)
}

func (s *RuntimeState) observeQualityCheckpoint(err error, now time.Time) {
	s.qualityCheckpoint.observe(err, now)
}

func (s *RuntimeState) Snapshot() RuntimeSnapshot {
	snapshot := RuntimeSnapshot{Running: s.running.Load(), StartedAt: s.startedAt.Load()}
	for index := range s.kafka {
		snapshot.Kafka[index] = s.kafka[index].snapshot()
	}
	snapshot.Collect = s.collect.snapshot()
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
