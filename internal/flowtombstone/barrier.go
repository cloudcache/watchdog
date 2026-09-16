// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowtombstone defines the immutable raw-flow deletion barrier shared
// by the management plane and flow workers. The worker hot path reads one
// atomic pointer and performs UTC-date comparisons only.
package flowtombstone

import (
	"errors"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const SchemaVersion uint16 = 1

// Revisions occupy the lower 63 bits because ClickHouse receipt generations
// reserve the high bit for lifecycle dispositions that must outrank an older
// ordinary ingest receipt for the same Kafka coordinate.
const MaxRevision uint64 = 1<<63 - 1

var ErrInvalidBarrier = errors.New("raw Flow deletion barrier is invalid")

// Barrier is an immutable publication. DeletedThrough protects every UTC day
// up to and including that date. ExceptionDays protects non-contiguous days
// above DeletedThrough until intervening deletions let them be compacted.
type Barrier struct {
	SchemaVersion  uint16    `json:"schema_version"`
	ID             string    `json:"id"`
	Revision       uint64    `json:"revision"`
	DeletedThrough string    `json:"deleted_through"`
	ExceptionDays  []string  `json:"exception_days"`
	PublishedAt    time.Time `json:"published_at"`
}

type Decision struct {
	Revision       uint64
	DeletedThrough string
	EventDay       string
	ExceptionDay   bool
}

func (b Barrier) Validate() error {
	if b.SchemaVersion != SchemaVersion || b.ID == "" || b.Revision == 0 || b.Revision > MaxRevision || b.PublishedAt.IsZero() {
		return ErrInvalidBarrier
	}
	through, err := parseDay(b.DeletedThrough)
	if err != nil {
		return ErrInvalidBarrier
	}
	previous := ""
	for _, value := range b.ExceptionDays {
		day, err := parseDay(value)
		if err != nil || !day.After(through) || value <= previous {
			return ErrInvalidBarrier
		}
		previous = value
	}
	return nil
}

func (b Barrier) Covers(eventTime time.Time) (Decision, bool) {
	if b.Validate() != nil || eventTime.IsZero() {
		return Decision{}, false
	}
	eventDay := eventTime.UTC().Format(time.DateOnly)
	if eventDay <= b.DeletedThrough {
		return Decision{Revision: b.Revision, DeletedThrough: b.DeletedThrough, EventDay: eventDay}, true
	}
	position, found := slices.BinarySearch(b.ExceptionDays, eventDay)
	_ = position
	if found {
		return Decision{Revision: b.Revision, DeletedThrough: b.DeletedThrough, EventDay: eventDay, ExceptionDay: true}, true
	}
	return Decision{}, false
}

// Advance returns the next canonical publication contents. The first deleted
// day becomes the inclusive cutoff: older event-time facts are even later and
// must also be quarantined. Gaps above the cutoff are retained explicitly.
func Advance(current *Barrier, day time.Time, id string, revision uint64, publishedAt time.Time) (Barrier, error) {
	day = day.UTC()
	if day.IsZero() || day != time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC) || id == "" || revision == 0 || revision > MaxRevision || publishedAt.IsZero() {
		return Barrier{}, ErrInvalidBarrier
	}
	result := Barrier{SchemaVersion: SchemaVersion, ID: id, Revision: revision, DeletedThrough: day.Format(time.DateOnly), ExceptionDays: []string{}, PublishedAt: publishedAt.UTC().Truncate(time.Millisecond)}
	if current == nil {
		return result, nil
	}
	if current.Validate() != nil || revision <= current.Revision {
		return Barrier{}, ErrInvalidBarrier
	}
	result.DeletedThrough = current.DeletedThrough
	result.ExceptionDays = append([]string(nil), current.ExceptionDays...)
	value := day.Format(time.DateOnly)
	if value <= result.DeletedThrough || slices.Contains(result.ExceptionDays, value) {
		return result, nil
	}
	through, _ := parseDay(result.DeletedThrough)
	if day.Equal(through.AddDate(0, 0, 1)) {
		result.DeletedThrough = value
		for len(result.ExceptionDays) > 0 {
			through, _ = parseDay(result.DeletedThrough)
			if result.ExceptionDays[0] != through.AddDate(0, 0, 1).Format(time.DateOnly) {
				break
			}
			result.DeletedThrough = result.ExceptionDays[0]
			result.ExceptionDays = result.ExceptionDays[1:]
		}
		return result, nil
	}
	result.ExceptionDays = append(result.ExceptionDays, value)
	sort.Strings(result.ExceptionDays)
	return result, nil
}

type Guard struct {
	installMu sync.Mutex
	state     atomic.Pointer[Barrier]
}

func NewGuard(initial *Barrier) (*Guard, error) {
	guard := &Guard{}
	if initial != nil {
		if err := guard.Install(*initial); err != nil {
			return nil, err
		}
	}
	return guard, nil
}

func (g *Guard) Install(next Barrier) error {
	if g == nil || next.Validate() != nil {
		return ErrInvalidBarrier
	}
	g.installMu.Lock()
	defer g.installMu.Unlock()
	current := g.state.Load()
	if current != nil {
		if next.Revision < current.Revision || (next.Revision == current.Revision && !equal(*current, next)) {
			return ErrInvalidBarrier
		}
		if next.Revision == current.Revision {
			return nil
		}
		if next.DeletedThrough < current.DeletedThrough {
			return ErrInvalidBarrier
		}
		for _, day := range current.ExceptionDays {
			parsed, _ := parseDay(day)
			if _, covered := next.Covers(parsed); !covered {
				return ErrInvalidBarrier
			}
		}
	}
	copy := next
	copy.ExceptionDays = append([]string(nil), next.ExceptionDays...)
	g.state.Store(&copy)
	return nil
}

func (g *Guard) Current() (Barrier, bool) {
	if g == nil {
		return Barrier{}, false
	}
	current := g.state.Load()
	if current == nil {
		return Barrier{}, false
	}
	copy := *current
	copy.ExceptionDays = append([]string(nil), current.ExceptionDays...)
	return copy, true
}

func (g *Guard) Covers(eventTime time.Time) (Decision, bool) {
	current, ok := g.Current()
	if !ok {
		return Decision{}, false
	}
	return current.Covers(eventTime)
}

func parseDay(value string) (time.Time, error) {
	day, err := time.Parse(time.DateOnly, value)
	if err != nil || day.Format(time.DateOnly) != value {
		return time.Time{}, ErrInvalidBarrier
	}
	return day, nil
}

func equal(left, right Barrier) bool {
	return left.SchemaVersion == right.SchemaVersion && left.ID == right.ID && left.Revision == right.Revision &&
		left.DeletedThrough == right.DeletedThrough && left.PublishedAt.Equal(right.PublishedAt) && slices.Equal(left.ExceptionDays, right.ExceptionDays)
}
