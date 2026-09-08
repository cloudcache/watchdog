package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cron "github.com/robfig/cron/v3"
)

var (
	ErrOperationJobScheduleConflict = errors.New("operation job schedule changed since it was read")
	ErrOperationJobScheduleExists   = errors.New("an operation job schedule already exists for this job type and partition")
)

const (
	operationJobSchedulePayloadMax = 256 * 1024
	operationJobScheduleSearchMax  = 5 * 366 * 24 * 60
	operationJobSchedulerKey       = "operation-jobs"
)

// OperationJobSchedule is a management-plane trigger. It only creates a
// normal operation_jobs row; execution, retries, cancellation and takeover
// remain owned by the existing lease state machine.
type OperationJobSchedule struct {
	ID              ID              `json:"id"`
	TenantID        ID              `json:"tenant_id,omitempty"`
	ScopeType       string          `json:"scope_type"`
	Name            string          `json:"name"`
	JobType         string          `json:"job_type"`
	PartitionKey    string          `json:"partition_key"`
	CronExpression  string          `json:"cron_expression"`
	Timezone        string          `json:"timezone"`
	PayloadJSON     json.RawMessage `json:"payload"`
	Enabled         bool            `json:"enabled"`
	MaxInflight     uint32          `json:"max_inflight"`
	NextRunAt       time.Time       `json:"next_run_at"`
	LastEnqueuedAt  time.Time       `json:"last_enqueued_at,omitzero"`
	LastErrorDetail string          `json:"last_error_detail,omitempty"`
	RowVersion      uint64          `json:"row_version"`
	CreatedBy       ID              `json:"created_by,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type OperationJobScheduleFilter struct {
	Search  string
	JobType string
	Enabled *bool
	Sort    string
	Desc    bool
	Limit   int
	Offset  int
}

type OperationJobScheduleRepository interface {
	ListOperationJobSchedules(context.Context, ID, OperationJobScheduleFilter) ([]OperationJobSchedule, int64, error)
	GetOperationJobSchedule(context.Context, ID, ID) (OperationJobSchedule, error)
	CreateOperationJobSchedule(context.Context, OperationJobSchedule) (OperationJobSchedule, error)
	UpdateOperationJobSchedule(context.Context, OperationJobSchedule, uint64) (OperationJobSchedule, error)
	DeleteOperationJobSchedule(context.Context, ID, ID, uint64) error
}

type OperationJobScheduleDispatchResult struct {
	Scanned       int `json:"scanned"`
	Enqueued      int `json:"enqueued"`
	Backpressured int `json:"backpressured"`
	Unregistered  int `json:"unregistered"`
}

type OperationJobScheduleDispatchRepository interface {
	DispatchDueOperationJobSchedules(context.Context, string, time.Time, int, int, map[string]int) (OperationJobScheduleDispatchResult, error)
}

func normalizeOperationJobSchedule(schedule OperationJobSchedule, now time.Time) (OperationJobSchedule, error) {
	schedule.Name = strings.TrimSpace(schedule.Name)
	schedule.JobType = strings.TrimSpace(schedule.JobType)
	schedule.PartitionKey = strings.TrimSpace(schedule.PartitionKey)
	schedule.CronExpression = strings.TrimSpace(schedule.CronExpression)
	schedule.Timezone = strings.TrimSpace(schedule.Timezone)
	if schedule.ScopeType == "" {
		schedule.ScopeType = OperationJobScopeTenant
	}
	if schedule.Timezone == "" {
		schedule.Timezone = "UTC"
	}
	if schedule.MaxInflight == 0 {
		schedule.MaxInflight = 1
	}
	if schedule.Name == "" || len(schedule.Name) > 190 {
		return OperationJobSchedule{}, errors.New("schedule name is required and must not exceed 190 bytes")
	}
	if schedule.JobType == "" || len(schedule.JobType) > 64 {
		return OperationJobSchedule{}, errors.New("schedule job_type is required and must not exceed 64 bytes")
	}
	if schedule.PartitionKey == "" || len(schedule.PartitionKey) > 128 {
		return OperationJobSchedule{}, errors.New("schedule partition_key is required and must not exceed 128 bytes")
	}
	if schedule.MaxInflight > 1024 {
		return OperationJobSchedule{}, errors.New("schedule max_inflight must be between 1 and 1024")
	}
	switch schedule.ScopeType {
	case OperationJobScopeTenant:
		if schedule.TenantID == "" {
			return OperationJobSchedule{}, errors.New("tenant schedule requires a tenant")
		}
	case OperationJobScopeSystem:
		if schedule.TenantID != "" {
			return OperationJobSchedule{}, errors.New("system schedule must not carry a tenant")
		}
	default:
		return OperationJobSchedule{}, errors.New("schedule scope must be tenant or system")
	}
	if err := validateOperationJobSchedulePayload(schedule.PayloadJSON); err != nil {
		return OperationJobSchedule{}, err
	}
	next, err := nextOperationJobScheduleTime(schedule.CronExpression, schedule.Timezone, now)
	if err != nil {
		return OperationJobSchedule{}, err
	}
	schedule.NextRunAt = next
	return schedule, nil
}

func validateOperationJobSchedulePayload(raw json.RawMessage) error {
	if len(raw) == 0 || len(raw) > operationJobSchedulePayloadMax {
		return fmt.Errorf("schedule payload is required and must not exceed %d bytes", operationJobSchedulePayloadMax)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return errors.New("schedule payload must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("schedule payload must contain exactly one JSON object")
	}
	return nil
}

// nextOperationJobScheduleTime evaluates the five-field cron expression in the
// requested timezone without depending on the removed legacy scheduler.
func nextOperationJobScheduleTime(expression, timezone string, after time.Time) (time.Time, error) {
	location, err := time.LoadLocation(strings.TrimSpace(timezone))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid schedule timezone: %w", err)
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse("CRON_TZ=" + location.String() + " " + strings.TrimSpace(expression))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	next := schedule.Next(after.UTC()).UTC()
	if next.IsZero() || next.After(after.UTC().Add(operationJobScheduleSearchMax*time.Minute)) {
		return time.Time{}, errors.New("cron expression has no occurrence in the next five years")
	}
	return next, nil
}

type OperationJobScheduleDispatcher struct {
	Repository    OperationJobScheduleDispatchRepository
	Registry      *OperationJobHandlerRegistry
	Interval      time.Duration
	ScanLimit     int
	DispatchLimit int
	Logf          func(string, ...any)
}

func (d OperationJobScheduleDispatcher) RunOnce(ctx context.Context, now time.Time) (OperationJobScheduleDispatchResult, error) {
	if d.Repository == nil || d.Registry == nil {
		return OperationJobScheduleDispatchResult{}, errors.New("operation job schedule repository and registry are required")
	}
	scanLimit := d.ScanLimit
	if scanLimit <= 0 {
		scanLimit = 100
	}
	dispatchLimit := d.DispatchLimit
	if dispatchLimit <= 0 {
		dispatchLimit = 50
	}
	return d.Repository.DispatchDueOperationJobSchedules(ctx, operationJobSchedulerKey, now.UTC(), scanLimit, dispatchLimit, d.Registry.concurrencyBudgets())
}

func (d OperationJobScheduleDispatcher) Run(ctx context.Context) {
	interval := d.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	run := func() {
		if _, err := d.RunOnce(ctx, time.Now()); err != nil && ctx.Err() == nil && d.Logf != nil {
			d.Logf("operation job schedule dispatch: %v", err)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
