// Package snmpch owns only the SNMP time-series schema, writes, and queries.
// ClickHouse connection pooling and migration execution remain owned by the
// existing flowch infrastructure.
package snmpch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Executor is implemented by flowch.NativeInserter. Keeping this boundary
// small lets the SNMP collector and API share the established bounded pool
// without creating another ClickHouse client or migration state machine.
type Executor interface {
	Do(context.Context, ch.Query) error
}

type Store struct {
	exec   Executor
	limits QueryLimits
}

// QueryLimits are operator budgets for ClickHouse reads. The hard ceilings
// prevent a bad configuration from turning a synchronous API request into an
// unbounded cluster query; deployments may tune anywhere below them.
type QueryLimits struct {
	MaxResultRows    uint32
	MaxExecutionTime time.Duration
	MaxRowsToRead    uint64
	MaxBytesToRead   uint64
	MaxMemoryBytes   uint64
}

const (
	HardMaxQueryExecutionTime = 2 * time.Minute
	HardMaxQueryRowsToRead    = uint64(500_000_000)
	HardMaxQueryBytesToRead   = uint64(64 << 30)
	HardMaxQueryMemoryBytes   = uint64(8 << 30)
)

func DefaultQueryLimits() QueryLimits {
	return QueryLimits{
		MaxResultRows: HardMaxAggregateRows, MaxExecutionTime: 15 * time.Second,
		MaxRowsToRead: 50_000_000, MaxBytesToRead: 4 << 30, MaxMemoryBytes: 2 << 30,
	}
}

func ValidateQueryLimits(limits QueryLimits) error {
	if limits.MaxResultRows == 0 || limits.MaxResultRows > HardMaxAggregateRows {
		return fmt.Errorf("SNMP query result rows must be 1..%d", HardMaxAggregateRows)
	}
	if limits.MaxExecutionTime <= 0 || limits.MaxExecutionTime > HardMaxQueryExecutionTime {
		return fmt.Errorf("SNMP query execution time must be positive and at most %s", HardMaxQueryExecutionTime)
	}
	if limits.MaxRowsToRead == 0 || limits.MaxRowsToRead > HardMaxQueryRowsToRead ||
		limits.MaxBytesToRead == 0 || limits.MaxBytesToRead > HardMaxQueryBytesToRead ||
		limits.MaxMemoryBytes == 0 || limits.MaxMemoryBytes > HardMaxQueryMemoryBytes {
		return errors.New("SNMP query read and memory budgets must be positive and within hard safety ceilings")
	}
	return nil
}

func New(exec Executor) (*Store, error) {
	return NewWithQueryLimits(exec, DefaultQueryLimits())
}

func NewWithQueryLimits(exec Executor, limits QueryLimits) (*Store, error) {
	if exec == nil {
		return nil, errors.New("ClickHouse executor is required")
	}
	if err := ValidateQueryLimits(limits); err != nil {
		return nil, err
	}
	return &Store{exec: exec, limits: limits}, nil
}

func (s *Store) querySettings(maxRows uint32) []ch.Setting {
	if maxRows > s.limits.MaxResultRows {
		maxRows = s.limits.MaxResultRows
	}
	return []ch.Setting{
		{Key: "max_execution_time", Value: strconv.FormatInt(int64((s.limits.MaxExecutionTime+time.Second-1)/time.Second), 10), Important: true},
		{Key: "max_result_rows", Value: strconv.FormatUint(uint64(maxRows)+1, 10), Important: true},
		{Key: "result_overflow_mode", Value: "throw", Important: true},
		{Key: "max_rows_to_read", Value: strconv.FormatUint(s.limits.MaxRowsToRead, 10), Important: true},
		{Key: "read_overflow_mode", Value: "throw", Important: true},
		{Key: "max_bytes_to_read", Value: strconv.FormatUint(s.limits.MaxBytesToRead, 10), Important: true},
		{Key: "max_memory_usage", Value: strconv.FormatUint(s.limits.MaxMemoryBytes, 10), Important: true},
	}
}

func (s *Store) allowsRows(rows uint32) bool {
	return s != nil && rows > 0 && rows <= s.limits.MaxResultRows
}

func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.exec == nil {
		return errors.New("SNMP ClickHouse store is not initialized")
	}
	var required proto.ColUInt64
	query := ch.Query{
		Body: `SELECT count() FROM system.columns
WHERE database=currentDatabase() AND (
  (table='snmp_samples' AND name IN
   ('observed_at','ingested_at','device_id','agent_id','entity_kind','entity_id','recipe_id','metric','value_kind','gauge_value','counter_value','counter_width','interval_ms','quality_flags','poll_sequence','source_run_id','sample_index'))
  OR
  (table='snmp_events' AND name IN
   ('id','occurred_at','ingested_at','device_id','entity_type','entity_id','source','severity','event_type','message','raw_json'))
)`,
		Result: proto.Results{{Name: "count()", Data: &required}},
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return fmt.Errorf("verify SNMP ClickHouse schema: %w", err)
	}
	if required.Rows() != 1 || required[0] != 28 {
		return fmt.Errorf("SNMP ClickHouse schema is not ready (columns=%d/28)", columnOrZero(required))
	}
	return nil
}

func columnOrZero(column proto.ColUInt64) uint64 {
	if column.Rows() == 0 {
		return 0
	}
	return column[0]
}
