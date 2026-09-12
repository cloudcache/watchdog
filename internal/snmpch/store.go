// Package snmpch owns only the SNMP time-series schema, writes, and queries.
// ClickHouse connection pooling and migration execution remain owned by the
// existing flowch infrastructure.
package snmpch

import (
	"context"
	"errors"
	"fmt"

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
	exec Executor
}

func New(exec Executor) (*Store, error) {
	if exec == nil {
		return nil, errors.New("ClickHouse executor is required")
	}
	return &Store{exec: exec}, nil
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
