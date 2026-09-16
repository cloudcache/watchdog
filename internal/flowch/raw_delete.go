// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
)

// DropRawDay removes exactly one UTC event-time partition from flow_records.
// The query ID is persisted in the MySQL deletion receipt before execution and
// reused across retries so an ambiguous acknowledgement remains traceable.
func (r *RollupRunner) DropRawDay(ctx context.Context, sourceDate time.Time, queryID string) error {
	if r == nil || r.executor == nil {
		return Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	day := sourceDate.UTC()
	queryID = strings.TrimSpace(queryID)
	if day.IsZero() || day != day.Truncate(24*time.Hour) || queryID == "" || len(queryID) > 128 {
		return Permanent(errors.New("raw deletion requires a UTC day and bounded query ID"))
	}
	partition := day.Year()*10000 + int(day.Month())*100 + day.Day()
	query := ch.Query{
		Body:    fmt.Sprintf("ALTER TABLE flow_records DROP PARTITION %d", partition),
		QueryID: queryID,
		Settings: []ch.Setting{
			{Key: "alter_sync", Value: "2", Important: true},
		},
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return classifyClickHouseError(fmt.Errorf("drop raw Flow partition %d: %w", partition, err))
	}
	return nil
}
