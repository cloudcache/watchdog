package watchdog

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestMySQLListAgentRunsPage proves keyset pagination over an agent's run
// history: newest first by (ended_at, id), no page overlap, full coverage.
func TestMySQLListAgentRunsPage(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES ('tgt_run', ?, 'Run', 'network', '10.9.0.1', 'up')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO target_agents (id, tenant_id, target_id, agent_type, mode, token_hash)
		VALUES ('agent_run', ?, 'tgt_run', 'snmp', 'pull', 'x')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		status := "success"
		if i%2 == 0 {
			status = "failure"
		}
		endedAt := base.Add(time.Duration(i) * time.Minute)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agent_run_history (id, tenant_id, agent_id, target_id, status, seen, started_at, ended_at, duration_ms)
			VALUES (?, ?, 'agent_run', 'tgt_run', ?, 0, ?, ?, 100)
		`, fmt.Sprintf("run_%02d", i), tenant, status, endedAt.Add(-time.Second), endedAt); err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
	}
	// Newest first: run_04, run_03, ... run_00.
	want := []ID{"run_04", "run_03", "run_02", "run_01", "run_00"}

	var got []ID
	cursor := ""
	for i := 0; ; i++ {
		page, next, total, err := store.ListAgentRunsPage(ctx, tenant, "agent_run", AgentRunPageFilter{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if total != len(want) {
			t.Fatalf("total = %d, want %d", total, len(want))
		}
		for _, run := range page {
			got = append(got, run.ID)
		}
		if next == "" {
			break
		}
		cursor = next
		if i > 50 {
			t.Fatal("pagination did not terminate")
		}
	}
	if !equalIDs(got, want) {
		t.Fatalf("paged order = %v, want %v", got, want)
	}

	// A different agent's runs are not returned.
	other, _, otherTotal, err := store.ListAgentRunsPage(ctx, tenant, "agent_missing", AgentRunPageFilter{Limit: 10})
	if err != nil || len(other) != 0 || otherTotal != 0 {
		t.Fatalf("other agent = %v total=%d err=%v", other, otherTotal, err)
	}

	seen := false
	filtered, next, total, err := store.ListAgentRunsPage(ctx, tenant, "agent_run", AgentRunPageFilter{
		Search: "run_0", Status: AgentRunFailure, Seen: &seen, Sort: "duration", Desc: false,
		Limit: 2, Offset: 1, TableMode: true,
	})
	if err != nil {
		t.Fatalf("table page: %v", err)
	}
	if next != "" || total != 3 || len(filtered) != 2 || filtered[0].ID != "run_02" || filtered[1].ID != "run_04" {
		t.Fatalf("table page = %v next=%q total=%d", runIDs(filtered), next, total)
	}
}

func runIDs(runs []AgentRunHistory) []ID {
	ids := make([]ID, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.ID)
	}
	return ids
}
