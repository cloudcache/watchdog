package watchdog

import (
	"context"
	"testing"
)

func TestMySQLListAgentsPage(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	for _, target := range []struct {
		id, name string
	}{
		{"target_agent_page_a", "Agent A"},
		{"target_agent_page_b", "Agent B"},
		{"target_agent_page_c", "Agent C"},
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES (?, ?, ?, 'system', ?, 'up')`, target.id, tenant, target.name, target.id+".example"); err != nil {
			t.Fatalf("seed target %s: %v", target.id, err)
		}
	}
	for _, agent := range []SNMPAgentConfig{
		{ID: "agent_page_alpha", TenantID: tenant, TargetID: "target_agent_page_a", AgentType: AgentTypeSNMP, Mode: AgentModePull, Endpoint: "alpha.example:161", TokenHash: "hash-a", Status: AgentStatusUp},
		{ID: "agent_page_bravo", TenantID: tenant, TargetID: "target_agent_page_b", AgentType: AgentTypeSystem, Mode: AgentModePush, Endpoint: "https://bravo.example", TokenHash: "hash-b", Status: AgentStatusDown},
		{ID: "agent_page_charlie", TenantID: tenant, TargetID: "target_agent_page_c", AgentType: AgentTypeSNMP, Mode: AgentModePush, Endpoint: "https://charlie.example", TokenHash: "hash-c", Status: AgentStatusError},
	} {
		if _, err := store.UpsertAgent(ctx, agent); err != nil {
			t.Fatalf("seed agent %s: %v", agent.ID, err)
		}
	}

	ids := func(agents []SNMPAgentConfig) []ID {
		result := make([]ID, 0, len(agents))
		for _, agent := range agents {
			result = append(result, agent.ID)
		}
		return result
	}
	page := func(filter AgentPageFilter) ([]ID, int) {
		t.Helper()
		agents, total, err := store.ListAgentsPage(ctx, tenant, true, nil, filter)
		if err != nil {
			t.Fatalf("page %+v: %v", filter, err)
		}
		return ids(agents), total
	}

	if got, total := page(AgentPageFilter{Sort: "id", Limit: 2}); !equalIDs(got, []ID{"agent_page_alpha", "agent_page_bravo"}) || total != 3 {
		t.Fatalf("first page = %v total=%d", got, total)
	}
	if got, total := page(AgentPageFilter{Sort: "id", Limit: 2, Offset: 2}); !equalIDs(got, []ID{"agent_page_charlie"}) || total != 3 {
		t.Fatalf("second page = %v total=%d", got, total)
	}
	if got, total := page(AgentPageFilter{Search: "BRAVO", Sort: "id", Limit: 10}); !equalIDs(got, []ID{"agent_page_bravo"}) || total != 1 {
		t.Fatalf("search page = %v total=%d", got, total)
	}
	if got, total := page(AgentPageFilter{AgentType: AgentTypeSNMP, Status: AgentStatusError, Sort: "id", Limit: 10}); !equalIDs(got, []ID{"agent_page_charlie"}) || total != 1 {
		t.Fatalf("filtered page = %v total=%d", got, total)
	}
	if got, total := page(AgentPageFilter{Sort: "id", Desc: true, Limit: 10}); !equalIDs(got, []ID{"agent_page_charlie", "agent_page_bravo", "agent_page_alpha"}) || total != 3 {
		t.Fatalf("descending page = %v total=%d", got, total)
	}

	scoped, total, err := store.ListAgentsPage(ctx, tenant, false, []ID{"target_agent_page_b"}, AgentPageFilter{Sort: "id", Limit: 10})
	if err != nil || !equalIDs(ids(scoped), []ID{"agent_page_bravo"}) || total != 1 {
		t.Fatalf("scoped page = %v total=%d err=%v", ids(scoped), total, err)
	}
	empty, total, err := store.ListAgentsPage(ctx, tenant, false, nil, AgentPageFilter{Limit: 10})
	if err != nil || len(empty) != 0 || total != 0 {
		t.Fatalf("empty scope = %v total=%d err=%v", empty, total, err)
	}
}
