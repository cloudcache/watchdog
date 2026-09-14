package server

import (
	"strings"
	"testing"
)

func TestNormalizeGrantValues(t *testing.T) {
	values, err := normalizeGrantValues("metrics", []string{" b ", "a", "b"}, 10, 8)
	if err != nil || strings.Join(values, ",") != "a,b" {
		t.Fatalf("normalize = %v, %v", values, err)
	}
	if _, err := normalizeGrantValues("metrics", []string{"too-long"}, 10, 3); err == nil {
		t.Fatal("overlong grant value was accepted")
	}
}

func TestAppendAggregateGraphScopeSQL(t *testing.T) {
	p := &principal{UserID: "user-a", Abilities: map[string]bool{}}
	where, args := appendAggregateGraphScopeSQL([]string{"1=1"}, nil, p)
	joined := strings.Join(where, " ")
	for _, table := range []string{"user_aggregate_graph_permissions", "user_metric_permissions", "user_port_permissions", "user_device_permissions", "user_device_group_permissions"} {
		if !strings.Contains(joined, table) {
			t.Fatalf("scope SQL is missing %s: %s", table, joined)
		}
	}
	if len(args) != 6 {
		t.Fatalf("scope args=%v, want six user ids", args)
	}
	adminWhere, adminArgs := appendAggregateGraphScopeSQL([]string{"1=1"}, nil, &principal{IsAdmin: true})
	if len(adminWhere) != 1 || len(adminArgs) != 0 {
		t.Fatalf("administrator unexpectedly scoped: %v %v", adminWhere, adminArgs)
	}
}
