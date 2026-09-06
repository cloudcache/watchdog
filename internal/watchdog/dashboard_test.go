package watchdog

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateDashboardLayout(t *testing.T) {
	cases := []struct {
		name    string
		layout  string
		wantErr bool
	}{
		{name: "empty panels object", layout: `{"panels":[]}`},
		{name: "valid panels", layout: `{"panels":[{"graph_id":"g1","x":0,"y":0,"w":6,"h":4},{"graph_id":"g2"}]}`},
		{name: "object with extra fields tolerated", layout: `{"title":"Ops","panels":[{"graph_id":"g1","note":"hi"}]}`},
		{name: "missing layout", layout: ``, wantErr: true},
		{name: "not an object", layout: `[{"graph_id":"g1"}]`, wantErr: true},
		{name: "scalar", layout: `42`, wantErr: true},
		{name: "invalid json", layout: `{"panels":[`, wantErr: true},
		{name: "trailing json", layout: `{"panels":[]} {"panels":[]}`, wantErr: true},
		{name: "panel without graph_id", layout: `{"panels":[{"x":1}]}`, wantErr: true},
		{name: "panel with blank graph_id", layout: `{"panels":[{"graph_id":"  "}]}`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDashboardLayout(json.RawMessage(tc.layout))
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateDashboardLayout(%q) err = %v, wantErr = %v", tc.layout, err, tc.wantErr)
			}
		})
	}
}

func TestValidateDashboardLayoutTooManyPanels(t *testing.T) {
	panels := make([]string, dashboardMaxPanels+1)
	for i := range panels {
		panels[i] = `{"graph_id":"g"}`
	}
	layout := `{"panels":[` + strings.Join(panels, ",") + `]}`
	if err := validateDashboardLayout(json.RawMessage(layout)); err == nil {
		t.Fatal("expected too-many-panels error")
	}
}

func TestNormalizeDashboard(t *testing.T) {
	// Trims the name and requires it.
	if _, err := normalizeDashboard(Dashboard{Name: "  ", Layout: json.RawMessage(`{"panels":[]}`)}); err == nil {
		t.Fatal("blank name must be rejected")
	}
	d, err := normalizeDashboard(Dashboard{Name: "  Ops  ", Layout: json.RawMessage(`{"panels":[]}`)})
	if err != nil {
		t.Fatalf("normalizeDashboard() error = %v", err)
	}
	if d.Name != "Ops" {
		t.Fatalf("name = %q, want trimmed 'Ops'", d.Name)
	}
	// A missing layout is rejected.
	if _, err := normalizeDashboard(Dashboard{Name: "Ops"}); err == nil {
		t.Fatal("missing layout must be rejected")
	}
}
