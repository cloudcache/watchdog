package watchdog

import "testing"

func TestCanCreateCorrectedExportWithExportPermission(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
	}
	grants := []Permission{{
		TenantID:     "tenant-a",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourcePort,
		ResourceID:   "port-a",
		Actions:      []Action{ActionExport},
	}}
	if !CanCreateExport(req, ExportValueCorrected, grants, false) {
		t.Fatal("expected corrected export to allow export permission")
	}
}

func TestCanCreateCorrectedExportWithConfigureOrAdmin(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Resource: ResourceRef{Type: ResourceTarget, ID: "target-a"},
	}
	for _, action := range []Action{ActionConfigure, ActionAdmin} {
		grants := []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourceTarget,
			ResourceID:   "target-a",
			Actions:      []Action{action},
		}}
		if !CanCreateExport(req, ExportValueCorrected, grants, false) {
			t.Fatalf("expected corrected export to allow %s permission", action)
		}
	}
	if !CanCreateExport(req, ExportValueCorrected, nil, true) {
		t.Fatal("expected corrected export to allow admin user")
	}
}

func TestCanCreateRawAndBothExportRequireAdmin(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
	}
	exportOnly := []Permission{{
		TenantID:     "tenant-a",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourcePort,
		ResourceID:   "port-a",
		Actions:      []Action{ActionExport},
	}}
	for _, mode := range []ExportValueMode{ExportValueRaw, ExportValueBoth} {
		if CanCreateExport(req, mode, exportOnly, false) {
			t.Fatalf("expected %s export to deny export-only permission", mode)
		}
	}
	admin := []Permission{{
		TenantID:     "tenant-a",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourcePort,
		ResourceID:   "port-a",
		Actions:      []Action{ActionAdmin},
	}}
	for _, mode := range []ExportValueMode{ExportValueRaw, ExportValueBoth} {
		if !CanCreateExport(req, mode, admin, false) {
			t.Fatalf("expected %s export to allow admin permission", mode)
		}
		if !CanCreateExport(req, mode, nil, true) {
			t.Fatalf("expected %s export to allow admin user", mode)
		}
	}
}
