package watchdog

import "testing"

func TestHasPermissionPortInheritsTargetGrant(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Action:   ActionView,
		Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
	}
	grants := []Permission{{
		TenantID:     "tenant-a",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourceTarget,
		ResourceID:   "target-a",
		Actions:      []Action{ActionView},
	}}
	if !HasPermission(req, grants) {
		t.Fatal("expected target grant to allow port view")
	}
}

func TestHasPermissionDoesNotCrossTenants(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Action:   ActionView,
		Resource: ResourceRef{Type: ResourceTarget, ID: "target-a"},
	}
	grants := []Permission{{
		TenantID:     "tenant-b",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourceTenant,
		ResourceID:   "tenant-b",
		Actions:      []Action{ActionAdmin},
	}}
	if HasPermission(req, grants) {
		t.Fatal("expected cross-tenant grant to be ignored")
	}
}

func TestMostSpecificPermissionPrefersPortGrant(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Action:   ActionExport,
		Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
	}
	grants := []Permission{
		{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourceTarget,
			ResourceID:   "target-a",
			Actions:      []Action{ActionExport},
		},
		{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourcePort,
			ResourceID:   "port-a",
			Actions:      []Action{ActionExport},
		},
	}
	grant, ok := MostSpecificPermission(req, grants)
	if !ok {
		t.Fatal("expected permission")
	}
	if grant.ResourceType != ResourcePort {
		t.Fatalf("expected port grant, got %s", grant.ResourceType)
	}
}

func TestHasPermissionAllowsBillingPeriodByTenantAdmin(t *testing.T) {
	req := AccessRequest{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Action:   ActionAdmin,
		Resource: ResourceRef{Type: ResourceBillingPeriod, ID: "period-a"},
	}
	grants := []Permission{{
		TenantID:     "tenant-a",
		SubjectType:  SubjectUser,
		SubjectID:    "user-a",
		ResourceType: ResourceTenant,
		ResourceID:   "tenant-a",
		Actions:      []Action{ActionAdmin},
	}}
	if !HasPermission(req, grants) {
		t.Fatal("expected tenant admin to manage billing period")
	}
}
