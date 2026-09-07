package watchdog

type Action string

const (
	ActionView                Action = "view"
	ActionConfigure           Action = "configure"
	ActionOperate             Action = "operate"
	ActionExport              Action = "export"
	ActionAdmin               Action = "admin"
	ActionViewRaw             Action = "view_raw"
	ActionViewSupplier        Action = "view_supplier"
	ActionViewCustomer        Action = "view_customer"
	ActionExportRaw           Action = "export_raw"
	ActionExportSupplier      Action = "export_supplier"
	ActionExportCustomer      Action = "export_customer"
	ActionVPNView             Action = "vpn_view"
	ActionVPNExport           Action = "vpn_export"
	ActionVPNTriage           Action = "vpn_triage"
	ActionVPNProbe            Action = "probe"
	ActionConfigureAdjustment Action = "configure_adjustment"
)

type SubjectType string

const (
	SubjectUser SubjectType = "user"
	SubjectRole SubjectType = "role"
)

type ResourceType string

const (
	ResourceTenant         ResourceType = "tenant"
	ResourceTarget         ResourceType = "target"
	ResourcePort           ResourceType = "port"
	ResourceExportTask     ResourceType = "export_task"
	ResourceBillingAccount ResourceType = "billing_account"
	ResourceBillingPeriod  ResourceType = "billing_period"
	ResourceVPNFinding     ResourceType = "vpn_finding"
	ResourceVPNRule        ResourceType = "vpn_rule"
)

type Permission struct {
	ID           ID
	TenantID     ID
	SubjectType  SubjectType
	SubjectID    ID
	ResourceType ResourceType
	ResourceID   ID
	Actions      []Action
}

type AccessRequest struct {
	TenantID ID
	UserID   ID
	RoleIDs  []ID
	Action   Action
	Resource ResourceRef
}

type ResourceRef struct {
	Type     ResourceType
	ID       ID
	ParentID ID
}

func HasPermission(req AccessRequest, grants []Permission) bool {
	for _, grant := range grants {
		if grant.TenantID != req.TenantID {
			continue
		}
		if !subjectMatches(req, grant) {
			continue
		}
		if !resourceMatches(req.Resource, grant) {
			continue
		}
		if actionAllowed(req.Action, grant.Actions) {
			return true
		}
	}
	return false
}

func subjectMatches(req AccessRequest, grant Permission) bool {
	switch grant.SubjectType {
	case SubjectUser:
		return grant.SubjectID == req.UserID
	case SubjectRole:
		for _, roleID := range req.RoleIDs {
			if roleID == grant.SubjectID {
				return true
			}
		}
	}
	return false
}

func resourceMatches(resource ResourceRef, grant Permission) bool {
	if grant.ResourceType == ResourceTenant && grant.ResourceID != "" {
		return true
	}
	if grant.ResourceType == resource.Type && grant.ResourceID == resource.ID {
		return true
	}
	if resource.Type == ResourcePort && grant.ResourceType == ResourceTarget && grant.ResourceID == resource.ParentID {
		return true
	}
	return false
}

func actionAllowed(action Action, actions []Action) bool {
	for _, allowed := range actions {
		if allowed == ActionAdmin || allowed == action {
			return true
		}
	}
	return false
}

func MostSpecificPermission(req AccessRequest, grants []Permission) (Permission, bool) {
	var best Permission
	bestRank := -1
	for _, grant := range grants {
		if grant.TenantID != req.TenantID || !subjectMatches(req, grant) {
			continue
		}
		if !resourceMatches(req.Resource, grant) || !actionAllowed(req.Action, grant.Actions) {
			continue
		}
		rank := resourceRank(req.Resource, grant)
		if rank > bestRank {
			best = grant
			bestRank = rank
		}
	}
	return best, bestRank >= 0
}

func resourceRank(resource ResourceRef, grant Permission) int {
	if grant.ResourceType == resource.Type && grant.ResourceID == resource.ID {
		return 3
	}
	if resource.Type == ResourcePort && grant.ResourceType == ResourceTarget && grant.ResourceID == resource.ParentID {
		return 2
	}
	if grant.ResourceType == ResourceTenant {
		return 1
	}
	return 0
}
