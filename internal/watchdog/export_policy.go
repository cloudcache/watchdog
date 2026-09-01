package watchdog

func CanCreateExport(req AccessRequest, valueMode ExportValueMode, grants []Permission, isAdmin bool) bool {
	if isAdmin {
		return true
	}
	if valueMode == ExportValueRaw || valueMode == ExportValueBoth {
		adminReq := req
		adminReq.Action = ActionAdmin
		return HasPermission(adminReq, grants)
	}
	for _, action := range []Action{ActionExport, ActionConfigure, ActionAdmin} {
		actionReq := req
		actionReq.Action = action
		if HasPermission(actionReq, grants) {
			return true
		}
	}
	return false
}
