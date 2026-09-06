package watchdog

func CanCreateExport(req AccessRequest, valueMode ExportValueMode, grants []Permission, isAdmin bool) bool {
	return CanCreateExportLayer(req, inferExportValueLayer(ExportTask{ValueMode: valueMode}), grants, isAdmin)
}

func CanCreateExportLayer(req AccessRequest, layer QueryValueLayer, grants []Permission, isAdmin bool) bool {
	if isAdmin {
		return true
	}
	if layer == QueryValueRaw || layer == QueryValueSupplier {
		for _, action := range []Action{exportLayerAction(layer), ActionAdmin} {
			actionReq := req
			actionReq.Action = action
			if HasPermission(actionReq, grants) {
				return true
			}
		}
		return false
	}
	for _, action := range []Action{ActionExportCustomer, ActionExport, ActionConfigure, ActionAdmin} {
		actionReq := req
		actionReq.Action = action
		if HasPermission(actionReq, grants) {
			return true
		}
	}
	return false
}
