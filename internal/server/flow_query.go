// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flowQueryService is the single ClickHouse-backed entry point for the v2 flow
// query API. Its runners share the server-wide ClickHouse connection pool, and it
// replaces the retired hub QueryGateway/DatasetProvider/VictoriaMetrics stack:
// flow business data is queried only through ClickHouse (KISS-06).
type flowQueryService struct {
	detail     *flowquery.DetailRunner
	overseas   *flowquery.OverseasRunner
	aggregate  *flowquery.Runner
	joint      *flowquery.JointRunner
	addressSet *flowquery.AddressSetRunner
}

func newFlowQueryService(exec flowquery.Executor) (*flowQueryService, error) {
	detail, err := flowquery.NewDetailRunner(exec)
	if err != nil {
		return nil, err
	}
	overseas, err := flowquery.NewOverseasRunner(exec)
	if err != nil {
		return nil, err
	}
	aggregate, err := flowquery.NewRunner(exec)
	if err != nil {
		return nil, err
	}
	joint, err := flowquery.NewJointRunner(exec)
	if err != nil {
		return nil, err
	}
	addressSet, err := flowquery.NewAddressSetRunner(exec)
	if err != nil {
		return nil, err
	}
	return &flowQueryService{detail: detail, overseas: overseas, aggregate: aggregate, joint: joint, addressSet: addressSet}, nil
}

// startFlowQuery builds the flow query runners on the shared ClickHouse pool.
func (s *Server) startFlowQuery() error {
	if s.clickHouse == nil {
		return nil
	}
	svc, err := newFlowQueryService(s.clickHouse)
	if err != nil {
		return err
	}
	s.flowQuery = svc
	return nil
}

// authorizeFlowView normalizes and authorizes the value-layer view of a flow
// query. The view (customer/supplier/raw) is the primary sensitivity gate; the
// least-privileged customer layer is the default. It writes the response and
// returns false when the view is unsupported or the caller lacks flow.view.<view>.
func (s *Server) authorizeFlowView(c *gin.Context, requested flowquery.View) (flowquery.View, bool) {
	view := requested
	if view == "" {
		view = flowquery.ViewCustomer
	}
	switch view {
	case flowquery.ViewCustomer, flowquery.ViewSupplier, flowquery.ViewRaw:
	default:
		fail(c, http.StatusBadRequest, "invalid_request", "view must be customer, supplier, or raw")
		return "", false
	}
	if !currentPrincipal(c).can("flow.view." + string(view)) {
		fail(c, http.StatusForbidden, "forbidden", "flow "+string(view)+" view is not permitted")
		return "", false
	}
	return view, true
}

// authorizeFlowResourceFilters denies a non-admin caller from scoping a flow
// query to devices/targets/exporters outside its resource scope. device.viewAll
// bypasses. Device filters are checked against the per-device ACL; target- and
// exporter-scoped queries require device.viewAll (fail-closed) because the v2
// resource model is device-centric.
func (s *Server) authorizeFlowResourceFilters(c *gin.Context, targetIDs, deviceIDs, exporterIDs []string) bool {
	p := currentPrincipal(c)
	if p.can("device.viewAll") {
		return true
	}
	if len(targetIDs) > 0 || len(exporterIDs) > 0 {
		fail(c, http.StatusForbidden, "forbidden", "target- and exporter-scoped flow queries require broader device access")
		return false
	}
	for _, deviceID := range deviceIDs {
		allowed, err := s.principalCanAccessDevice(c.Request.Context(), p, deviceID)
		if err != nil {
			writeSQLError(c, err)
			return false
		}
		if !allowed {
			fail(c, http.StatusForbidden, "forbidden", "flow query resource permission denied")
			return false
		}
	}
	return true
}

// principalCanAccessDevice reports whether the principal may see the device, via
// the device.viewAll ability or a direct/group device-permission grant. It
// writes nothing, so callers that authorize several devices at once (the flow
// resource-filter check) can reuse it; requireDeviceAccess wraps it.
func (s *Server) principalCanAccessDevice(ctx context.Context, p *principal, deviceID string) (bool, error) {
	if p == nil {
		return false, nil
	}
	if p.can("device.viewAll") {
		return true, nil
	}
	var allowed bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM user_device_permissions WHERE user_id=? AND device_id=?
		UNION ALL
		SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=?
	)`, p.UserID, deviceID, p.UserID, deviceID).Scan(&allowed)
	return allowed, err
}

// writeFlowQueryError maps a flowquery error to an HTTP response: a typed
// RequestError (validation/permission) becomes 400/403 with its field and code;
// a cancelled/timed-out context becomes 504; any other execution failure is a
// 503 (the ClickHouse backend is the only flow query store).
// writeFlowQueryError maps a flow query/report execution failure to the hub's QUERY_*
// error vocabulary, carrying the offending field + engine code in details so a client
// sees the same envelope the gateway produced.
func writeFlowQueryError(c *gin.Context, err error) {
	var reqErr *flowquery.RequestError
	if errors.As(err, &reqErr) {
		status, code := http.StatusBadRequest, "QUERY_INVALID"
		switch reqErr.Code {
		case flowquery.ErrorLimitExceeded:
			code = "QUERY_RANGE_LIMIT"
		case flowquery.ErrorIncompleteRange:
			status, code = http.StatusConflict, "QUERY_INCOMPLETE"
		case flowquery.ErrorPermissionDenied:
			status, code = http.StatusForbidden, "QUERY_PERMISSION_DENIED"
		}
		failDetails(c, status, code, reqErr.Message,
			map[string]any{"field": reqErr.Field, "flow_code": string(reqErr.Code)})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		failDetails(c, http.StatusGatewayTimeout, "QUERY_TIMEOUT", "flow query timed out", nil)
		return
	}
	failDetails(c, http.StatusServiceUnavailable, "QUERY_PROVIDER_FAILURE", "flow query failed", nil)
}

// flowDetailResourceFilters extracts the target/device/exporter ids a flow
// detail request scopes to, from both structured filters and column filters —
// the inputs authorizeFlowResourceFilters checks.
func flowDetailResourceFilters(filters flowquery.DetailFilters, columns []flowquery.DetailColumnFilter) (targetIDs, deviceIDs, exporterIDs []string) {
	targetIDs = append(targetIDs, filters.TargetIDs...)
	deviceIDs = append(deviceIDs, filters.DeviceIDs...)
	exporterIDs = append(exporterIDs, filters.ExporterIDs...)
	for _, filter := range columns {
		switch filter.Field {
		case string(flowquery.DetailFieldTargetID):
			targetIDs = append(targetIDs, filter.Values...)
		case string(flowquery.DetailFieldDeviceID):
			deviceIDs = append(deviceIDs, filter.Values...)
		case string(flowquery.DetailFieldExporterID):
			exporterIDs = append(exporterIDs, filter.Values...)
		}
	}
	return targetIDs, deviceIDs, exporterIDs
}
