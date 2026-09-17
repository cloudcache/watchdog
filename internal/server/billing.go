// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

func (s *Server) registerBillingRoutes(auth *gin.RouterGroup) {
	parties := auth.Group("/billing/parties")
	parties.GET("", s.requirePermission("bill.viewAll"), s.listBillingParties)
	parties.POST("", s.requirePermission("bill.create"), s.createBillingParty)
	parties.GET("/:id", s.requirePermission("bill.viewAll"), s.getBillingParty)
	parties.PATCH("/:id", s.requirePermission("bill.update"), s.updateBillingParty)
	parties.DELETE("/:id", s.requirePermission("bill.delete"), s.deleteBillingParty)

	accounts := auth.Group("/billing/accounts")
	accounts.GET("", s.requirePermission("bill.view"), s.listBillingAccounts)
	accounts.POST("", s.requirePermission("bill.create"), s.createBillingAccount)
	accounts.GET("/:id", s.requirePermission("bill.view"), s.getBillingAccount)
	accounts.PATCH("/:id", s.requirePermission("bill.update"), s.updateBillingAccount)
	accounts.DELETE("/:id", s.requirePermission("bill.delete"), s.deleteBillingAccount)
	accounts.GET("/:id/ports", s.requirePermission("bill.view"), s.listBillingAccountPorts)
	accounts.PUT("/:id/ports", s.requirePermission("bill.update"), s.replaceBillingAccountPorts)
	accounts.GET("/:id/periods", s.requirePermission("bill.view"), s.listBillingPeriods)
	accounts.POST("/:id/periods", s.requirePermission("bill.calculate"), s.createBillingPeriod)
	accounts.GET("/:id/snmp-usage", s.requirePermission("bill.view"), s.readSNMPBilling)

	periods := auth.Group("/billing/periods")
	periods.GET("/:id", s.requirePermission("bill.view"), s.getBillingPeriod)
	periods.GET("/:id/values", s.requirePermission("bill.view"), s.listBillingValues)
	periods.POST("/:id/calculate", s.requirePermission("bill.calculate"), s.enqueueBillingCalculation)
	periods.POST("/:id/reconcile", s.requirePermission("bill.reconcile"), s.enqueueBillingReconciliation)
	periods.POST("/:id/external", s.requirePermission("bill.reconcile"), s.importExternalBillingValue)
	periods.POST("/:id/approve", s.requirePermission("bill.approve"), s.approveBillingPeriod)
	periods.POST("/:id/close", s.requirePermission("bill.approve"), s.closeBillingPeriod)
	periods.GET("/:id/adjustments", s.requirePermission("bill.view"), s.listBillingAdjustments)
	periods.POST("/:id/adjustments", s.requirePermission("bill.update"), s.createBillingAdjustment)
	periods.GET("/:id/issues", s.requirePermission("bill.view"), s.listBillingIssues)
	periods.GET("/:id/reconciliations", s.requirePermission("bill.view"), s.listBillingReconciliations)
	periods.POST("/:id/exports", s.requirePermission("bill.export"), s.createBillingExport)

	adjustments := auth.Group("/billing/adjustments")
	adjustments.POST("/:id/approve", s.requirePermission("bill.approve"), s.approveBillingAdjustment)
	adjustments.POST("/:id/reverse", s.requirePermission("bill.approve"), s.reverseBillingAdjustment)
	issues := auth.Group("/billing/issues")
	issues.PATCH("/:id", s.requirePermission("bill.reconcile"), s.resolveBillingIssue)
	jobs := auth.Group("/billing/jobs")
	jobs.GET("/:id", s.requirePermission("bill.view"), s.getBillingJob)
	jobs.POST("/:id/cancel", s.cancelBillingJob)
	exports := auth.Group("/billing/exports")
	exports.GET("", s.requirePermission("bill.export"), s.listBillingExports)
	exports.GET("/:id", s.requirePermission("bill.export"), s.getBillingExport)
	exports.GET("/:id/download", s.requirePermission("bill.export"), s.downloadBillingExport)
	exports.POST("/:id/cancel", s.requirePermission("bill.export"), s.cancelBillingExport)
}

func writeBillingError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, billing.ErrNotFound):
		fail(c, http.StatusNotFound, "not_found", "billing resource not found")
	case errors.Is(err, billing.ErrConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", "billing resource changed since it was read")
	case errors.Is(err, billing.ErrImmutable):
		fail(c, http.StatusConflict, "period_immutable", "billing period state does not allow this operation")
	case errors.Is(err, billing.ErrInUse):
		fail(c, http.StatusConflict, "resource_in_use", "billing resource has dependent evidence")
	case errors.Is(err, billing.ErrWindowOffset):
		fail(c, http.StatusUnprocessableEntity, "billing_window_offset", err.Error())
	case errors.Is(err, billing.ErrEvidenceUnavailable):
		fail(c, http.StatusServiceUnavailable, "billing_evidence_unavailable", err.Error())
	default:
		var mysqlErr *mysql.MySQLError
		if errors.Is(err, sql.ErrNoRows) || errors.As(err, &mysqlErr) {
			writeSQLError(c, err)
		} else {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		}
	}
}

func requireBillingIfMatch(c *gin.Context) (uint64, bool) {
	expected, supplied, err := ifMatch(c)
	if !supplied {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match is required")
		return 0, false
	}
	if err != nil || expected == 0 {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "If-Match must be a positive row version")
		return 0, false
	}
	return expected, true
}

func (s *Server) billingPage(c *gin.Context, allowed ...string) (billing.PageFilter, bool) {
	if _, ok := addressListParam(c, allowed...); !ok {
		return billing.PageFilter{}, false
	}
	maxPageSize := s.cfg.Billing.MaxPageSize
	if maxPageSize <= 0 {
		maxPageSize = billing.DefaultLimits().MaxPageSize
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "25"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit < 1 {
		limit = 25
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	if offset < 0 {
		offset = 0
	}
	return billing.PageFilter{
		Limit: limit, Offset: offset, Query: c.Query("q"), Sort: c.Query("sort"), Order: c.Query("order"),
		Status: c.Query("status"), Kind: c.Query("kind"), Type: c.Query("type"),
	}, true
}

func (s *Server) listBillingParties(c *gin.Context) {
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status", "kind")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListParties(c, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) createBillingParty(c *gin.Context) {
	var input billing.Party
	if !addressDecodeStrict(c, &input, 64<<10) {
		return
	}
	item, err := s.billingStore.CreateParty(c, input, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.Header("Location", "/api/v1/billing/parties/"+item.ID)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) getBillingParty(c *gin.Context) {
	item, err := s.billingStore.GetParty(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.JSON(http.StatusOK, item)
}

func (s *Server) updateBillingParty(c *gin.Context) {
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	existing, err := s.billingStore.GetParty(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	var input struct {
		Kind   *string `json:"kind"`
		Status *string `json:"status"`
		Name   *string `json:"name"`
		Ref    *string `json:"ref"`
		Notes  *string `json:"notes"`
	}
	if !addressDecodeStrict(c, &input, 64<<10) {
		return
	}
	if input.Kind != nil {
		existing.Kind = *input.Kind
	}
	if input.Status != nil {
		existing.Status = *input.Status
	}
	if input.Name != nil {
		existing.Name = *input.Name
	}
	if input.Ref != nil {
		existing.Ref = *input.Ref
	}
	if input.Notes != nil {
		existing.Notes = *input.Notes
	}
	updated, err := s.billingStore.UpdateParty(c, existing, expected, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteBillingParty(c *gin.Context) {
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	if err := s.billingStore.DeleteParty(c, c.Param("id"), expected, currentPrincipal(c).UserID); err != nil {
		writeBillingError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) listBillingAccounts(c *gin.Context) {
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status", "type")
	if !ok {
		return
	}
	p := currentPrincipal(c)
	items, total, err := s.billingStore.ListAccounts(c, page, p.UserID, p.can("bill.viewAll"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) createBillingAccount(c *gin.Context) {
	var input struct {
		billing.Account
		Items []billing.AccountPort `json:"items"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	if !s.requireBillingPortScope(c, input.Items) {
		return
	}
	item, err := s.billingStore.CreateAccountWithPorts(c, input.Account, input.Items, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	if !currentPrincipal(c).can("bill.viewAll") {
		_, _ = s.db.ExecContext(c, `INSERT IGNORE INTO user_billing_permissions (user_id,account_id) VALUES (?,?)`, currentPrincipal(c).UserID, item.ID)
	}
	c.Header("ETag", etag(item.RowVersion))
	c.Header("Location", "/api/v1/billing/accounts/"+item.ID)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) getBillingAccount(c *gin.Context) {
	id := c.Param("id")
	if !s.requireBillingAccountAccess(c, id) {
		return
	}
	item, err := s.billingStore.GetAccount(c, id)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	ports, err := s.billingStore.ListAccountPorts(c, id)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	dateFrom, dateTo, err := billing.SuggestedPeriodWindow(item, time.Now())
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.JSON(http.StatusOK, gin.H{
		"account":        item,
		"ports":          ports,
		"capacity_check": billing.BuildCapacityCheck(item.ContractBandwidthBPS, ports),
		"suggested_period": gin.H{
			"date_from": dateFrom,
			"date_to":   dateTo,
		},
	})
}

func (s *Server) updateBillingAccount(c *gin.Context) {
	id := c.Param("id")
	if !s.requireBillingAccountAccess(c, id) {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	item, err := s.billingStore.GetAccount(c, id)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	var input struct {
		PartyID           *string                  `json:"party_id"`
		Name              *string                  `json:"name"`
		Status            *string                  `json:"status"`
		MeasurementType   *billing.MeasurementType `json:"measurement_type"`
		BillingMethod     *billing.BillingMethod   `json:"billing_method"`
		Algorithm         *billing.Algorithm       `json:"algorithm"`
		BillingDay        *uint8                   `json:"billing_day"`
		Timezone          *string                  `json:"timezone"`
		Direction         *billing.Direction       `json:"direction"`
		DefaultLayer      *billing.Layer           `json:"default_layer"`
		PriceCurrency     *string                  `json:"price_currency"`
		UnitPrice         *string                  `json:"unit_price"`
		MinimumPercent    *float64                 `json:"minimum_percent"`
		ContractBandwidth *uint64                  `json:"contract_bandwidth_bps"`
		TrafficAllowance  *uint64                  `json:"traffic_allowance_bytes"`
		ReconcileAbs      *uint64                  `json:"reconcile_abs"`
		ReconcilePercent  *float64                 `json:"reconcile_percent"`
		Ref               *string                  `json:"ref"`
		Notes             *string                  `json:"notes"`
		Items             *[]billing.AccountPort   `json:"items"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	if input.PartyID != nil {
		item.PartyID = *input.PartyID
	}
	if input.Name != nil {
		item.Name = *input.Name
	}
	if input.Status != nil {
		item.Status = *input.Status
	}
	if input.MeasurementType != nil {
		item.MeasurementType = *input.MeasurementType
	}
	if input.BillingMethod != nil {
		item.BillingMethod = *input.BillingMethod
	}
	if input.Algorithm != nil {
		item.Algorithm = *input.Algorithm
	}
	if input.BillingDay != nil {
		item.BillingDay = *input.BillingDay
	}
	if input.Timezone != nil {
		item.Timezone = *input.Timezone
	}
	if input.Direction != nil {
		item.Direction = *input.Direction
	}
	if input.DefaultLayer != nil {
		item.DefaultLayer = *input.DefaultLayer
	}
	if input.PriceCurrency != nil {
		item.PriceCurrency = *input.PriceCurrency
	}
	if input.UnitPrice != nil {
		item.UnitPrice = *input.UnitPrice
	}
	if input.MinimumPercent != nil {
		item.MinimumPercent = *input.MinimumPercent
	}
	if input.ContractBandwidth != nil {
		item.ContractBandwidthBPS = input.ContractBandwidth
	}
	if input.TrafficAllowance != nil {
		item.TrafficAllowance = input.TrafficAllowance
	}
	if input.ReconcileAbs != nil {
		item.ReconcileAbs = *input.ReconcileAbs
	}
	if input.ReconcilePercent != nil {
		item.ReconcilePercent = *input.ReconcilePercent
	}
	if input.Ref != nil {
		item.Ref = *input.Ref
	}
	if input.Notes != nil {
		item.Notes = *input.Notes
	}
	var updated billing.Account
	if input.Items != nil {
		if !s.requireBillingPortScope(c, *input.Items) {
			return
		}
		updated, err = s.billingStore.UpdateAccountWithPorts(c, item, *input.Items, expected, currentPrincipal(c).UserID)
	} else {
		updated, err = s.billingStore.UpdateAccount(c, item, expected, currentPrincipal(c).UserID)
	}
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteBillingAccount(c *gin.Context) {
	id := c.Param("id")
	if !s.requireBillingAccountAccess(c, id) {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	if err := s.billingStore.DeleteAccount(c, id, expected, currentPrincipal(c).UserID); err != nil {
		writeBillingError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) listBillingAccountPorts(c *gin.Context) {
	id := c.Param("id")
	if !s.requireBillingAccountAccess(c, id) {
		return
	}
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "type")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListAccountPortsPage(c, id, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) replaceBillingAccountPorts(c *gin.Context) {
	id := c.Param("id")
	if !s.requireBillingAccountAccess(c, id) {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	var input struct {
		Items []billing.AccountPort `json:"items"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	if !s.requireBillingPortScope(c, input.Items) {
		return
	}
	if err := s.billingStore.ReplaceAccountPorts(c, id, input.Items, expected, currentPrincipal(c).UserID); err != nil {
		writeBillingError(c, err)
		return
	}
	account, err := s.billingStore.GetAccount(c, id)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	items, err := s.billingStore.ListAccountPorts(c, id)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(account.RowVersion))
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items), "row_version": account.RowVersion})
}

// requireBillingPortScope applies the same device-root and explicit-port
// grants as device, SNMP and aggregate-graph queries. Billing permissions
// authorize the operation; they do not grant access to otherwise hidden ports.
func (s *Server) requireBillingPortScope(c *gin.Context, items []billing.AccountPort) bool {
	seen := make(map[string]struct{}, len(items))
	portIDs := make([]string, 0, len(items))
	for _, item := range items {
		portID := strings.TrimSpace(item.PortID)
		if portID == "" {
			continue
		}
		if _, exists := seen[portID]; exists {
			continue
		}
		seen[portID] = struct{}{}
		portIDs = append(portIDs, portID)
	}
	if len(portIDs) == 0 {
		return true
	}
	if _, err := s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), nil, portIDs); err != nil {
		if errors.Is(err, errSNMPScopeForbidden) {
			fail(c, http.StatusForbidden, "forbidden", "billing port is outside the caller's resource scope")
		} else {
			writeSQLError(c, err)
		}
		return false
	}
	return true
}

func (s *Server) listBillingPeriods(c *gin.Context) {
	accountID := c.Param("id")
	if !s.requireBillingAccountAccess(c, accountID) {
		return
	}
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListPeriods(c, accountID, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) createBillingPeriod(c *gin.Context) {
	accountID := c.Param("id")
	if !s.requireBillingAccountAccess(c, accountID) {
		return
	}
	var input struct {
		DateFrom time.Time `json:"date_from"`
		DateTo   time.Time `json:"date_to"`
	}
	if !addressDecodeStrict(c, &input, 16<<10) {
		return
	}
	item, err := s.billingStore.CreatePeriod(c, accountID, input.DateFrom, input.DateTo, time.Now(), currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.Header("Location", "/api/v1/billing/periods/"+item.ID)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) billingPeriodAccess(c *gin.Context) (billing.Period, bool) {
	period, err := s.billingStore.GetPeriod(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return billing.Period{}, false
	}
	if !s.requireBillingAccountAccess(c, period.AccountID) {
		return billing.Period{}, false
	}
	return period, true
}

func (s *Server) getBillingPeriod(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	values, err := s.billingStore.ListValues(c, period.ID, period.CalculationVersion)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(period.RowVersion))
	c.JSON(http.StatusOK, gin.H{"period": period, "values": values})
}

func (s *Server) listBillingValues(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	generation := period.CalculationVersion
	if raw := strings.TrimSpace(c.Query("calculation_version")); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "calculation_version must be unsigned")
			return
		}
		generation = value
	}
	items, err := s.billingStore.ListValues(c, period.ID, generation)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items), "calculation_version": generation})
}

func (s *Server) importExternalBillingValue(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	if period.RowVersion != expected {
		writeBillingError(c, billing.ErrConflict)
		return
	}
	var input struct {
		Unit                   string          `json:"unit"`
		InBytes                uint64          `json:"in_bytes"`
		OutBytes               uint64          `json:"out_bytes"`
		SelectedBytes          uint64          `json:"selected_bytes"`
		Rate95thBPS            uint64          `json:"rate_95th_bps"`
		RateDaily95thBPS       uint64          `json:"rate_daily_95th_bps"`
		RateAverageBPS         uint64          `json:"rate_average_bps"`
		AlgorithmValue         uint64          `json:"algorithm_value"`
		Coverage               float64         `json:"coverage"`
		ExpectedBuckets        uint32          `json:"expected_buckets"`
		ObservedBuckets        uint32          `json:"observed_buckets"`
		MissingBuckets         uint32          `json:"missing_buckets"`
		ResetBuckets           uint32          `json:"reset_buckets"`
		GapBuckets             uint32          `json:"gap_buckets"`
		UnknownSamplingRecords uint64          `json:"unknown_sampling_records"`
		Provenance             json.RawMessage `json:"provenance"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	item, err := s.billingStore.SaveExternalDraft(c, period.ID, billing.Value{
		Unit: input.Unit, InBytes: input.InBytes, OutBytes: input.OutBytes, SelectedBytes: input.SelectedBytes,
		Rate95thBPS: input.Rate95thBPS, RateDaily95thBPS: input.RateDaily95thBPS,
		RateAverageBPS: input.RateAverageBPS, AlgorithmValue: input.AlgorithmValue,
		Coverage: input.Coverage, ExpectedBuckets: input.ExpectedBuckets, ObservedBuckets: input.ObservedBuckets,
		MissingBuckets: input.MissingBuckets, ResetBuckets: input.ResetBuckets, GapBuckets: input.GapBuckets,
		UnknownSamplingRecords: input.UnknownSamplingRecords, Provenance: input.Provenance,
	}, expected, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	updated, _ := s.billingStore.GetPeriod(c, period.ID)
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, item)
}

type billingApprovalRequest struct {
	CalculationVersion uint64 `json:"calculation_version"`
}

func (s *Server) approveBillingPeriod(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	var input billingApprovalRequest
	if !addressDecodeStrict(c, &input, 8<<10) {
		return
	}
	updated, err := s.billingStore.ApprovePeriod(c, period.ID, expected, input.CalculationVersion, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) closeBillingPeriod(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	var input billingApprovalRequest
	if !addressDecodeStrict(c, &input, 8<<10) {
		return
	}
	updated, err := s.billingStore.ClosePeriod(c, period.ID, expected, input.CalculationVersion, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) listBillingAdjustments(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListAdjustments(c, period.ID, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) createBillingAdjustment(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	var input struct {
		Layer       billing.Layer `json:"layer"`
		Unit        string        `json:"unit"`
		Amount      int64         `json:"amount"`
		Reason      string        `json:"reason"`
		EvidenceRef string        `json:"evidence_ref"`
	}
	if !addressDecodeStrict(c, &input, 64<<10) {
		return
	}
	item, err := s.billingStore.CreateAdjustment(c, billing.AdjustmentInput{PeriodID: period.ID, Layer: input.Layer, Unit: input.Unit, Amount: input.Amount, Reason: input.Reason, EvidenceRef: input.EvidenceRef, Actor: currentPrincipal(c).UserID})
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.JSON(http.StatusCreated, item)
}

func (s *Server) approveBillingAdjustment(c *gin.Context) {
	item, err := s.billingStore.GetAdjustment(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	period, err := s.billingStore.GetPeriod(c, item.PeriodID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	if !s.requireBillingAccountAccess(c, period.AccountID) {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	updated, err := s.billingStore.ApproveAdjustment(c, item.ID, expected, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) reverseBillingAdjustment(c *gin.Context) {
	item, err := s.billingStore.GetAdjustment(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	period, err := s.billingStore.GetPeriod(c, item.PeriodID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	if !s.requireBillingAccountAccess(c, period.AccountID) {
		return
	}
	var input struct {
		Reason string `json:"reason"`
	}
	if !addressDecodeStrict(c, &input, 16<<10) {
		return
	}
	reversal, err := s.billingStore.ReverseAdjustment(c, item.ID, currentPrincipal(c).UserID, input.Reason)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(reversal.RowVersion))
	c.JSON(http.StatusCreated, reversal)
}

func (s *Server) listBillingIssues(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status", "type")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListIssues(c, period.ID, period.CalculationVersion, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset, "calculation_version": period.CalculationVersion})
}

func (s *Server) listBillingReconciliations(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order", "status")
	if !ok {
		return
	}
	items, total, err := s.billingStore.ListReconciliationRuns(c, period.ID, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) resolveBillingIssue(c *gin.Context) {
	item, err := s.billingStore.GetIssue(c, c.Param("id"))
	if err != nil {
		writeBillingError(c, err)
		return
	}
	var accountID string
	if err := s.db.QueryRowContext(c, `SELECT p.account_id FROM reconciliation_runs r JOIN billing_periods p ON p.id=r.period_id WHERE r.id=?`, item.RunID).Scan(&accountID); err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireBillingAccountAccess(c, accountID) {
		return
	}
	expected, ok := requireBillingIfMatch(c)
	if !ok {
		return
	}
	var input struct {
		Status         string `json:"status"`
		ResolutionNote string `json:"resolution_note"`
	}
	if !addressDecodeStrict(c, &input, 32<<10) {
		return
	}
	updated, err := s.billingStore.ResolveIssue(c, item.ID, input.Status, input.ResolutionNote, currentPrincipal(c).UserID, expected)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}
