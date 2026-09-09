package server

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

func (s *Server) readSNMPBilling(c *gin.Context) {
	for name := range c.Request.URL.Query() {
		switch name {
		case "start", "end", "limit", "offset", "max_buckets":
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+name)
			return
		}
	}
	accountID := strings.TrimSpace(c.Param("id"))
	if !s.requireBillingAccountAccess(c, accountID) {
		return
	}
	from, err := time.Parse(time.RFC3339, strings.TrimSpace(c.Query("start")))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_range", "start must be RFC3339")
		return
	}
	to, err := time.Parse(time.RFC3339, strings.TrimSpace(c.Query("end")))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_range", "end must be RFC3339")
		return
	}
	maxBuckets := uint32(120000)
	if raw := strings.TrimSpace(c.Query("max_buckets")); raw != "" {
		value, parseErr := strconv.ParseUint(raw, 10, 32)
		if parseErr != nil || value == 0 || value > 120000 {
			fail(c, http.StatusBadRequest, "invalid_range", "max_buckets must be 1..120000")
			return
		}
		maxBuckets = uint32(value)
	}
	ports, err := s.billingPorts(c, accountID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if len(ports) == 0 {
		fail(c, http.StatusConflict, "billing_scope_empty", "billing account has no ports")
		return
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	result, err := s.snmpMetrics.ReadBilling(c.Request.Context(), snmpch.BillingRequest{
		Ports: ports, From: from, To: to, MaxBuckets: maxBuckets,
	})
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	limit, offset := pageParams(c)
	end := offset + limit
	if offset > len(result.Buckets) {
		offset = len(result.Buckets)
	}
	if end > len(result.Buckets) {
		end = len(result.Buckets)
	}
	items := result.Buckets[offset:end]
	result.Buckets = nil
	c.JSON(http.StatusOK, gin.H{
		"account_id": accountID, "summary": result, "items": items,
		"total": result.ObservedBuckets, "limit": limit, "offset": offset,
	})
}

func (s *Server) requireBillingAccountAccess(c *gin.Context, accountID string) bool {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return false
	}
	var exists bool
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM billing_accounts WHERE id=?)`, accountID).Scan(&exists); err != nil {
		writeSQLError(c, err)
		return false
	}
	if !exists {
		writeSQLError(c, sql.ErrNoRows)
		return false
	}
	if p.can("bill.viewAll") {
		return true
	}
	var allowed bool
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(
		SELECT 1 FROM user_billing_permissions WHERE user_id=? AND account_id=?
	)`, p.UserID, accountID).Scan(&allowed); err != nil {
		writeSQLError(c, err)
		return false
	}
	if !allowed {
		fail(c, http.StatusForbidden, "forbidden", "billing account is outside the caller's resource scope")
		return false
	}
	return true
}

func (s *Server) billingPorts(c *gin.Context, accountID string) ([]snmpch.BillingPort, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT bap.port_id,bap.direction
		FROM billing_account_ports bap
		JOIN ports p ON p.id=bap.port_id
		WHERE bap.account_id=?
		ORDER BY bap.port_id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []snmpch.BillingPort
	for rows.Next() {
		var item snmpch.BillingPort
		if err := rows.Scan(&item.PortID, &item.Direction); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
