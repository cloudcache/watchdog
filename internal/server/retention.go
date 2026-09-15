package server

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const globalRetentionScope = "__global__"

// metricRetentionPolicy preserves the established API field names while the
// current single-domain implementation remains owned by the Gin server.
type metricRetentionPolicy struct {
	ID                   string
	TargetID             string
	HighPrecisionDays    uint32
	ManualCleanupEnabled bool
	Notes                string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func (s *Server) listRetentionPolicies(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT id, COALESCE(target_id,''), high_precision_days, manual_cleanup_enabled,
		       COALESCE(notes,''), created_at, updated_at
		FROM metric_retention_policies
		ORDER BY target_id IS NOT NULL, target_id, id`)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]metricRetentionPolicy, 0)
	for rows.Next() {
		item, err := scanSingleDomainRetentionPolicy(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) putRetentionPolicy(c *gin.Context) {
	var policy metricRetentionPolicy
	if err := c.ShouldBindJSON(&policy); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	policy.TargetID = strings.TrimSpace(policy.TargetID)
	policy.Notes = strings.TrimSpace(policy.Notes)
	if policy.HighPrecisionDays == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "high precision days is required")
		return
	}
	if len(policy.TargetID) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "target id must not exceed 26 characters")
		return
	}
	scope := globalRetentionScope
	if policy.TargetID != "" {
		scope = string(policy.TargetID)
	}
	policy.ID = stableManagementID("metric-retention", scope)
	_, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO metric_retention_policies
			(id,scope_key,target_id,high_precision_days,manual_cleanup_enabled,notes)
		VALUES (?, ?, NULLIF(?,''), ?, ?, ?)
		ON DUPLICATE KEY UPDATE high_precision_days=VALUES(high_precision_days),
			manual_cleanup_enabled=VALUES(manual_cleanup_enabled),notes=VALUES(notes),updated_at=CURRENT_TIMESTAMP(3)`,
		policy.ID, scope, policy.TargetID, policy.HighPrecisionDays, policy.ManualCleanupEnabled, policy.Notes)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	saved, err := scanSingleDomainRetentionPolicy(s.db.QueryRowContext(c.Request.Context(), `
		SELECT id, COALESCE(target_id,''), high_precision_days, manual_cleanup_enabled,
		       COALESCE(notes,''), created_at, updated_at
		FROM metric_retention_policies WHERE scope_key=?`, scope))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "retention_policy.upsert", "metric_retention_policy", string(saved.ID))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteRetentionPolicy(c *gin.Context) {
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM metric_retention_policies WHERE id=?`, c.Param("policy_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed == 0 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "retention_policy.delete", "metric_retention_policy", c.Param("policy_id"))
	c.Status(http.StatusNoContent)
	c.Writer.WriteHeaderNow()
}

func scanSingleDomainRetentionPolicy(row rowScanner) (metricRetentionPolicy, error) {
	var value metricRetentionPolicy
	err := row.Scan(&value.ID, &value.TargetID, &value.HighPrecisionDays, &value.ManualCleanupEnabled,
		&value.Notes, &value.CreatedAt, &value.UpdatedAt)
	return value, err
}
