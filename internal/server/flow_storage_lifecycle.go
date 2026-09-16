package server

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/gin-gonic/gin"
)

type flowRetentionPolicyInput struct {
	BootstrapFrom             string `json:"bootstrap_from"`
	RawRetentionSeconds       uint64 `json:"raw_retention_seconds"`
	ArchiveRetentionSeconds   uint64 `json:"archive_retention_seconds"`
	LateArrivalSeconds        uint32 `json:"late_arrival_seconds"`
	DeleteGraceSeconds        uint32 `json:"delete_grace_seconds"`
	MaxPartitionsPerRun       uint32 `json:"max_partitions_per_run"`
	RawDeleteEnabled          bool   `json:"raw_delete_enabled"`
	ArchiveDeleteEnabled      bool   `json:"archive_delete_enabled"`
	RequireBackupBeforeDelete *bool  `json:"require_backup_before_delete"`
}

func (request flowRetentionPolicyInput) policy(id string) (flowlifecycle.Policy, error) {
	bootstrap, err := time.Parse("2006-01-02", strings.TrimSpace(request.BootstrapFrom))
	if err != nil {
		return flowlifecycle.Policy{}, flowlifecycle.ErrInvalidPolicy
	}
	requireBackup := true
	if request.RequireBackupBeforeDelete != nil {
		requireBackup = *request.RequireBackupBeforeDelete
	}
	return flowlifecycle.NormalizePolicy(flowlifecycle.Policy{
		ID: id, Version: 1, Status: flowlifecycle.PolicyDraft, BootstrapFrom: bootstrap,
		RawRetentionSeconds: request.RawRetentionSeconds, ArchiveRetentionSeconds: request.ArchiveRetentionSeconds,
		LateArrivalSeconds: request.LateArrivalSeconds, DeleteGraceSeconds: request.DeleteGraceSeconds,
		MaxPartitionsPerRun: request.MaxPartitionsPerRun, RawDeleteEnabled: request.RawDeleteEnabled,
		ArchiveDeleteEnabled: request.ArchiveDeleteEnabled, RequireBackupBeforeDelete: requireBackup,
	})
}

func (s *Server) registerFlowStorageLifecycleRoutes(auth *gin.RouterGroup) {
	policies := auth.Group("/flow/storage/policies")
	policies.GET("", s.requirePermission("job.view"), s.listFlowRetentionPolicies)
	policies.POST("", s.requirePermission("job.manage"), s.createFlowRetentionPolicy)
	policies.GET("/:id", s.requirePermission("job.view"), s.getFlowRetentionPolicy)
	policies.PATCH("/:id", s.requirePermission("job.manage"), s.updateFlowRetentionPolicy)
	policies.DELETE("/:id", s.requirePermission("job.manage"), s.deleteFlowRetentionPolicy)
	policies.POST("/:id/actions/publish", s.requirePermission("job.manage"), s.publishFlowRetentionPolicy)
	policies.POST("/:id/actions/retire", s.requirePermission("job.manage"), s.retireFlowRetentionPolicy)

	storage := auth.Group("/flow/storage")
	storage.GET("/partitions", s.requirePermission("job.view"), s.listFlowRetentionPartitions)
	storage.GET("/watermarks", s.requirePermission("job.view"), s.listFlowReconciliationWatermarks)
	storage.GET("/deletion-receipts", s.requirePermission("job.view"), s.listFlowDeletionReceipts)
}

func (s *Server) listFlowRetentionPolicies(c *gin.Context) {
	limit, offset := pageParams(c)
	items, total, err := flowlifecycle.NewStore(s.db).ListPolicies(c.Request.Context(), flowlifecycle.PolicyFilter{
		Status: strings.TrimSpace(c.Query("status")), Search: strings.TrimSpace(c.Query("q")),
		Sort: strings.TrimSpace(c.Query("sort")), Order: strings.TrimSpace(c.Query("order")), Limit: limit, Offset: offset,
	})
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getFlowRetentionPolicy(c *gin.Context) {
	policy, err := flowlifecycle.NewStore(s.db).GetPolicy(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.Header("ETag", etag(policy.RowVersion))
	c.JSON(http.StatusOK, policy)
}

func (s *Server) createFlowRetentionPolicy(c *gin.Context) {
	var request flowRetentionPolicyInput
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	policy, err := request.policy(newID())
	if err == nil {
		policy, err = flowlifecycle.NewStore(s.db).CreateDraft(c.Request.Context(), policy, currentPrincipal(c).UserID)
	}
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_retention_policy.create", "flow_retention_policy", policy.ID)
	c.Header("ETag", etag(policy.RowVersion))
	c.JSON(http.StatusCreated, policy)
}

func (s *Server) updateFlowRetentionPolicy(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	current, err := flowlifecycle.NewStore(s.db).GetPolicy(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	var request flowRetentionPolicyInput
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	policy, err := request.policy(current.ID)
	if err == nil {
		policy.Version = current.Version
		policy, err = flowlifecycle.NewStore(s.db).UpdateDraft(c.Request.Context(), policy, expected)
	}
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_retention_policy.update", "flow_retention_policy", policy.ID)
	c.Header("ETag", etag(policy.RowVersion))
	c.JSON(http.StatusOK, policy)
}

func (s *Server) deleteFlowRetentionPolicy(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	if err := flowlifecycle.NewStore(s.db).DeleteDraft(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_retention_policy.delete", "flow_retention_policy", c.Param("id"))
	c.Status(http.StatusNoContent)
}

func (s *Server) publishFlowRetentionPolicy(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	policy, err := flowlifecycle.NewStore(s.db).Publish(c.Request.Context(), c.Param("id"), currentPrincipal(c).UserID, expected, time.Now())
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_retention_policy.publish", "flow_retention_policy", policy.ID)
	c.Header("ETag", etag(policy.RowVersion))
	c.JSON(http.StatusOK, policy)
}

func (s *Server) retireFlowRetentionPolicy(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	policy, err := flowlifecycle.NewStore(s.db).Retire(c.Request.Context(), c.Param("id"), currentPrincipal(c).UserID, expected, time.Now())
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_retention_policy.retire", "flow_retention_policy", policy.ID)
	c.Header("ETag", etag(policy.RowVersion))
	c.JSON(http.StatusOK, policy)
}

func requiredFlowLifecycleIfMatch(c *gin.Context) (uint64, bool) {
	expected, supplied, err := ifMatch(c)
	if err != nil || (supplied && expected == 0) {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match must be a positive row version")
		return 0, false
	}
	if !supplied {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match is required")
		return 0, false
	}
	return expected, true
}

func writeFlowLifecycleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, flowlifecycle.ErrInvalidPolicy):
		fail(c, http.StatusBadRequest, "invalid_flow_retention_policy", err.Error())
	case errors.Is(err, flowlifecycle.ErrDeleteLocked):
		fail(c, http.StatusConflict, "flow_deletion_locked", "physical deletion remains locked until reconciliation and backup restore gates are enabled")
	case errors.Is(err, flowlifecycle.ErrVersionConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", "record changed since it was loaded")
	case errors.Is(err, flowlifecycle.ErrTransition):
		fail(c, http.StatusConflict, "invalid_state", err.Error())
	default:
		writeSQLError(c, err)
	}
}

func (s *Server) listFlowRetentionPartitions(c *gin.Context) {
	listFlowStorageRows(c, s.db, "flow_retention_partition_states", []string{
		"source_date", "policy_id", "policy_version", "state", "generation", "repair_attempt",
		"source_record_count", "archive_record_count", "delete_eligible_at", "raw_deleted_at", "row_version", "updated_at",
	}, "source_date", map[string]string{"state": "state", "policy_id": "policy_id"})
}

func (s *Server) listFlowReconciliationWatermarks(c *gin.Context) {
	listFlowStorageRows(c, s.db, "flow_reconciliation_watermarks", []string{
		"source_stream_id", "kafka_topic", "consumer_group", "kafka_partition", "bootstrap_offset",
		"committed_next_offset", "reconciled_next_offset", "status", "mismatch_count", "last_verified_at", "row_version", "updated_at",
	}, "updated_at", map[string]string{"status": "status", "kafka_topic": "kafka_topic", "consumer_group": "consumer_group"})
}

func (s *Server) listFlowDeletionReceipts(c *gin.Context) {
	listFlowStorageRows(c, s.db, "flow_deletion_receipts", []string{
		"id", "storage_kind", "partition_granularity", "partition_start", "partition_end", "policy_version",
		"generation", "operation_job_id", "backup_evidence_id", "source_record_count", "ch_query_id", "status", "requested_at", "completed_at",
	}, "requested_at", map[string]string{"status": "status", "storage_kind": "storage_kind", "operation_job_id": "operation_job_id"})
}

func listFlowStorageRows(c *gin.Context, db *sql.DB, table string, columns []string, defaultSort string, filters map[string]string) {
	limit, offset := pageParams(c)
	allowedSort := make(map[string]bool, len(columns))
	for _, column := range columns {
		allowedSort[column] = true
	}
	sortColumn := strings.TrimSpace(c.Query("sort"))
	if !allowedSort[sortColumn] {
		sortColumn = defaultSort
	}
	order := sortDirection(c)
	where := []string{"1=1"}
	args := make([]any, 0, len(filters)+2)
	for queryName, column := range filters {
		if value := strings.TrimSpace(c.Query(queryName)); value != "" {
			where = append(where, column+"=?")
			args = append(args, value)
		}
	}
	clause := strings.Join(where, " AND ")
	var total uint64
	if err := db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM "+table+" WHERE "+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := db.QueryContext(c.Request.Context(), "SELECT "+strings.Join(columns, ",")+" FROM "+table+" WHERE "+clause+" ORDER BY "+sortColumn+" "+order+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			writeSQLError(c, err)
			return
		}
		item := make(map[string]any, len(columns))
		for index, column := range columns {
			value := values[index]
			if raw, ok := value.([]byte); ok {
				value = string(raw)
			}
			item[column] = value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}
