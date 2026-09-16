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

type flowBackupEvidenceInput struct {
	StorageKind     string `json:"storage_kind"`
	CoveredFrom     string `json:"covered_from"`
	CoveredThrough  string `json:"covered_through"`
	BackupRef       string `json:"backup_ref"`
	ChecksumSHA256  string `json:"checksum_sha256"`
	RestoreTestedAt string `json:"restore_tested_at"`
	RestoreTestRef  string `json:"restore_test_ref"`
}

func (request flowBackupEvidenceInput) evidence(id string) (flowlifecycle.BackupEvidence, error) {
	coveredFrom, err := time.Parse(time.DateOnly, strings.TrimSpace(request.CoveredFrom))
	if err != nil {
		return flowlifecycle.BackupEvidence{}, flowlifecycle.ErrInvalidBackupEvidence
	}
	coveredThrough, err := time.Parse(time.DateOnly, strings.TrimSpace(request.CoveredThrough))
	if err != nil {
		return flowlifecycle.BackupEvidence{}, flowlifecycle.ErrInvalidBackupEvidence
	}
	restoreTestedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(request.RestoreTestedAt))
	if err != nil {
		return flowlifecycle.BackupEvidence{}, flowlifecycle.ErrInvalidBackupEvidence
	}
	return flowlifecycle.BackupEvidence{
		ID: id, StorageKind: request.StorageKind, CoveredFrom: coveredFrom, CoveredThrough: coveredThrough,
		BackupRef: request.BackupRef, ChecksumSHA256: request.ChecksumSHA256,
		RestoreTestedAt: restoreTestedAt, RestoreTestRef: request.RestoreTestRef,
	}, nil
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
	storage.GET("/partitions/:date/delete-readiness", s.requirePermission("job.view"), s.getFlowRawDeleteReadiness)
	storage.POST("/partitions/:date/actions/approve-delete", s.requirePermission("job.manage"), s.approveFlowRawDelete)
	storage.GET("/watermarks", s.requirePermission("job.view"), s.listFlowReconciliationWatermarks)
	storage.GET("/deletion-receipts", s.requirePermission("job.view"), s.listFlowDeletionReceipts)
	storage.GET("/deletion-approvals", s.requirePermission("job.view"), s.listFlowDeletionApprovals)
	storage.GET("/deletion-approvals/:id", s.requirePermission("job.view"), s.getFlowDeletionApproval)
	storage.POST("/deletion-approvals/:id/actions/revoke", s.requirePermission("job.manage"), s.revokeFlowDeletionApproval)
	storage.GET("/backup-evidence", s.requirePermission("job.view"), s.listFlowBackupEvidence)
	storage.POST("/backup-evidence", s.requirePermission("job.manage"), s.createFlowBackupEvidence)
	storage.GET("/backup-evidence/:id", s.requirePermission("job.view"), s.getFlowBackupEvidence)
	storage.POST("/backup-evidence/:id/actions/revoke", s.requirePermission("job.manage"), s.revokeFlowBackupEvidence)
}

func (s *Server) listFlowDeletionApprovals(c *gin.Context) {
	limit, offset := pageParams(c)
	items, total, err := flowlifecycle.NewStore(s.db).ListDeletionApprovals(c.Request.Context(), flowlifecycle.DeletionApprovalFilter{
		StorageKind: strings.TrimSpace(c.Query("storage_kind")), Status: strings.TrimSpace(c.Query("status")),
		Search: strings.TrimSpace(c.Query("q")), Sort: strings.TrimSpace(c.Query("sort")),
		Order: strings.TrimSpace(c.Query("order")), Limit: limit, Offset: offset,
	})
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getFlowDeletionApproval(c *gin.Context) {
	approval, err := flowlifecycle.NewStore(s.db).GetDeletionApproval(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.Header("ETag", etag(approval.RowVersion))
	c.JSON(http.StatusOK, approval)
}

func (s *Server) approveFlowRawDelete(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	day, err := time.Parse(time.DateOnly, strings.TrimSpace(c.Param("date")))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_source_date", "source date must use YYYY-MM-DD")
		return
	}
	if s.flowLifecycle == nil || s.flowDeleteEvidence == nil {
		fail(c, http.StatusServiceUnavailable, "flow_lifecycle_unavailable", "Flow lifecycle evidence reader is unavailable")
		return
	}
	now := time.Now().UTC()
	readiness, err := s.flowLifecycle.RawDayDeleteReadiness(c.Request.Context(), day, now, s.flowDeleteEvidence)
	if err == nil && !readiness.EvidenceReady {
		err = flowlifecycle.ErrDeleteLocked
	}
	var approval flowlifecycle.DeletionApproval
	var partition flowlifecycle.PartitionState
	if err == nil {
		approval, partition, err = s.flowLifecycle.ApproveRawDayDelete(c.Request.Context(), readiness, expected, currentPrincipal(c).UserID, now)
	}
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_deletion_approval.create", "flow_deletion_approval", approval.ID)
	c.Header("ETag", etag(approval.RowVersion))
	c.JSON(http.StatusCreated, gin.H{"approval": approval, "partition": partition})
}

func (s *Server) revokeFlowDeletionApproval(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	approval, partition, err := flowlifecycle.NewStore(s.db).RevokeRawDayDeleteApproval(
		c.Request.Context(), c.Param("id"), expected, currentPrincipal(c).UserID, time.Now())
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_deletion_approval.revoke", "flow_deletion_approval", approval.ID)
	c.Header("ETag", etag(approval.RowVersion))
	c.JSON(http.StatusOK, gin.H{"approval": approval, "partition": partition})
}

func (s *Server) listFlowBackupEvidence(c *gin.Context) {
	limit, offset := pageParams(c)
	items, total, err := flowlifecycle.NewStore(s.db).ListBackupEvidence(c.Request.Context(), flowlifecycle.BackupEvidenceFilter{
		StorageKind: strings.TrimSpace(c.Query("storage_kind")), Status: strings.TrimSpace(c.Query("status")),
		Search: strings.TrimSpace(c.Query("q")), Sort: strings.TrimSpace(c.Query("sort")),
		Order: strings.TrimSpace(c.Query("order")), Limit: limit, Offset: offset,
	})
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getFlowBackupEvidence(c *gin.Context) {
	evidence, err := flowlifecycle.NewStore(s.db).GetBackupEvidence(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	c.Header("ETag", etag(evidence.RowVersion))
	c.JSON(http.StatusOK, evidence)
}

func (s *Server) createFlowBackupEvidence(c *gin.Context) {
	var request flowBackupEvidenceInput
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	evidence, err := request.evidence(newID())
	if err == nil {
		evidence, err = flowlifecycle.NewStore(s.db).CreateBackupEvidence(c.Request.Context(), evidence, currentPrincipal(c).UserID, time.Now())
	}
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_backup_evidence.create", "flow_backup_evidence", evidence.ID)
	c.Header("ETag", etag(evidence.RowVersion))
	c.JSON(http.StatusCreated, evidence)
}

func (s *Server) revokeFlowBackupEvidence(c *gin.Context) {
	expected, ok := requiredFlowLifecycleIfMatch(c)
	if !ok {
		return
	}
	evidence, err := flowlifecycle.NewStore(s.db).RevokeBackupEvidence(c.Request.Context(), c.Param("id"), currentPrincipal(c).UserID, expected, time.Now())
	if err != nil {
		writeFlowLifecycleError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow_backup_evidence.revoke", "flow_backup_evidence", evidence.ID)
	c.Header("ETag", etag(evidence.RowVersion))
	c.JSON(http.StatusOK, evidence)
}

func (s *Server) getFlowRawDeleteReadiness(c *gin.Context) {
	day, err := time.Parse(time.DateOnly, strings.TrimSpace(c.Param("date")))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_source_date", "source date must use YYYY-MM-DD")
		return
	}
	if s.flowLifecycle == nil || s.flowDeleteEvidence == nil {
		fail(c, http.StatusServiceUnavailable, "flow_lifecycle_unavailable", "Flow lifecycle evidence reader is unavailable")
		return
	}
	readiness, err := s.flowLifecycle.RawDayDeleteReadiness(c.Request.Context(), day, time.Now().UTC(), s.flowDeleteEvidence)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_lifecycle_evidence_failed", "Flow lifecycle evidence could not be verified")
		return
	}
	c.Header("ETag", etag(readiness.PartitionVersion))
	c.JSON(http.StatusOK, readiness)
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
	case errors.Is(err, flowlifecycle.ErrInvalidBackupEvidence):
		fail(c, http.StatusBadRequest, "invalid_flow_backup_evidence", err.Error())
	case errors.Is(err, flowlifecycle.ErrInvalidDeletionApproval):
		fail(c, http.StatusBadRequest, "invalid_flow_deletion_approval", err.Error())
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
		"source_record_count", "archive_record_count", "delete_approval_id", "delete_job_id", "delete_eligible_at", "raw_deleted_at", "row_version", "updated_at",
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
		"generation", "operation_job_id", "deletion_approval_id", "backup_evidence_id", "source_record_count", "ch_query_id", "status", "requested_at", "completed_at",
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
