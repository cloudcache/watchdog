package server

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

// registerPlatformOperationsRoutes exposes the one MySQL-backed operation-job
// state machine and append-only audit log. The aliases retain the current UI
// paths while /jobs and /audit are the compact canonical names.
func (s *Server) registerPlatformOperationsRoutes(auth *gin.RouterGroup) {
	for _, path := range []string{"/jobs", "/operation-jobs"} {
		jobs := auth.Group(path)
		jobs.GET("", s.requirePermission("job.view"), s.listOperationJobs)
		jobs.GET("/:id", s.requirePermission("job.view"), s.getOperationJob)
		jobs.POST("/:id/actions/cancel", s.requirePermission("job.manage"), s.cancelOperationJob)
	}
	for _, path := range []string{"/audit", "/audit-logs"} {
		audit := auth.Group(path)
		audit.GET("", s.requirePermission("audit.view"), s.listAuditLogs)
	}
	retention := auth.Group("/retention/policies")
	retention.GET("", s.requirePermission("job.manage"), s.listRetentionPolicies)
	retention.PUT("", s.requirePermission("job.manage"), s.putRetentionPolicy)
	retention.DELETE("/:policy_id", s.requirePermission("job.manage"), s.deleteRetentionPolicy)
}

func (s *Server) getOperationJob(c *gin.Context) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "jobs_unavailable", "operation job store is unavailable")
		return
	}
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	principal := currentPrincipal(c)
	if principal == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return
	}
	if !principal.IsAdmin && !principal.can("job.manage") && job.CreatedBy != principal.UserID {
		fail(c, http.StatusForbidden, "forbidden", "operation job is outside the caller's scope")
		return
	}
	c.JSON(http.StatusOK, job)
}

func (s *Server) listOperationJobs(c *gin.Context) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "jobs_unavailable", "operation job store is unavailable")
		return
	}
	limit, offset := pageParams(c)
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			fail(c, http.StatusBadRequest, "invalid_request", "cursor must be a non-negative offset")
			return
		}
		offset = parsed
	}
	status := strings.TrimSpace(c.Query("status"))
	if status != "" && !validOperationJobStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid operation job status")
		return
	}
	filter := opjob.Filter{
		JobType: strings.TrimSpace(c.Query("job_type")),
		Status:  status,
		Search:  strings.TrimSpace(c.Query("q")),
		Limit:   limit,
		Offset:  offset,
	}
	principal := currentPrincipal(c)
	if principal == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return
	}
	if !principal.IsAdmin && !principal.can("job.manage") {
		filter.CreatedBy = principal.UserID
	}
	items, err := s.jobs.List(c.Request.Context(), filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if items == nil {
		items = []opjob.Job{}
	}
	total, err := s.jobs.Count(c.Request.Context(), filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	response := gin.H{"items": items, "total": total, "limit": limit, "offset": offset}
	if uint64(offset+len(items)) < total {
		response["next_cursor"] = strconv.Itoa(offset + len(items))
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) cancelOperationJob(c *gin.Context) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "jobs_unavailable", "operation job store is unavailable")
		return
	}
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	principal := currentPrincipal(c)
	if principal == nil || (!principal.IsAdmin && !principal.can("job.manage") && job.CreatedBy != principal.UserID) {
		fail(c, http.StatusForbidden, "forbidden", "operation job is outside the caller's scope")
		return
	}
	if job.Terminal() {
		fail(c, http.StatusConflict, "invalid_state", "terminal operation jobs cannot be canceled")
		return
	}
	if job.JobType == flowlifecycle.RawDeleteJobType {
		fail(c, http.StatusConflict, "destructive_job_not_cancelable", "a scheduled raw partition deletion cannot be canceled; revoke the approval before scheduling")
		return
	}
	if err := s.jobs.RequestCancel(c.Request.Context(), job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	job, err = s.jobs.Get(c.Request.Context(), job.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), principal.UserID, "operation_job.cancel", "operation_job", job.ID)
	c.JSON(http.StatusOK, job)
}

type auditLogView struct {
	ID            string         `json:"id"`
	ActorID       string         `json:"actor_id,omitempty"`
	ActorUsername string         `json:"actor_username,omitempty"`
	Action        string         `json:"action"`
	ResourceType  string         `json:"resource_type"`
	ResourceID    string         `json:"resource_id,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
	CreatedAt     string         `json:"created_at"`
}

func (s *Server) listAuditLogs(c *gin.Context) {
	limit, offset := pageParams(c)
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			fail(c, http.StatusBadRequest, "invalid_request", "cursor must be a non-negative offset")
			return
		}
		offset = parsed
	}
	resource := strings.TrimSpace(c.Query("resource_type"))
	resourceID := strings.TrimSpace(c.Query("resource_id"))
	actorID := strings.TrimSpace(c.Query("actor_id"))
	action := strings.TrimSpace(c.Query("action"))
	search := strings.TrimSpace(c.Query("q"))

	where := " WHERE 1=1"
	args := make([]any, 0, 8)
	if resource != "" {
		where += " AND a.resource = ?"
		args = append(args, resource)
	}
	if resourceID != "" {
		where += " AND a.resource_id = ?"
		args = append(args, resourceID)
	}
	if actorID != "" {
		where += " AND a.actor_id = ?"
		args = append(args, actorID)
	}
	if action != "" {
		where += " AND a.action LIKE ?"
		args = append(args, action+"%")
	}
	if search != "" {
		where += " AND (LOCATE(?, a.action)>0 OR LOCATE(?, a.resource)>0 OR LOCATE(?, a.resource_id)>0 OR LOCATE(?, COALESCE(a.actor_id, ''))>0 OR LOCATE(?, COALESCE(u.username, ''))>0)"
		args = append(args, search, search, search, search, search)
	}
	var total uint64
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM audit_logs a LEFT JOIN users u ON u.id=a.actor_id"+where, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT a.id, COALESCE(a.actor_id, ''), COALESCE(u.username, ''), a.action, a.resource,
		       a.resource_id, a.detail_json, a.occurred_at
		FROM audit_logs a LEFT JOIN users u ON u.id=a.actor_id`+where+`
		ORDER BY a.occurred_at DESC, a.id DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]auditLogView, 0, limit)
	for rows.Next() {
		var item auditLogView
		var detail sql.NullString
		var occurredAt sql.NullTime
		if err := rows.Scan(&item.ID, &item.ActorID, &item.ActorUsername, &item.Action, &item.ResourceType, &item.ResourceID, &detail, &occurredAt); err != nil {
			writeSQLError(c, err)
			return
		}
		if detail.Valid && strings.TrimSpace(detail.String) != "" {
			if err := json.Unmarshal([]byte(detail.String), &item.Detail); err != nil {
				fail(c, http.StatusInternalServerError, "invalid_audit_record", "audit detail is not valid JSON")
				return
			}
		}
		if occurredAt.Valid {
			item.CreatedAt = occurredAt.Time.UTC().Format("2006-01-02T15:04:05.000Z07:00")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	response := gin.H{"items": items, "total": total, "limit": limit, "offset": offset}
	if uint64(offset+len(items)) < total {
		response["next_cursor"] = strconv.Itoa(offset + len(items))
	}
	c.JSON(http.StatusOK, response)
}
