// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

// exports.go is the generic export lifecycle surface at /api/v1/exports/* — the
// unified list/detail/download/cancel/retry/delete that the frontend drives for
// every export type (SNMP metric, flow query/report/detail, VPN findings). It
// restores the hub contract where one lifecycle served all export jobs: the
// handlers dispatch by opjob job type to the type-specific logic, so flow/vpn
// export jobs (job type "flow.export") are no longer invisible/404 behind the
// SNMP-only binding. The feature-specific create endpoints keep their own paths.

// exportJobTypes are the opjob job types surfaced through /api/v1/exports.
var exportJobTypes = []string{snmpCSVExportJobType, flowExportJobType}

// registerExportRoutes wires the generic export lifecycle. Detail/download/cancel
// delegate to the per-type handlers (which authorize); list/retry/delete are
// generic. POST "" stays the SNMP metric-export create.
func (s *Server) registerExportRoutes(auth *gin.RouterGroup) {
	exports := auth.Group("/exports")
	exports.GET("", s.listExports)
	exports.POST("", s.requirePermission("device.view"), s.createSNMPExport)
	exports.GET("/:id", s.getExport)
	exports.GET("/:id/download", s.downloadExport)
	exports.POST("/:id/cancel", s.cancelExport)
	exports.POST("/:id/retry", s.retryExport)
	exports.DELETE("/:id", s.deleteExport)
}

// exportView renders one export job in its type-specific shape (the frontend
// ExportTask type accepts each).
func (s *Server) exportView(job opjob.Job) any {
	switch job.JobType {
	case snmpCSVExportJobType:
		return s.snmpExportView(job)
	case flowExportJobType:
		return s.flowExportView(job)
	default:
		return gin.H{"id": job.ID, "operation_job_id": job.ID, "status": exportStatus(job.Status),
			"created_at": job.CreatedAt.UTC().Format(time.RFC3339Nano)}
	}
}

// listExports merges every export job type into one page, newest first, scoped to
// the caller (non-admins see only their own).
func (s *Server) listExports(c *gin.Context) {
	if s.jobs == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0, "limit": 0, "offset": 0})
		return
	}
	limit, offset := pageParams(c)
	createdBy := ""
	if p := currentPrincipal(c); !p.IsAdmin {
		createdBy = p.UserID
	}
	window := limit + offset
	var all []opjob.Job
	total := 0
	for _, jobType := range exportJobTypes {
		filter := opjob.Filter{JobType: jobType, CreatedBy: createdBy, Limit: window, Offset: 0}
		jobs, err := s.jobs.List(c.Request.Context(), filter)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		all = append(all, jobs...)
		count, err := s.jobs.Count(c.Request.Context(), opjob.Filter{JobType: jobType, CreatedBy: createdBy})
		if err != nil {
			writeSQLError(c, err)
			return
		}
		total += int(count)
	}
	page := mergeExportPage(all, limit, offset)
	items := make([]any, 0, len(page))
	for _, job := range page {
		items = append(items, s.exportView(job))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

// mergeExportPage sorts the union of per-type export jobs newest-first and returns
// the [offset, offset+limit) slice. Each type is fetched top-(limit+offset) already,
// so the global top of that window is complete within `jobs` (standard top-K merge).
func mergeExportPage(jobs []opjob.Job, limit, offset int) []opjob.Job {
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	start := min(offset, len(jobs))
	end := min(offset+limit, len(jobs))
	return jobs[start:end]
}

// getExport / downloadExport / cancelExport dispatch to the per-type handler, which
// performs the type-specific authorization and response.
func (s *Server) getExport(c *gin.Context) {
	switch s.exportJobType(c) {
	case snmpCSVExportJobType:
		s.getSNMPExport(c)
	case flowExportJobType:
		s.getFlowExport(c)
	}
}

func (s *Server) downloadExport(c *gin.Context) {
	switch s.exportJobType(c) {
	case snmpCSVExportJobType:
		s.downloadSNMPExport(c)
	case flowExportJobType:
		s.downloadFlowExport(c)
	}
}

func (s *Server) cancelExport(c *gin.Context) {
	switch s.exportJobType(c) {
	case snmpCSVExportJobType:
		s.cancelSNMPExport(c)
	case flowExportJobType:
		s.cancelFlowExport(c)
	}
}

// exportJobType resolves the job's type for dispatch, writing a 404/503 (and
// returning "") when the job is missing or not an export.
func (s *Server) exportJobType(c *gin.Context) string {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "unavailable", "export store is not configured")
		return ""
	}
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return ""
	}
	if job.JobType != snmpCSVExportJobType && job.JobType != flowExportJobType {
		fail(c, http.StatusNotFound, "not_found", "export not found")
		return ""
	}
	return job.JobType
}

// authorizedExportOwner resolves an export job for an owner-scoped action
// (retry/delete): it must be an export type and owned by the caller (or admin).
func (s *Server) authorizedExportOwner(c *gin.Context) (opjob.Job, bool) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "unavailable", "export store is not configured")
		return opjob.Job{}, false
	}
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return opjob.Job{}, false
	}
	p := currentPrincipal(c)
	if (job.JobType != snmpCSVExportJobType && job.JobType != flowExportJobType) || (!p.IsAdmin && job.CreatedBy != p.UserID) {
		fail(c, http.StatusNotFound, "not_found", "export not found")
		return opjob.Job{}, false
	}
	return job, true
}

// retryExport re-enqueues a fresh export job from the same frozen spec.
func (s *Server) retryExport(c *gin.Context) {
	job, ok := s.authorizedExportOwner(c)
	if !ok {
		return
	}
	retried, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType: job.JobType, IdempotencyKey: sha256hex(newID()),
		RequestHash: job.RequestHash, CheckpointJSON: job.CheckpointJSON,
		CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "export.retry", "operation_job", retried.ID)
	c.JSON(http.StatusAccepted, s.exportView(retried))
}

// deleteExport cancels the job if active, removes its artifact, and drops the row.
func (s *Server) deleteExport(c *gin.Context) {
	job, ok := s.authorizedExportOwner(c)
	if !ok {
		return
	}
	_ = s.jobs.RequestCancel(c.Request.Context(), job.ID)
	switch job.JobType {
	case snmpCSVExportJobType:
		if path, _, err := s.snmpExportArtifact(job); err == nil {
			_ = os.Remove(path)
		}
	case flowExportJobType:
		if path, _, _, err := s.flowExportArtifact(job); err == nil {
			_ = os.Remove(path)
		}
	}
	if _, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM operation_jobs WHERE id=?", job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "export.delete", "operation_job", job.ID)
	c.JSON(http.StatusOK, gin.H{"id": job.ID})
}
