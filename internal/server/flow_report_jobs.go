// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

const (
	flowReportQueryJobType        = "flow.report.query"
	flowReportQueryPayloadSchema  = 1
	flowReportQueryArtifactPrefix = "flow-report-query/"
)

type flowReportQueryPayload struct {
	View   flowquery.View    `json:"view"`
	Report flowReportRequest `json:"report"`
}

type flowReportQueryJobResponse struct {
	JobID       string `json:"job_id"`
	Status      string `json:"status"`
	PollAfterMS int64  `json:"poll_after_ms"`
}

func (s *Server) flowReportQueryNeedsAsync(req flowReportRequest) bool {
	return req.To.Sub(req.From) > s.cfg.Flow.Query.SynchronousMaxRange
}

func (s *Server) flowReportQueryResultDir() string {
	return filepath.Clean(s.cfg.Flow.Query.AsyncResultDir)
}

func (s *Server) enqueueFlowReportQuery(c *gin.Context, view flowquery.View, req flowReportRequest) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "flow_query_jobs_unavailable", "asynchronous flow query jobs are not configured")
		return
	}
	req.View = view
	payload, err := opjob.EncodePayload(flowReportQueryPayloadSchema, flowReportQueryPayload{View: view, Report: req})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	key := strings.TrimSpace(c.GetHeader("X-Request-ID"))
	if key == "" {
		key = newID()
	}
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType: flowReportQueryJobType, IdempotencyKey: sha256hex(currentPrincipal(c).UserID + "\x00" + key),
		RequestHash: sha256hex(string(payload)), CheckpointJSON: payload, CreatedBy: currentPrincipal(c).UserID,
	})
	if errors.Is(err, opjob.ErrHashMismatch) {
		fail(c, http.StatusConflict, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.report.query.enqueue", "operation_job", job.ID)
	c.JSON(http.StatusAccepted, s.flowReportQueryJobView(job))
}

func (s *Server) flowReportQueryJobView(job opjob.Job) flowReportQueryJobResponse {
	return flowReportQueryJobResponse{
		JobID: job.ID, Status: job.Status,
		PollAfterMS: s.cfg.Flow.Query.AsyncPollInterval.Milliseconds(),
	}
}

func (s *Server) runFlowReportQuery(dir string) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload flowReportQueryPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, flowReportQueryPayloadSchema, &payload); err != nil {
			return "", opjob.TerminalError(err)
		}
		if err := normalizeFlowReport(&payload.Report); err != nil || payload.Report.View != payload.View {
			if err == nil {
				err = errors.New("flow report query view does not match its payload")
			}
			return "", opjob.TerminalError(err)
		}
		abilities, isAdmin, err := loadPrincipalAbilities(ctx, s.db, job.CreatedBy)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", opjob.TerminalError(errors.New("flow report query subject is not authorized"))
			}
			return "", err
		}
		principal := &principal{UserID: job.CreatedBy, IsAdmin: isAdmin, Abilities: abilities}
		if err := s.authorizeFlowExportScope(ctx, principal, payload.View, payload.Report.Filters.TargetIDs, payload.Report.Filters.DeviceIDs, payload.Report.Filters.ExporterIDs); err != nil {
			return "", opjob.TerminalError(err)
		}
		if payload.Report.Kind == flowReportVPN && !principal.can("flow.vpn.view") {
			return "", opjob.TerminalError(errors.New("vpn reports require flow.vpn.view"))
		}
		now := time.Now().UTC()
		response, err := s.buildFlowReportResponse(ctx, flowquery.Scope{AllowedViews: []flowquery.View{payload.View}}, payload.View, payload.Report, now, principal.can("flow.vpn.view"))
		if err != nil {
			var requestErr *flowquery.RequestError
			if errors.As(err, &requestErr) {
				// Validation and ClickHouse resource guards are deterministic for
				// this immutable payload. Retrying the same SQL three times only
				// multiplies pressure and delays the actionable failure.
				return "", opjob.TerminalError(err)
			}
			return "", err
		}
		data, err := json.Marshal(response)
		if err != nil {
			return "", err
		}
		ref, err := writeFlowReportQueryArtifact(dir, job.ID, data)
		if err != nil {
			return "", err
		}
		s.auditFlowQuery(ctx, job.CreatedBy, "flow.report.query", "flow_report", string(payload.Report.Kind), payload.Report.Operator)
		return ref, nil
	}
}

func writeFlowReportQueryArtifact(dir, jobID string, data []byte) (string, error) {
	temporary, err := os.CreateTemp(dir, ".flow-report-query-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0600); err != nil {
		return "", err
	}
	if _, err := temporary.Write(data); err != nil {
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	filename := jobID + "-" + hex.EncodeToString(digest[:]) + ".json"
	if err := os.Rename(temporaryName, filepath.Join(dir, filename)); err != nil {
		return "", err
	}
	keep = true
	return flowReportQueryArtifactPrefix + filename, nil
}

func (s *Server) getFlowReportQuery(c *gin.Context) {
	if s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "flow_query_jobs_unavailable", "asynchronous flow query jobs are not configured")
		return
	}
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	p := currentPrincipal(c)
	if job.JobType != flowReportQueryJobType || (!p.IsAdmin && job.CreatedBy != p.UserID) {
		fail(c, http.StatusNotFound, "not_found", "flow report query not found")
		return
	}
	switch job.Status {
	case opjob.StatusQueued, opjob.StatusRunning, opjob.StatusCancelRequested:
		c.JSON(http.StatusAccepted, s.flowReportQueryJobView(job))
		return
	case opjob.StatusFailed:
		fail(c, http.StatusServiceUnavailable, "QUERY_JOB_FAILED", job.LastErrorDetail)
		return
	case opjob.StatusCanceled:
		fail(c, http.StatusConflict, "QUERY_JOB_CANCELED", "flow report query was canceled")
		return
	case opjob.StatusSucceeded:
	default:
		fail(c, http.StatusConflict, "QUERY_JOB_INVALID", "flow report query has an invalid state")
		return
	}
	if job.FinishedAt.IsZero() || time.Now().UTC().After(job.FinishedAt.Add(s.cfg.Flow.Query.AsyncResultRetention)) {
		fail(c, http.StatusGone, "QUERY_JOB_EXPIRED", "flow report query result has expired")
		return
	}
	path, checksum, err := s.flowReportQueryArtifact(job)
	if err != nil {
		fail(c, http.StatusConflict, "QUERY_JOB_CORRUPT", err.Error())
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fail(c, http.StatusGone, "QUERY_JOB_MISSING", "flow report query result is missing")
			return
		}
		writeSQLError(c, err)
		return
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != checksum || !json.Valid(data) {
		fail(c, http.StatusConflict, "QUERY_JOB_CORRUPT", "flow report query result checksum validation failed")
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", data)
}

func (s *Server) flowReportQueryArtifact(job opjob.Job) (string, string, error) {
	if !strings.HasPrefix(job.ResultRef, flowReportQueryArtifactPrefix) {
		return "", "", errors.New("invalid flow report query result reference")
	}
	filename := strings.TrimPrefix(job.ResultRef, flowReportQueryArtifactPrefix)
	if filepath.Base(filename) != filename || !strings.HasPrefix(filename, job.ID+"-") || filepath.Ext(filename) != ".json" {
		return "", "", errors.New("invalid flow report query result reference")
	}
	checksum := strings.TrimSuffix(strings.TrimPrefix(filename, job.ID+"-"), ".json")
	if len(checksum) != sha256.Size*2 {
		return "", "", errors.New("invalid flow report query checksum")
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return "", "", fmt.Errorf("invalid flow report query checksum: %w", err)
	}
	return filepath.Join(s.flowReportQueryResultDir(), filename), checksum, nil
}
