// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

const (
	billingCalculateJobType = "billing.period.calculate"
	billingReconcileJobType = "billing.period.reconcile"
	billingJobPayloadSchema = 1
)

type billingJobPayload struct {
	PeriodID           string `json:"period_id"`
	ExpectedRowVersion uint64 `json:"expected_row_version"`
}

func (s *Server) startBillingJobs() {
	if s.billingService == nil || s.jobs == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.billingCancel = cancel
	for _, jobType := range []string{billingCalculateJobType, billingReconcileJobType} {
		worker := &opjob.Worker{Repo: s.jobs, JobType: jobType, Owner: "watchdog-server/" + jobType, Handler: s.runBillingJob(jobType)}
		go worker.Run(ctx)
	}
	exportWorker := &opjob.Worker{Repo: s.jobs, JobType: billingExportJobType, Owner: "watchdog-server/" + billingExportJobType, Handler: s.runBillingExport}
	go exportWorker.Run(ctx)
}

func (s *Server) runBillingJob(jobType string) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload billingJobPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, billingJobPayloadSchema, &payload); err != nil {
			return "", err
		}
		if payload.PeriodID == "" || payload.ExpectedRowVersion == 0 {
			return "", opjob.TerminalError(errors.New("invalid billing operation payload"))
		}
		abilities, admin, err := loadPrincipalAbilities(ctx, s.db, job.CreatedBy)
		if err != nil {
			return "", opjob.TerminalError(err)
		}
		ability := "bill.calculate"
		if jobType == billingReconcileJobType {
			ability = "bill.reconcile"
		}
		if !admin && !abilities[ability] {
			return "", opjob.TerminalError(errors.New("billing operation permission was revoked"))
		}
		period, err := s.billingStore.GetPeriod(ctx, payload.PeriodID)
		if err != nil {
			return "", opjob.TerminalError(err)
		}
		if !billingAccountAllowed(ctx, s.db, job.CreatedBy, period.AccountID, admin, abilities["bill.viewAll"]) {
			return "", opjob.TerminalError(errors.New("billing account access was revoked"))
		}
		if jobType == billingReconcileJobType {
			run, err := s.billingStore.ReconcileCurrent(ctx, period.ID, payload.ExpectedRowVersion, job.CreatedBy, job.ID)
			if err != nil {
				return "", classifyBillingJobError(err)
			}
			return "billing-period/" + period.ID + "/reconciliation/" + run.ID, nil
		}
		result, err := s.billingService.CalculateForOperation(ctx, period.ID, payload.ExpectedRowVersion, job.CreatedBy, job.ID)
		if err != nil {
			return "", classifyBillingJobError(err)
		}
		return "billing-period/" + period.ID + "/calculation/" + strconv.FormatUint(result.Period.CalculationVersion, 10), nil
	}
}

func classifyBillingJobError(err error) error {
	if errors.Is(err, billing.ErrNotFound) || errors.Is(err, billing.ErrConflict) || errors.Is(err, billing.ErrImmutable) || errors.Is(err, billing.ErrWindowOffset) {
		return opjob.TerminalError(err)
	}
	return err
}

func billingAccountAllowed(ctx context.Context, db *sql.DB, userID, accountID string, admin, viewAll bool) bool {
	if admin || viewAll {
		return true
	}
	var allowed bool
	return db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM user_billing_permissions WHERE user_id=? AND account_id=?)`, userID, accountID).Scan(&allowed) == nil && allowed
}

func (s *Server) enqueueBillingCalculation(c *gin.Context) {
	s.enqueueBillingOperation(c, billingCalculateJobType)
}
func (s *Server) enqueueBillingReconciliation(c *gin.Context) {
	s.enqueueBillingOperation(c, billingReconcileJobType)
}

func (s *Server) enqueueBillingOperation(c *gin.Context, jobType string) {
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
	if s.billingService == nil || s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "billing SNMP and Flow ClickHouse readers are not configured")
		return
	}
	var request struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	if c.Request.ContentLength > 0 && !addressDecodeStrict(c, &request, 8<<10) {
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	}
	if idempotencyKey == "" {
		idempotencyKey = newID()
	}
	if len(idempotencyKey) > 190 {
		fail(c, http.StatusBadRequest, "invalid_idempotency_key", "idempotency key exceeds 190 characters")
		return
	}
	payload, err := opjob.EncodePayload(billingJobPayloadSchema, billingJobPayload{PeriodID: period.ID, ExpectedRowVersion: expected})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	job, err := s.jobs.Enqueue(c, opjob.Job{JobType: jobType, IdempotencyKey: sha256hex(currentPrincipal(c).UserID + "\x00" + idempotencyKey),
		RequestHash: sha256hex(string(payload)), CheckpointJSON: payload, CreatedBy: currentPrincipal(c).UserID})
	if errors.Is(err, opjob.ErrHashMismatch) {
		fail(c, http.StatusConflict, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Location", "/api/v1/billing/jobs/"+job.ID)
	c.JSON(http.StatusAccepted, job)
}

func (s *Server) getBillingJob(c *gin.Context) {
	job, err := s.jobs.Get(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if job.JobType != billingCalculateJobType && job.JobType != billingReconcileJobType {
		fail(c, http.StatusNotFound, "not_found", "billing job not found")
		return
	}
	p := currentPrincipal(c)
	if job.CreatedBy != p.UserID && !p.can("bill.viewAll") {
		fail(c, http.StatusForbidden, "forbidden", "billing job is outside the caller's scope")
		return
	}
	c.Header("ETag", etag(job.RowVersion))
	c.JSON(http.StatusOK, job)
}

func (s *Server) cancelBillingJob(c *gin.Context) {
	job, err := s.jobs.Get(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	p := currentPrincipal(c)
	if (job.JobType != billingCalculateJobType && job.JobType != billingReconcileJobType) || (job.CreatedBy != p.UserID && !p.can("bill.viewAll")) {
		fail(c, http.StatusForbidden, "forbidden", "billing job cannot be canceled")
		return
	}
	ability := "bill.calculate"
	if job.JobType == billingReconcileJobType {
		ability = "bill.reconcile"
	}
	if !p.can(ability) {
		fail(c, http.StatusForbidden, "forbidden", "billing job cancel permission is missing")
		return
	}
	if err := s.jobs.RequestCancel(c, job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	job, _ = s.jobs.Get(c, job.ID)
	c.JSON(http.StatusAccepted, job)
}
