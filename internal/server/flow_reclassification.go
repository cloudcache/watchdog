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
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const (
	flowReclassificationJobType       = "flow_historical_reclassification"
	flowReclassificationPayloadSchema = 1
	flowReclassificationPageRows      = 5_000
)

type flowReclassification struct {
	ID, OperationJobID, RequestHash, SourcePublicationID, TargetPublicationID, View string
	WindowStart, WindowEnd                                                          time.Time
	Generation                                                                      uint64
	SourceDimensionSnapshotID                                                       string
	SourceClassificationVersion                                                     uint32
	TargetDimensionSnapshotID                                                       string
	TargetDimensionVersion                                                          uint64
	TargetDimensionObjectRef, TargetDimensionChecksum                               string
	TargetClassificationVersion                                                     uint32
	TargetClassificationObjectRef, TargetClassificationChecksum                     string
	Authorization                                                                   json.RawMessage
	Expected                                                                        flowch.ReclassificationEvidence
	Output                                                                          *flowch.ReclassificationEvidence
	ActivatedAt                                                                     sql.NullTime
	RowVersion                                                                      uint64
	CreatedBy                                                                       string
	CreatedAt                                                                       time.Time
	Job                                                                             opjob.Job
}

func (r flowReclassification) spec() flowch.ReclassificationSpec {
	return flowch.ReclassificationSpec{ID: r.ID, Generation: r.Generation, SourcePublicationID: r.SourcePublicationID, TargetPublicationID: r.TargetPublicationID, View: r.View,
		WindowStart: r.WindowStart, WindowEnd: r.WindowEnd, SourceDimensionSnapshotID: r.SourceDimensionSnapshotID, SourceClassificationVersion: r.SourceClassificationVersion}
}

type flowReclassificationCheckpoint struct {
	RunID     string                        `json:"run_id"`
	Cursor    flowch.ReclassificationCursor `json:"cursor"`
	Page      uint64                        `json:"page"`
	Processed uint64                        `json:"processed"`
}

type flowReclassificationCreateRequest struct {
	IdempotencyKey      string    `json:"idempotency_key"`
	SourcePublicationID string    `json:"source_publication_id"`
	TargetPublicationID string    `json:"target_publication_id"`
	View                string    `json:"view"`
	From                time.Time `json:"from"`
	To                  time.Time `json:"to"`
}

func (s *Server) registerFlowReclassificationRoutes(auth *gin.RouterGroup) {
	routes := auth.Group("/flow/reclassifications")
	routes.GET("", s.requirePermission("flow.reclassify"), s.listFlowReclassifications)
	routes.POST("", s.requirePermission("flow.reclassify"), s.createFlowReclassification)
	routes.GET("/:id", s.requirePermission("flow.reclassify"), s.getFlowReclassification)
	routes.POST("/:id/query", s.requirePermission("flow.reclassify"), s.queryFlowReclassification)
	routes.POST("/:id/cancel", s.requirePermission("flow.reclassify"), s.cancelFlowReclassification)
}

func (s *Server) startFlowReclassification() error {
	if s.clickHouse == nil {
		return nil
	}
	if s.jobs == nil || s.db == nil {
		return errors.New("Flow reclassification management dependencies are not initialized")
	}
	runner, err := flowch.NewReclassificationRunner(s.clickHouse)
	if err != nil {
		return err
	}
	if err := runner.Ready(context.Background()); err != nil {
		return err
	}
	s.flowReclassificationRunner = runner
	ctx, cancel := context.WithCancel(context.Background())
	s.flowReclassificationCancel = cancel
	worker := &opjob.Worker{Repo: s.jobs, JobType: flowReclassificationJobType, Owner: "watchdog-server/flow-reclassification", Handler: s.flowReclassificationHandler(runner), Logf: log.Printf, MaxAttempts: 5}
	go worker.Run(ctx)
	return nil
}

func (s *Server) createFlowReclassification(c *gin.Context) {
	if s.flowReclassificationRunner == nil {
		fail(c, http.StatusServiceUnavailable, "flow_reclassification_unavailable", "ClickHouse reclassification worker is unavailable")
		return
	}
	var request flowReclassificationCreateRequest
	if !addressDecodeStrict(c, &request, 64<<10) {
		return
	}
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.SourcePublicationID = strings.TrimSpace(request.SourcePublicationID)
	request.TargetPublicationID = strings.TrimSpace(request.TargetPublicationID)
	request.View = strings.TrimSpace(request.View)
	if request.IdempotencyKey == "" || len(request.IdempotencyKey) > 128 || request.SourcePublicationID == "" || request.TargetPublicationID == "" || (request.View != "customer" && request.View != "supplier") {
		fail(c, http.StatusBadRequest, "invalid_request", "idempotency_key, source/target publication, and customer/supplier view are required")
		return
	}
	_, fromOffset := request.From.Zone()
	_, toOffset := request.To.Zone()
	if request.From.IsZero() || request.To.IsZero() || fromOffset != 0 || toOffset != 0 || !request.From.Before(request.To) || request.To.Sub(request.From) > 31*24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_request", "window must be UTC, non-empty, and no longer than 31 days")
		return
	}
	source, err := s.readFlowEnrichmentPublication(c.Request.Context(), request.SourcePublicationID, false)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	target, err := s.readFlowEnrichmentPublication(c.Request.Context(), request.TargetPublicationID, false)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if source.ID == target.ID {
		fail(c, http.StatusBadRequest, "invalid_request", "source and target publications must differ")
		return
	}
	canonical := struct {
		SourcePublicationID, TargetPublicationID, View string
		From, To                                       time.Time
	}{source.ID, target.ID, request.View, request.From.UTC(), request.To.UTC()}
	encoded, _ := json.Marshal(canonical)
	digest := sha256.Sum256(encoded)
	requestHash := hex.EncodeToString(digest[:])
	if existing, readErr := s.readFlowReclassificationByHash(c.Request.Context(), requestHash); readErr == nil {
		c.JSON(http.StatusOK, flowReclassificationResponse(existing))
		return
	} else if !errors.Is(readErr, sql.ErrNoRows) {
		writeSQLError(c, readErr)
		return
	}
	if blocked, blockErr := s.rawDeletionOverlapsReclassification(c.Request.Context(), request.From.UTC(), request.To.UTC()); blockErr != nil {
		writeSQLError(c, blockErr)
		return
	} else if blocked {
		fail(c, http.StatusConflict, "raw_deletion_pending", "an approved raw deletion overlaps the requested reclassification window")
		return
	}
	if _, _, err = s.loadReclassificationSnapshots(target); err != nil {
		fail(c, http.StatusConflict, "target_publication_unavailable", err.Error())
		return
	}
	runID := newID()
	spec := flowch.ReclassificationSpec{ID: runID, Generation: 1, SourcePublicationID: source.ID, TargetPublicationID: target.ID, View: request.View, WindowStart: request.From.UTC(), WindowEnd: request.To.UTC(), SourceDimensionSnapshotID: source.DimensionSnapshotID, SourceClassificationVersion: source.ClassificationVersion}
	evidence, err := s.flowReclassificationRunner.SourceEvidence(c.Request.Context(), spec)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	if evidence.RecordCount == 0 {
		fail(c, http.StatusConflict, "source_facts_unavailable", "the source publication has no retained raw facts in this window")
		return
	}
	authorization := flowReclassificationAuthorization(currentPrincipal(c))
	authJSON, _ := json.Marshal(authorization)
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(c.Request.Context(), `UPDATE flow_reclassification_sequence SET next_generation=LAST_INSERT_ID(next_generation+1) WHERE id=1`); err != nil {
		writeSQLError(c, err)
		return
	}
	var generation uint64
	if err = tx.QueryRowContext(c.Request.Context(), `SELECT LAST_INSERT_ID()`).Scan(&generation); err != nil {
		writeSQLError(c, err)
		return
	}
	spec.Generation = generation
	cp := flowReclassificationCheckpoint{RunID: runID}
	checkpoint, err := opjob.EncodePayload(flowReclassificationPayloadSchema, cp)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	job, err := opjob.EnqueueTx(c.Request.Context(), tx, opjob.Job{JobType: flowReclassificationJobType, IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, ProgressTotal: evidence.RecordCount, CheckpointJSON: checkpoint, CreatedBy: currentPrincipal(c).UserID})
	if err != nil {
		if errors.Is(err, opjob.ErrHashMismatch) {
			fail(c, http.StatusConflict, "idempotency_conflict", "idempotency key was already used with a different request")
			return
		}
		writeSQLError(c, err)
		return
	}
	_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO flow_reclassifications (
id,operation_job_id,request_hash,source_publication_id,target_publication_id,value_view,window_start,window_end,generation,
source_dimension_snapshot_id,source_classification_version,target_dimension_snapshot_id,target_dimension_version,target_dimension_object_ref,target_dimension_checksum,
target_classification_version,target_classification_object_ref,target_classification_checksum,authorization_json,
expected_record_count,expected_raw_bytes,expected_raw_packets,expected_estimated_bytes,expected_estimated_packets,expected_estimated_valid_records,created_by)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS JSON),?,?,?,?,?,?,?)`, runID, job.ID, requestHash, source.ID, target.ID, request.View, request.From.UTC(), request.To.UTC(), generation,
		source.DimensionSnapshotID, source.ClassificationVersion, target.DimensionSnapshotID, target.DimensionVersion, target.DimensionObjectRef, target.DimensionChecksum,
		target.ClassificationVersion, target.ClassificationObjectRef, target.ClassificationChecksum, string(authJSON), evidence.RecordCount, evidence.RawBytes, evidence.RawPackets, evidence.EstimatedBytes, evidence.EstimatedPackets, evidence.EstimatedValidRecords, currentPrincipal(c).UserID)
	if err != nil {
		var duplicate *mysqldriver.MySQLError
		if errors.As(err, &duplicate) && duplicate.Number == 1062 {
			_ = tx.Rollback()
			existing, readErr := s.readFlowReclassificationByHash(c.Request.Context(), requestHash)
			if readErr != nil {
				writeSQLError(c, readErr)
				return
			}
			c.JSON(http.StatusOK, flowReclassificationResponse(existing))
			return
		}
		writeSQLError(c, err)
		return
	}
	if err = tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	run, err := s.readFlowReclassification(c.Request.Context(), runID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, flowReclassificationResponse(run))
}

func (s *Server) rawDeletionOverlapsReclassification(ctx context.Context, from, to time.Time) (bool, error) {
	var count uint64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_deletion_approvals
WHERE storage_kind='raw' AND status='approved' AND partition_start < ? AND partition_end > ?`, to, from).Scan(&count)
	return count != 0, err
}

func flowReclassificationAuthorization(p *principal) any {
	abilities := []string{}
	if p != nil {
		for ability, enabled := range p.Abilities {
			if enabled {
				abilities = append(abilities, ability)
			}
		}
		sort.Strings(abilities)
		return struct {
			UserID, Username string
			IsAdmin          bool
			Abilities        []string
		}{p.UserID, p.Username, p.IsAdmin, abilities}
	}
	return struct{}{}
}

func (s *Server) flowReclassificationHandler(runner *flowch.ReclassificationRunner) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var checkpoint flowReclassificationCheckpoint
		if err := opjob.DecodePayload(job.CheckpointJSON, flowReclassificationPayloadSchema, &checkpoint); err != nil {
			return "", err
		}
		run, err := s.readFlowReclassification(ctx, checkpoint.RunID)
		if err != nil {
			return "", err
		}
		if run.ActivatedAt.Valid {
			exists, markerErr := runner.MarkerExists(ctx, run.spec())
			if markerErr != nil {
				return "", markerErr
			}
			if !exists {
				return "", opjob.TerminalError(errors.New("activated reclassification has no ClickHouse marker"))
			}
			return flowReclassificationResultRef(run), nil
		}
		dimension, classification, err := s.loadReclassificationSnapshotsFromRun(run)
		if err != nil {
			return "", opjob.TerminalError(err)
		}
		source, err := runner.SourceEvidence(ctx, run.spec())
		if err != nil {
			return "", err
		}
		if !source.Equal(run.Expected) {
			return "", opjob.TerminalError(errors.New("source raw facts changed or were deleted after the job was frozen"))
		}
		for {
			page, readErr := runner.ReadSourcePage(ctx, run.spec(), checkpoint.Cursor, flowReclassificationPageRows)
			if readErr != nil {
				return "", readErr
			}
			if len(page.Batches) > 0 {
				for index, raw := range page.Records {
					enriched, enrichErr := flowworker.ReclassifyRecord(raw, dimension, classification)
					if enrichErr != nil {
						return "", opjob.TerminalError(enrichErr)
					}
					page.Batches[index].Records[0] = enriched
				}
				if err := runner.InsertPage(ctx, run.spec(), checkpoint.Page+1, page.Batches); err != nil {
					return "", err
				}
				checkpoint.Page++
				checkpoint.Processed += uint64(len(page.Batches))
				checkpoint.Cursor = page.Cursor
				encoded, encodeErr := opjob.EncodePayload(flowReclassificationPayloadSchema, checkpoint)
				if encodeErr != nil {
					return "", opjob.TerminalError(encodeErr)
				}
				if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
					if err := reporter.Report(ctx, checkpoint.Processed, encoded); err != nil {
						return "", err
					}
				}
			}
			if page.Done {
				break
			}
		}
		sourceAfter, err := runner.SourceEvidence(ctx, run.spec())
		if err != nil {
			return "", err
		}
		if !sourceAfter.Equal(run.Expected) {
			return "", opjob.TerminalError(errors.New("source raw facts changed during reclassification"))
		}
		output, err := runner.OutputEvidence(ctx, run.spec())
		if err != nil {
			return "", err
		}
		if !output.Equal(run.Expected) {
			return "", opjob.TerminalError(fmt.Errorf("reclassification counters do not conserve source facts: expected=%+v output=%+v", run.Expected, output))
		}
		diff, err := runner.CoordinateDiff(ctx, run.spec())
		if err != nil {
			return "", err
		}
		if diff != 0 {
			return "", opjob.TerminalError(fmt.Errorf("reclassification Kafka coordinate coverage differs by %d rows", diff))
		}
		if err := runner.WriteCompletionMarker(ctx, run.spec()); err != nil {
			return "", err
		}
		exists, err := runner.MarkerExists(ctx, run.spec())
		if err != nil {
			return "", err
		}
		if !exists {
			return "", errors.New("completion marker was not durably visible")
		}
		if err := s.activateFlowReclassification(ctx, run, job.LeaseToken, output); err != nil {
			return "", err
		}
		return flowReclassificationResultRef(run), nil
	}
}

func (s *Server) loadReclassificationSnapshots(publication flowEnrichmentPublication) (flowworker.DimensionSnapshot, *flowdimension.ClassificationSnapshot, error) {
	dimensionPath, err := s.addressObjects.ResolveDimensionObject(publication.DimensionObjectRef)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve target AddressSnap: %w", err)
	}
	dimensionData, err := os.ReadFile(dimensionPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read target AddressSnap: %w", err)
	}
	dimension, err := flowdimension.DecodeAndCompileAddressSnapshot(dimensionData, publication.DimensionChecksum, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		return nil, nil, fmt.Errorf("compile target AddressSnap: %w", err)
	}
	classificationPath, err := s.addressObjects.ResolveDimensionObject(publication.ClassificationObjectRef)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve target classification: %w", err)
	}
	classificationData, err := os.ReadFile(classificationPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read target classification: %w", err)
	}
	classification, err := flowdimension.DecodeAndCompileClassificationBundle(classificationData, publication.ClassificationChecksum, flowdimension.ClassificationCompileLimits{})
	if err != nil {
		return nil, nil, fmt.Errorf("compile target classification: %w", err)
	}
	if dimension.Metadata().SnapshotID != publication.DimensionSnapshotID || classification.Metadata().Version != publication.ClassificationVersion {
		return nil, nil, errors.New("target publication object metadata does not match its immutable row")
	}
	return dimension, classification, nil
}

func (s *Server) loadReclassificationSnapshotsFromRun(run flowReclassification) (flowworker.DimensionSnapshot, *flowdimension.ClassificationSnapshot, error) {
	publication := flowEnrichmentPublication{ID: run.TargetPublicationID, DimensionSnapshotID: run.TargetDimensionSnapshotID, DimensionVersion: run.TargetDimensionVersion, DimensionObjectRef: run.TargetDimensionObjectRef, DimensionChecksum: run.TargetDimensionChecksum, ClassificationVersion: run.TargetClassificationVersion, ClassificationObjectRef: run.TargetClassificationObjectRef, ClassificationChecksum: run.TargetClassificationChecksum}
	return s.loadReclassificationSnapshots(publication)
}

func (s *Server) activateFlowReclassification(ctx context.Context, run flowReclassification, leaseToken string, output flowch.ReclassificationEvidence) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var activated sql.NullTime
	var status, currentToken string
	if err := tx.QueryRowContext(ctx, `SELECT r.activated_at,j.status,COALESCE(j.lease_token,'')
FROM flow_reclassifications r JOIN operation_jobs j ON j.id=r.operation_job_id
WHERE r.id=? AND r.generation=? FOR UPDATE`, run.ID, run.Generation).Scan(&activated, &status, &currentToken); err != nil {
		return err
	}
	if activated.Valid {
		return tx.Commit()
	}
	if status == opjob.StatusCancelRequested || status == opjob.StatusCanceled {
		return opjob.ErrCancelRequested
	}
	if status != opjob.StatusRunning || leaseToken == "" || currentToken != leaseToken {
		return opjob.ErrLeaseLost
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_reclassifications SET output_record_count=?,output_raw_bytes=?,output_raw_packets=?,output_estimated_bytes=?,output_estimated_packets=?,output_estimated_valid_records=?,activated_at=UTC_TIMESTAMP(3),row_version=row_version+1 WHERE id=? AND generation=? AND activated_at IS NULL`, output.RecordCount, output.RawBytes, output.RawPackets, output.EstimatedBytes, output.EstimatedPackets, output.EstimatedValidRecords, run.ID, run.Generation); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Server) readFlowReclassification(ctx context.Context, id string) (flowReclassification, error) {
	return s.scanFlowReclassification(s.db.QueryRowContext(ctx, flowReclassificationSelect+` WHERE r.id=?`, id))
}
func (s *Server) readFlowReclassificationByHash(ctx context.Context, hash string) (flowReclassification, error) {
	return s.scanFlowReclassification(s.db.QueryRowContext(ctx, flowReclassificationSelect+` WHERE r.request_hash=?`, hash))
}

var flowReclassificationSelect = `SELECT r.id,r.operation_job_id,r.request_hash,r.source_publication_id,r.target_publication_id,r.value_view,r.window_start,r.window_end,r.generation,
r.source_dimension_snapshot_id,r.source_classification_version,r.target_dimension_snapshot_id,r.target_dimension_version,r.target_dimension_object_ref,r.target_dimension_checksum,
r.target_classification_version,r.target_classification_object_ref,r.target_classification_checksum,r.authorization_json,
r.expected_record_count,CAST(r.expected_raw_bytes AS CHAR),CAST(r.expected_raw_packets AS CHAR),CAST(r.expected_estimated_bytes AS CHAR),CAST(r.expected_estimated_packets AS CHAR),r.expected_estimated_valid_records,
r.output_record_count,CAST(r.output_raw_bytes AS CHAR),CAST(r.output_raw_packets AS CHAR),CAST(r.output_estimated_bytes AS CHAR),CAST(r.output_estimated_packets AS CHAR),r.output_estimated_valid_records,
r.activated_at,r.row_version,COALESCE(r.created_by,''),r.created_at,` + jobSelectColumns() + ` FROM flow_reclassifications r JOIN operation_jobs j ON j.id=r.operation_job_id`

func jobSelectColumns() string {
	return `j.id,j.job_type,j.status,j.idempotency_key,j.request_hash,j.progress_total,j.progress_done,j.checkpoint_json,COALESCE(j.result_ref,''),COALESCE(j.lease_owner,''),COALESCE(j.lease_token,''),j.lease_expires_at,j.next_attempt_at,j.attempt_count,COALESCE(j.last_error_code,''),COALESCE(j.last_error_detail,''),j.row_version,COALESCE(j.created_by,''),j.created_at,j.started_at,j.cancel_requested_at,j.finished_at`
}

type flowReclassificationScanner interface{ Scan(...any) error }

func (s *Server) scanFlowReclassification(row flowReclassificationScanner) (flowReclassification, error) {
	var r flowReclassification
	var auth []byte
	var outputCount, outputValid sql.NullInt64
	var outRaw, outPackets, outEst, outEstPackets sql.NullString
	var leaseExpires, started, canceled, finished sql.NullTime
	var checkpoint []byte
	err := row.Scan(&r.ID, &r.OperationJobID, &r.RequestHash, &r.SourcePublicationID, &r.TargetPublicationID, &r.View, &r.WindowStart, &r.WindowEnd, &r.Generation, &r.SourceDimensionSnapshotID, &r.SourceClassificationVersion, &r.TargetDimensionSnapshotID, &r.TargetDimensionVersion, &r.TargetDimensionObjectRef, &r.TargetDimensionChecksum, &r.TargetClassificationVersion, &r.TargetClassificationObjectRef, &r.TargetClassificationChecksum, &auth,
		&r.Expected.RecordCount, &r.Expected.RawBytes, &r.Expected.RawPackets, &r.Expected.EstimatedBytes, &r.Expected.EstimatedPackets, &r.Expected.EstimatedValidRecords,
		&outputCount, &outRaw, &outPackets, &outEst, &outEstPackets, &outputValid, &r.ActivatedAt, &r.RowVersion, &r.CreatedBy, &r.CreatedAt,
		&r.Job.ID, &r.Job.JobType, &r.Job.Status, &r.Job.IdempotencyKey, &r.Job.RequestHash, &r.Job.ProgressTotal, &r.Job.ProgressDone, &checkpoint, &r.Job.ResultRef, &r.Job.LeaseOwner, &r.Job.LeaseToken, &leaseExpires, &r.Job.NextAttemptAt, &r.Job.AttemptCount, &r.Job.LastErrorCode, &r.Job.LastErrorDetail, &r.Job.RowVersion, &r.Job.CreatedBy, &r.Job.CreatedAt, &started, &canceled, &finished)
	if err != nil {
		return r, err
	}
	r.Authorization = auth
	r.Job.CheckpointJSON = checkpoint
	r.Job.LeaseExpiresAt = leaseExpires.Time
	r.Job.StartedAt = started.Time
	r.Job.CancelRequestedAt = canceled.Time
	r.Job.FinishedAt = finished.Time
	if outputCount.Valid && outputValid.Valid && outRaw.Valid && outPackets.Valid && outEst.Valid && outEstPackets.Valid {
		r.Output = &flowch.ReclassificationEvidence{RecordCount: uint64(outputCount.Int64), RawBytes: outRaw.String, RawPackets: outPackets.String, EstimatedBytes: outEst.String, EstimatedPackets: outEstPackets.String, EstimatedValidRecords: uint64(outputValid.Int64)}
	}
	return r, nil
}

func flowReclassificationResponse(r flowReclassification) gin.H {
	return gin.H{"id": r.ID, "operation_job_id": r.OperationJobID, "status": r.Job.Status, "progress_total": r.Job.ProgressTotal, "progress_done": r.Job.ProgressDone, "source_publication_id": r.SourcePublicationID, "target_publication_id": r.TargetPublicationID, "view": r.View, "from": r.WindowStart, "to": r.WindowEnd, "generation": r.Generation, "expected": r.Expected, "output": r.Output, "activated_at": nullableTime(r.ActivatedAt), "last_error_code": r.Job.LastErrorCode, "last_error_detail": r.Job.LastErrorDetail, "row_version": r.RowVersion, "created_by": r.CreatedBy, "created_at": r.CreatedAt}
}
func flowReclassificationResultRef(r flowReclassification) string {
	return fmt.Sprintf("flow-reclassification:%s:%d", r.ID, r.Generation)
}

func (s *Server) getFlowReclassification(c *gin.Context) {
	r, err := s.readFlowReclassification(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, flowReclassificationResponse(r))
}

func (s *Server) queryFlowReclassification(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	if s.flowReclassificationRunner == nil {
		fail(c, http.StatusServiceUnavailable, "flow_reclassification_unavailable", "historical reclassification requires migrated ClickHouse tables")
		return
	}
	run, err := s.readFlowReclassification(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !run.ActivatedAt.Valid {
		fail(c, http.StatusConflict, "reclassification_not_active", "historical reclassification is not active")
		return
	}
	marker, err := s.flowReclassificationRunner.MarkerExists(c.Request.Context(), run.spec())
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	if !marker {
		fail(c, http.StatusConflict, "reclassification_not_active", "historical reclassification completion marker is unavailable")
		return
	}
	var envelope flowQueryEnvelope
	if !addressDecodeStrict(c, &envelope, 272<<10) {
		return
	}
	requestedView := envelope.ValueLayer
	if requestedView == "" {
		requestedView = flowquery.View(run.View)
	}
	view, ok := s.authorizeFlowView(c, requestedView)
	if !ok {
		return
	}
	if string(view) != run.View {
		fail(c, http.StatusBadRequest, "invalid_request", "query value_layer must match the immutable reclassification view")
		return
	}
	if envelope.From.Before(run.WindowStart) || envelope.To.After(run.WindowEnd) {
		fail(c, http.StatusBadRequest, "invalid_request", "query range must be inside the immutable reclassification window")
		return
	}
	input := envelope.Parameters
	if input.Operator != nil || input.DirectionSplit || input.AddressSetFilter != nil || strings.TrimSpace(input.AddressSetEndpoint) != "" {
		fail(c, http.StatusBadRequest, "invalid_request", "historical reclassification query supports typed base-fact dimensions and filters only")
		return
	}
	if !s.authorizeFlowResourceFilters(c, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs) {
		return
	}
	if !s.requireMetricAccess(c, string(input.Metric)) {
		return
	}
	dimensions := append([]flowquery.Dimension(nil), input.Dimensions...)
	if len(dimensions) == 0 && input.Dimension != "" {
		dimensions = []flowquery.Dimension{input.Dimension}
	}
	var tableReq *flowTableRequest
	if input.Table != nil {
		if err := normalizeFlowTableRequest(input.Table); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.Table.timezone = input.Timezone
		tableReq = input.Table
	}
	compiled, err := flowquery.CompileJoint(flowquery.Scope{AllowedViews: []flowquery.View{view}}, flowquery.JointRequest{
		From: envelope.From, To: envelope.To, Interval: time.Duration(envelope.StepSeconds) * time.Second, TargetPoints: input.TargetPoints,
		Metric: input.Metric, Dimensions: dimensions, Filters: input.Filters, Filter: input.Filter,
		View: view, TopN: input.TopN, IncludeOther: input.IncludeOther, Timezone: input.Timezone, TimeWindows: input.TimeWindows,
		Reclassification: &flowquery.ReclassificationSource{ID: run.ID, Generation: run.Generation},
	}, time.Now().UTC())
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.joint.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	raw, err := marshalFlowJointResult(result, tableReq, s.flowGeo)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode historical reclassification result")
		return
	}
	s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.reclassification.query", "flow_reclassification", run.ID, nil)
	meta := flowQueryResultMeta(view, result.Metric.Unit, compiled.Plan.Source, input.Timezone, compiled.Plan.StepSeconds, 1, false, nil)
	meta["reclassification_id"] = run.ID
	meta["reclassification_generation"] = run.Generation
	meta["source_publication_id"] = run.SourcePublicationID
	meta["target_publication_id"] = run.TargetPublicationID
	c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": meta})
}

func (s *Server) cancelFlowReclassification(c *gin.Context) {
	activated, err := s.requestFlowReclassificationCancel(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if activated {
		fail(c, http.StatusConflict, "already_activated", "an activated generation cannot be canceled")
		return
	}
	r, err := s.readFlowReclassification(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, flowReclassificationResponse(r))
}

func (s *Server) requestFlowReclassificationCancel(ctx context.Context, id string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var jobID, status string
	var activated sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT r.operation_job_id,r.activated_at,j.status
FROM flow_reclassifications r JOIN operation_jobs j ON j.id=r.operation_job_id
WHERE r.id=? FOR UPDATE`, id).Scan(&jobID, &activated, &status); err != nil {
		return false, err
	}
	if activated.Valid {
		return true, nil
	}
	now := time.Now().UTC()
	switch status {
	case opjob.StatusQueued:
		_, err = tx.ExecContext(ctx, `UPDATE operation_jobs SET status='canceled',cancel_requested_at=?,finished_at=?,row_version=row_version+1,updated_at=CURRENT_TIMESTAMP(3) WHERE id=? AND status='queued'`, now, now, jobID)
	case opjob.StatusRunning:
		_, err = tx.ExecContext(ctx, `UPDATE operation_jobs SET status='cancel_requested',cancel_requested_at=COALESCE(cancel_requested_at,?),row_version=row_version+1,updated_at=CURRENT_TIMESTAMP(3) WHERE id=? AND status='running'`, now, jobID)
	case opjob.StatusCancelRequested, opjob.StatusCanceled, opjob.StatusFailed:
		// Idempotent terminal/pending cancellation.
	case opjob.StatusSucceeded:
		return false, errors.New("succeeded reclassification is missing its activation row")
	default:
		return false, fmt.Errorf("unsupported reclassification job status %q", status)
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Server) listFlowReclassifications(c *gin.Context) {
	limit, offset := pageParams(c)
	where := ""
	args := []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = " WHERE (r.id LIKE ? OR r.source_publication_id LIKE ? OR r.target_publication_id LIKE ? OR j.status LIKE ?)"
		like := "%" + escapeLike(q) + "%"
		args = []any{like, like, like, like}
	}
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_reclassifications r JOIN operation_jobs j ON j.id=r.operation_job_id`+where, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), flowReclassificationSelect+where+` ORDER BY r.created_at DESC,r.id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		r, scanErr := s.scanFlowReclassification(rows)
		if scanErr != nil {
			writeSQLError(c, scanErr)
			return
		}
		items = append(items, flowReclassificationResponse(r))
	}
	if err = rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}
