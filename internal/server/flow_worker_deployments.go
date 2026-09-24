package server

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

const (
	flowWorkerDeploymentJobType       = "flow.worker.deployment.publish"
	flowWorkerDeploymentPayloadSchema = 1
	flowWorkerDeploymentPageLimit     = 100
)

type flowWorkerDeploymentRequest struct {
	WorkerIDs     []string  `json:"worker_ids"`
	DeviceIDs     []string  `json:"device_ids"`
	EffectiveFrom time.Time `json:"effective_from"`
}

type flowWorkerDeploymentPayload struct {
	WorkerIDs            []string          `json:"worker_ids"`
	DeviceIDs            []string          `json:"device_ids"`
	EffectiveFrom        string            `json:"effective_from"`
	AddressSnapshotID    string            `json:"address_snapshot_id"`
	ProfileRowVersion    uint64            `json:"profile_row_version"`
	ProfileDefinitionID  string            `json:"profile_definition_digest"`
	DeviceBoundaryDigest map[string]string `json:"device_boundary_digests"`
}

type flowArtifactRecord struct {
	ID, Kind, ScopeID, Format, Checksum, ObjectRef, SourceRevision string
	Version, SizeBytes                                             uint64
	FormatVersion                                                  uint16
	EffectiveFrom                                                  time.Time
}

type flowWorkerDeploymentACKRequest struct {
	Generation         uint64                       `json:"generation"`
	ManifestChecksum   string                       `json:"manifest_checksum"`
	BootID             string                       `json:"boot_id"`
	SoftwareVersion    string                       `json:"software_version"`
	State              string                       `json:"state"`
	FailureStage       string                       `json:"failure_stage,omitempty"`
	FailureCode        string                       `json:"failure_code,omitempty"`
	FailureMessage     string                       `json:"failure_message,omitempty"`
	InstalledArtifacts map[string]flowInstalledItem `json:"installed_artifacts,omitempty"`
}

type flowInstalledItem struct {
	Version  uint64 `json:"version"`
	Checksum string `json:"checksum"`
}

func (s *Server) validateFlowWorkerDeployment(c *gin.Context) {
	request, snapshot, profileVersion, profileDigest, boundaryDigests, err := s.resolveFlowWorkerDeploymentRequest(c.Request.Context(), c)
	if err != nil {
		writeFlowWorkerDeploymentError(c, err)
		return
	}
	recommended, err := s.recommendedFlowWorkers(c.Request.Context(), request.DeviceIDs)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"valid": true, "worker_ids": request.WorkerIDs, "device_ids": request.DeviceIDs,
		"address_snapshot_id": snapshot.ID, "address_version": snapshot.Version,
		"profile_row_version": profileVersion, "profile_definition_digest": profileDigest,
		"device_boundary_digests": boundaryDigests,
		"recommended_worker_ids":  recommended,
		"artifact_count":          2 + len(request.DeviceIDs),
	})
}

func (s *Server) publishFlowWorkerDeployment(c *gin.Context) {
	request, snapshot, profileVersion, profileDigest, boundaryDigests, err := s.resolveFlowWorkerDeploymentRequest(c.Request.Context(), c)
	if err != nil {
		writeFlowWorkerDeploymentError(c, err)
		return
	}
	payload := flowWorkerDeploymentPayload{
		WorkerIDs: request.WorkerIDs, DeviceIDs: request.DeviceIDs,
		EffectiveFrom: request.EffectiveFrom.Format(time.RFC3339Nano), AddressSnapshotID: snapshot.ID,
		ProfileRowVersion: profileVersion, ProfileDefinitionID: profileDigest, DeviceBoundaryDigest: boundaryDigests,
	}
	checkpoint, err := opjob.EncodePayload(flowWorkerDeploymentPayloadSchema, payload)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	job, err := opjob.EnqueueTx(c.Request.Context(), tx, opjob.Job{
		JobType: flowWorkerDeploymentJobType,
		IdempotencyKey: fmt.Sprintf("flow-deployment:%s:%d:%s:%s", snapshot.ID, profileVersion,
			sha256hex(strings.Join(request.WorkerIDs, ",")), sha256hex(strings.Join(request.DeviceIDs, ",")+request.EffectiveFrom.Format(time.RFC3339Nano))),
		RequestHash: sha256hex(string(checkpoint)), CheckpointJSON: checkpoint,
		ProgressTotal: uint64(2 + len(request.DeviceIDs) + len(request.WorkerIDs)), CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Location", "/api/v1/operation-jobs/"+job.ID)
	c.JSON(http.StatusAccepted, gin.H{"job": job})
}

// flowWorkerDeploymentResolution is a validated deployment target: normalized
// worker/device sets, the effective minute, and the snapshot/profile/boundary
// digests the deployment job pins.
type flowWorkerDeploymentResolution struct {
	WorkerIDs         []string
	DeviceIDs         []string
	EffectiveFrom     time.Time
	Snapshot          flowAddressSnapshotRef
	ProfileRowVersion uint64
	ProfileDigest     string
	BoundaryDigests   map[string]string
}

func (s *Server) resolveFlowWorkerDeploymentRequest(ctx context.Context, c *gin.Context) (flowWorkerDeploymentRequest, flowAddressSnapshotRef, uint64, string, map[string]string, error) {
	var request flowWorkerDeploymentRequest
	if !decodeStrictBody(c, &request) {
		return request, flowAddressSnapshotRef{}, 0, "", nil, errors.New("request body is invalid")
	}
	res, err := s.resolveFlowWorkerDeploymentValues(ctx, request.WorkerIDs, request.DeviceIDs, request.EffectiveFrom)
	if err != nil {
		return request, flowAddressSnapshotRef{}, 0, "", nil, err
	}
	request.WorkerIDs, request.DeviceIDs, request.EffectiveFrom = res.WorkerIDs, res.DeviceIDs, res.EffectiveFrom
	return request, res.Snapshot, res.ProfileRowVersion, res.ProfileDigest, res.BoundaryDigests, nil
}

// resolveFlowWorkerDeploymentValues validates the workers, device boundaries,
// classification profile and active WADS snapshot for a deployment. It is the
// shared core behind the manual publish endpoint and the automatic publish on
// address activation.
func (s *Server) resolveFlowWorkerDeploymentValues(ctx context.Context, workerIDs, deviceIDs []string, effectiveFrom time.Time) (flowWorkerDeploymentResolution, error) {
	res := flowWorkerDeploymentResolution{WorkerIDs: sortedUniqueStrings(workerIDs), DeviceIDs: sortedUniqueStrings(deviceIDs), EffectiveFrom: effectiveFrom}
	if len(res.WorkerIDs) == 0 || len(res.DeviceIDs) == 0 {
		return res, errors.New("worker_ids and device_ids must both be selected explicitly")
	}
	if res.EffectiveFrom.IsZero() {
		res.EffectiveFrom = time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	} else {
		res.EffectiveFrom = res.EffectiveFrom.UTC()
	}
	if !flowUTCMinute(res.EffectiveFrom) || res.EffectiveFrom.Before(time.Now().UTC().Truncate(time.Minute)) {
		return res, errors.New("effective_from must be a current or future UTC minute boundary")
	}
	// getActiveFlowAddressSnapshot locks the selected immutable snapshot so the
	// validation result cannot race an activation change. MySQL rejects that
	// SELECT ... FOR UPDATE inside an explicitly read-only transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()
	for _, workerID := range res.WorkerIDs {
		var kind, status string
		if err := tx.QueryRowContext(ctx, `SELECT kind,status FROM agents WHERE id=?`, workerID).Scan(&kind, &status); err != nil {
			return res, err
		}
		if kind != "flow_worker" || (status != "active" && status != "registered") {
			return res, fmt.Errorf("agent %s is not an active Flow worker", workerID)
		}
	}
	res.BoundaryDigests = make(map[string]string, len(res.DeviceIDs))
	for _, deviceID := range res.DeviceIDs {
		_, digest, err := readDeviceBoundarySource(ctx, tx, deviceID)
		if err != nil {
			return res, err
		}
		res.BoundaryDigests[deviceID] = digest
	}
	if err := tx.QueryRowContext(ctx, `SELECT row_version,definition_digest FROM flow_classification_profiles WHERE id=1`).Scan(&res.ProfileRowVersion, &res.ProfileDigest); err != nil {
		return res, err
	}
	snapshot, err := getActiveFlowAddressSnapshot(ctx, tx, res.EffectiveFrom)
	if err != nil {
		return res, err
	}
	if snapshot.Status != "active" || snapshot.ApprovalState != "approved" || snapshot.ObjectDeletedAt.Valid ||
		snapshot.ObjectFormat != "wads" || snapshot.ObjectFormatVersion != uint64(flowdimension.AddressSnapshotFormatVersion) {
		return res, errors.New("the selected effective time has no approved WADS address catalog")
	}
	res.Snapshot = snapshot
	return res, nil
}

// enqueueFlowWorkerDeploymentTx enqueues the deployment publish job for a
// resolved target inside the given transaction. Shared by the manual endpoint
// and the automatic publish on activation.
func (s *Server) enqueueFlowWorkerDeploymentTx(ctx context.Context, tx *sql.Tx, actor string, res flowWorkerDeploymentResolution) (opjob.Job, error) {
	payload := flowWorkerDeploymentPayload{
		WorkerIDs: res.WorkerIDs, DeviceIDs: res.DeviceIDs,
		EffectiveFrom: res.EffectiveFrom.Format(time.RFC3339Nano), AddressSnapshotID: res.Snapshot.ID,
		ProfileRowVersion: res.ProfileRowVersion, ProfileDefinitionID: res.ProfileDigest, DeviceBoundaryDigest: res.BoundaryDigests,
	}
	checkpoint, err := opjob.EncodePayload(flowWorkerDeploymentPayloadSchema, payload)
	if err != nil {
		return opjob.Job{}, err
	}
	return opjob.EnqueueTx(ctx, tx, opjob.Job{
		JobType: flowWorkerDeploymentJobType,
		IdempotencyKey: fmt.Sprintf("flow-deployment:%s:%d:%s:%s", res.Snapshot.ID, res.ProfileRowVersion,
			sha256hex(strings.Join(res.WorkerIDs, ",")), sha256hex(strings.Join(res.DeviceIDs, ",")+res.EffectiveFrom.Format(time.RFC3339Nano))),
		RequestHash: sha256hex(string(checkpoint)), CheckpointJSON: checkpoint,
		ProgressTotal: uint64(2 + len(res.DeviceIDs) + len(res.WorkerIDs)), CreatedBy: actor,
	})
}

// clampFlowDeploymentEffectiveFrom moves a requested deployment effective_from
// forward to a current-or-future UTC minute boundary. Activation effective_from
// values are frequently already in the past when a deployment is derived from
// them (an activation is effective immediately), and the deployment API requires
// a valid future minute; a zero value likewise becomes the next minute.
func clampFlowDeploymentEffectiveFrom(requested, now time.Time) time.Time {
	nextMinute := now.UTC().Truncate(time.Minute).Add(time.Minute)
	if requested.IsZero() {
		return nextMinute
	}
	requested = requested.UTC()
	minute := requested.Truncate(time.Minute)
	if !requested.Equal(minute) {
		minute = minute.Add(time.Minute)
	}
	if minute.Before(nextMinute) {
		return nextMinute
	}
	return minute
}

// autoPublishFlowWorkerDeploymentsForActivation closes the last publish hop when
// an address snapshot is activated: for every device already bound to a worker,
// it enqueues a deployment publish so the worker converges without a separate
// manual step. Best-effort — a failure here never fails the activation, and the
// worker still holds its previous LKG. One deployment per worker carries only
// that worker's bound devices.
func (s *Server) autoPublishFlowWorkerDeploymentsForActivation(ctx context.Context, actor string, effectiveFrom time.Time) (int, error) {
	// An activation takes effect immediately, so its effective_from is usually
	// already in the past by the time we build the deployment — but a worker
	// deployment must land on a current-or-future UTC minute boundary, or the
	// publish is rejected. Clamp it forward so auto-publish converges.
	effectiveFrom = clampFlowDeploymentEffectiveFrom(effectiveFrom, time.Now())
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id, device_id FROM flow_worker_device_bindings ORDER BY worker_id, device_id`)
	if err != nil {
		return 0, err
	}
	devicesByWorker := map[string][]string{}
	for rows.Next() {
		var workerID, deviceID string
		if err := rows.Scan(&workerID, &deviceID); err != nil {
			rows.Close()
			return 0, err
		}
		devicesByWorker[workerID] = append(devicesByWorker[workerID], deviceID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	workerIDs := make([]string, 0, len(devicesByWorker))
	for workerID := range devicesByWorker {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	published := 0
	failures := make([]error, 0)
	for _, workerID := range workerIDs {
		deviceIDs := devicesByWorker[workerID]
		res, err := s.resolveFlowWorkerDeploymentValues(ctx, []string{workerID}, deviceIDs, effectiveFrom)
		if err != nil {
			failures = append(failures, fmt.Errorf("worker %s: %w", workerID, err))
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			failures = append(failures, fmt.Errorf("worker %s: %w", workerID, err))
			continue
		}
		if _, err := s.enqueueFlowWorkerDeploymentTx(ctx, tx, actor, res); err != nil {
			tx.Rollback()
			failures = append(failures, fmt.Errorf("worker %s: %w", workerID, err))
			continue
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			failures = append(failures, fmt.Errorf("worker %s: %w", workerID, err))
			continue
		}
		published++
	}
	return published, errors.Join(failures...)
}

func (s *Server) recommendedFlowWorkers(ctx context.Context, deviceIDs []string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT worker_id FROM flow_worker_device_bindings WHERE device_id IN (`+sqlPlaceholders(len(deviceIDs))+`) ORDER BY worker_id`, stringSliceAny(deviceIDs)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func (s *Server) flowWorkerDeploymentHandler() opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload flowWorkerDeploymentPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, flowWorkerDeploymentPayloadSchema, &payload); err != nil {
			return "", err
		}
		effectiveFrom, err := time.Parse(time.RFC3339Nano, payload.EffectiveFrom)
		if err != nil || !flowUTCMinute(effectiveFrom) || len(payload.WorkerIDs) == 0 || len(payload.DeviceIDs) == 0 ||
			payload.AddressSnapshotID == "" || payload.ProfileRowVersion == 0 || payload.ProfileDefinitionID == "" ||
			len(payload.DeviceBoundaryDigest) != len(payload.DeviceIDs) {
			return "", opjob.TerminalError(errors.New("invalid Flow worker deployment payload"))
		}
		for _, deviceID := range payload.DeviceIDs {
			if !flowSHA256(payload.DeviceBoundaryDigest[deviceID]) {
				return "", opjob.TerminalError(errors.New("invalid Flow worker deployment boundary revision"))
			}
		}
		ids, err := s.createFlowWorkerDeployments(ctx, job, payload, effectiveFrom)
		if errors.Is(err, errFlowEnrichmentSuperseded) || errors.Is(err, sql.ErrNoRows) {
			return "", opjob.TerminalError(err)
		}
		if err != nil {
			return "", err
		}
		return "flow-deployments:" + strings.Join(ids, ","), nil
	}
}

func (s *Server) createFlowWorkerDeployments(ctx context.Context, job opjob.Job, payload flowWorkerDeploymentPayload, effectiveFrom time.Time) ([]string, error) {
	if len(s.agentPlanSigner.PrivateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("Flow deployment signing is unavailable")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var profileVersion uint64
	var profileDigest string
	var overseasIncludesHMT bool
	if err := tx.QueryRowContext(ctx, `SELECT row_version,definition_digest,overseas_includes_hmt FROM flow_classification_profiles WHERE id=1 FOR UPDATE`).
		Scan(&profileVersion, &profileDigest, &overseasIncludesHMT); err != nil {
		return nil, err
	}
	if profileVersion != payload.ProfileRowVersion || profileDigest != payload.ProfileDefinitionID {
		return nil, errFlowEnrichmentSuperseded
	}
	snapshot, err := readFlowAddressSnapshotByID(ctx, tx, payload.AddressSnapshotID)
	if err != nil {
		return nil, err
	}
	if snapshot.Status != "active" || snapshot.ApprovalState != "approved" || snapshot.ObjectDeletedAt.Valid ||
		snapshot.ObjectFormat != "wads" || snapshot.ObjectFormatVersion != uint64(flowdimension.AddressSnapshotFormatVersion) || !flowSHA256(snapshot.Checksum) {
		return nil, errors.New("approved WADS address catalog is unavailable")
	}
	artifacts := make([]flowArtifactRecord, 0, 2+len(payload.DeviceIDs))
	addressArtifact, err := s.ensureWADSFlowArtifact(ctx, tx, snapshot, job.CreatedBy)
	if err != nil {
		return nil, err
	}
	artifacts = append(artifacts, addressArtifact)
	policyArtifact, err := s.buildPolicyFlowArtifact(ctx, tx, profileVersion, overseasIncludesHMT, effectiveFrom, job.CreatedBy)
	if err != nil {
		return nil, err
	}
	artifacts = append(artifacts, policyArtifact)
	for _, deviceID := range payload.DeviceIDs {
		artifact, err := s.buildDeviceBoundaryFlowArtifact(ctx, tx, deviceID, payload.DeviceBoundaryDigest[deviceID], effectiveFrom, job.CreatedBy)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
		if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
			_ = reporter.Report(ctx, uint64(len(artifacts)), nil)
		}
	}
	deploymentIDs := make([]string, 0, len(payload.WorkerIDs))
	for _, workerID := range payload.WorkerIDs {
		id, err := s.insertFlowWorkerDeployment(ctx, tx, workerID, effectiveFrom, artifacts, job)
		if err != nil {
			return nil, err
		}
		deploymentIDs = append(deploymentIDs, id)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return deploymentIDs, nil
}

func readFlowAddressSnapshotByID(ctx context.Context, tx *sql.Tx, id string) (flowAddressSnapshotRef, error) {
	var snapshot flowAddressSnapshotRef
	err := tx.QueryRowContext(ctx, `SELECT id,version,effective_from,object_ref,object_format,object_format_version,
		checksum,status,approval_state,object_deleted_at FROM dimension_snapshots WHERE id=? FOR UPDATE`, id).
		Scan(&snapshot.ID, &snapshot.Version, &snapshot.EffectiveFrom, &snapshot.ObjectRef, &snapshot.ObjectFormat,
			&snapshot.ObjectFormatVersion, &snapshot.Checksum, &snapshot.Status, &snapshot.ApprovalState, &snapshot.ObjectDeletedAt)
	return snapshot, err
}

func (s *Server) ensureWADSFlowArtifact(ctx context.Context, tx *sql.Tx, snapshot flowAddressSnapshotRef, actor string) (flowArtifactRecord, error) {
	if existing, found, err := findFlowArtifactByChecksum(ctx, tx, flowworker.ArtifactKindAddressCatalog, "global", snapshot.Checksum); err != nil || found {
		return existing, err
	}
	path, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		return flowArtifactRecord{}, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return flowArtifactRecord{}, errors.New("WADS address catalog object is unavailable")
	}
	record := flowArtifactRecord{
		ID: newID(), Kind: flowworker.ArtifactKindAddressCatalog, ScopeID: "global", Version: snapshot.Version,
		Format: flowworker.VersionObjectFormatWADS, FormatVersion: flowdimension.AddressSnapshotFormatVersion,
		Checksum: snapshot.Checksum, SizeBytes: uint64(info.Size()), ObjectRef: snapshot.ObjectRef, SourceRevision: snapshot.ID, EffectiveFrom: snapshot.EffectiveFrom.UTC(),
	}
	return record, insertFlowArtifact(ctx, tx, record, actor)
}

func (s *Server) buildPolicyFlowArtifact(ctx context.Context, tx *sql.Tx, revision uint64, overseas bool, effective time.Time, actor string) (flowArtifactRecord, error) {
	var sourceRevision string
	if err := tx.QueryRowContext(ctx, `SELECT definition_digest FROM flow_classification_profiles WHERE id=1`).Scan(&sourceRevision); err != nil {
		return flowArtifactRecord{}, err
	}
	if existing, found, err := findFlowArtifactBySourceRevision(ctx, tx, flowworker.ArtifactKindClassificationPolicy, "global", sourceRevision); err != nil || found {
		if found && existing.EffectiveFrom.After(effective) {
			return flowArtifactRecord{}, errors.New("classification policy artifact becomes effective after the requested deployment")
		}
		return existing, err
	}
	version, err := nextFlowArtifactVersion(ctx, tx, flowworker.ArtifactKindClassificationPolicy, "global")
	if err != nil {
		return flowArtifactRecord{}, err
	}
	data, checksum, err := flowdimension.EncodeClassificationPolicy(flowdimension.ClassificationPolicyArtifact{
		SchemaVersion: flowdimension.ClassificationPolicySchemaVersion, Revision: revision,
		Algorithm: flowdimension.ClassificationPolicyAlgorithm, OverseasIncludesHMT: overseas,
		UnmatchedPolicy: "unknown", EffectiveFrom: effective,
	})
	if err != nil {
		return flowArtifactRecord{}, err
	}
	if existing, found, err := findFlowArtifactByChecksum(ctx, tx, flowworker.ArtifactKindClassificationPolicy, "global", checksum); err != nil || found {
		return existing, err
	}
	id := newID()
	object, err := s.addressObjects.SaveDimensionObject(ctx, id, data)
	if err != nil {
		return flowArtifactRecord{}, err
	}
	record := flowArtifactRecord{ID: id, Kind: flowworker.ArtifactKindClassificationPolicy, ScopeID: "global", Version: version,
		Format: flowworker.VersionObjectFormatJSON, FormatVersion: flowdimension.ClassificationPolicySchemaVersion,
		Checksum: object.Checksum, SizeBytes: object.Size, ObjectRef: object.Ref, SourceRevision: sourceRevision, EffectiveFrom: effective}
	return record, insertFlowArtifact(ctx, tx, record, actor)
}

func (s *Server) buildDeviceBoundaryFlowArtifact(ctx context.Context, tx *sql.Tx, deviceID, expectedSourceRevision string, effective time.Time, actor string) (flowArtifactRecord, error) {
	customers, sourceRevision, err := readDeviceBoundarySource(ctx, tx, deviceID)
	if err != nil {
		return flowArtifactRecord{}, err
	}
	if sourceRevision != expectedSourceRevision {
		return flowArtifactRecord{}, errFlowEnrichmentSuperseded
	}
	if existing, found, err := findFlowArtifactBySourceRevision(ctx, tx, flowworker.ArtifactKindDeviceBoundary, deviceID, sourceRevision); err != nil || found {
		if found && existing.EffectiveFrom.After(effective) {
			return flowArtifactRecord{}, errors.New("device boundary artifact becomes effective after the requested deployment")
		}
		return existing, err
	}
	version, err := nextFlowArtifactVersion(ctx, tx, flowworker.ArtifactKindDeviceBoundary, deviceID)
	if err != nil {
		return flowArtifactRecord{}, err
	}
	data, checksum, err := flowdimension.EncodeDeviceCustomerBoundary(flowdimension.DeviceCustomerBoundary{
		SchemaVersion: flowdimension.DeviceBoundarySchemaVersion, DeviceID: deviceID, Revision: version,
		EffectiveFrom: effective, Customers: customers,
	})
	if err != nil {
		return flowArtifactRecord{}, err
	}
	if existing, found, err := findFlowArtifactByChecksum(ctx, tx, flowworker.ArtifactKindDeviceBoundary, deviceID, checksum); err != nil || found {
		return existing, err
	}
	id := newID()
	object, err := s.addressObjects.SaveDimensionObject(ctx, id, data)
	if err != nil {
		return flowArtifactRecord{}, err
	}
	record := flowArtifactRecord{ID: id, Kind: flowworker.ArtifactKindDeviceBoundary, ScopeID: deviceID, Version: version,
		Format: flowworker.VersionObjectFormatJSON, FormatVersion: flowdimension.DeviceBoundarySchemaVersion,
		Checksum: object.Checksum, SizeBytes: object.Size, ObjectRef: object.Ref, SourceRevision: sourceRevision, EffectiveFrom: effective}
	return record, insertFlowArtifact(ctx, tx, record, actor)
}

func readDeviceBoundarySource(ctx context.Context, tx *sql.Tx, deviceID string) ([]flowdimension.DeviceBoundaryCustomer, string, error) {
	var deviceKind string
	if err := tx.QueryRowContext(ctx, `SELECT kind FROM devices WHERE id=?`, deviceID).Scan(&deviceKind); err != nil {
		return nil, "", err
	}
	if canonicalDeviceKind(deviceKind) != "network" {
		return nil, "", fmt.Errorf("device %s is not a network device", deviceID)
	}
	rows, err := tx.QueryContext(ctx, `SELECT b.customer_id,c.name,p.id,p.cidr FROM flow_device_customers b
		JOIN parties c ON c.id=b.customer_id JOIN flow_customer_source_prefixes p ON p.device_customer_id=b.id
		WHERE b.device_id=? AND c.kind='customer' AND c.status='active' ORDER BY b.customer_id,p.cidr,p.id`, deviceID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	customers := []flowdimension.DeviceBoundaryCustomer{}
	byID := map[string]int{}
	for rows.Next() {
		var customerID, customerName, prefixID, cidr string
		if err := rows.Scan(&customerID, &customerName, &prefixID, &cidr); err != nil {
			return nil, "", err
		}
		index, exists := byID[customerID]
		if !exists {
			index = len(customers)
			byID[customerID] = index
			customers = append(customers, flowdimension.DeviceBoundaryCustomer{CustomerID: customerID, CustomerName: customerName})
		}
		customers[index].Prefixes = append(customers[index].Prefixes, flowdimension.DeviceBoundaryPrefix{ID: prefixID, CIDR: cidr})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	_, digest, err := flowdimension.EncodeDeviceCustomerBoundary(flowdimension.DeviceCustomerBoundary{
		SchemaVersion: flowdimension.DeviceBoundarySchemaVersion, DeviceID: deviceID, Revision: 1,
		EffectiveFrom: time.Unix(0, 0).UTC(), Customers: customers,
	})
	if err != nil {
		return nil, "", err
	}
	return customers, digest, nil
}

func nextFlowArtifactVersion(ctx context.Context, tx *sql.Tx, kind, scope string) (uint64, error) {
	var current uint64
	err := tx.QueryRowContext(ctx, `SELECT version FROM flow_artifacts WHERE kind=? AND scope_id=? ORDER BY version DESC LIMIT 1 FOR UPDATE`, kind, scope).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	return current + 1, err
}

func findFlowArtifactByChecksum(ctx context.Context, tx *sql.Tx, kind, scope, checksum string) (flowArtifactRecord, bool, error) {
	var item flowArtifactRecord
	err := tx.QueryRowContext(ctx, `SELECT id,kind,scope_id,version,format,format_version,checksum,size_bytes,object_ref,source_revision,effective_from
		FROM flow_artifacts WHERE kind=? AND scope_id=? AND checksum=?`, kind, scope, checksum).
		Scan(&item.ID, &item.Kind, &item.ScopeID, &item.Version, &item.Format, &item.FormatVersion, &item.Checksum,
			&item.SizeBytes, &item.ObjectRef, &item.SourceRevision, &item.EffectiveFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return item, false, nil
	}
	return item, err == nil, err
}

func findFlowArtifactBySourceRevision(ctx context.Context, tx *sql.Tx, kind, scope, sourceRevision string) (flowArtifactRecord, bool, error) {
	var item flowArtifactRecord
	err := tx.QueryRowContext(ctx, `SELECT id,kind,scope_id,version,format,format_version,checksum,size_bytes,object_ref,source_revision,effective_from
		FROM flow_artifacts WHERE kind=? AND scope_id=? AND source_revision=? ORDER BY version DESC LIMIT 1`, kind, scope, sourceRevision).
		Scan(&item.ID, &item.Kind, &item.ScopeID, &item.Version, &item.Format, &item.FormatVersion, &item.Checksum,
			&item.SizeBytes, &item.ObjectRef, &item.SourceRevision, &item.EffectiveFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return item, false, nil
	}
	return item, err == nil, err
}

func insertFlowArtifact(ctx context.Context, tx *sql.Tx, item flowArtifactRecord, actor string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO flow_artifacts
		(id,kind,scope_id,version,format,format_version,checksum,size_bytes,object_ref,source_revision,effective_from,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,NULLIF(?,''))`, item.ID, item.Kind, item.ScopeID, item.Version, item.Format, item.FormatVersion,
		item.Checksum, item.SizeBytes, item.ObjectRef, item.SourceRevision, item.EffectiveFrom, actor)
	return err
}

func (s *Server) insertFlowWorkerDeployment(ctx context.Context, tx *sql.Tx, workerID string, effective time.Time, artifacts []flowArtifactRecord, job opjob.Job) (string, error) {
	var kind, status string
	if err := tx.QueryRowContext(ctx, `SELECT kind,status FROM agents WHERE id=? FOR UPDATE`, workerID).Scan(&kind, &status); err != nil {
		return "", err
	}
	if kind != "flow_worker" || (status != "active" && status != "registered") {
		return "", fmt.Errorf("agent %s is no longer an active Flow worker", workerID)
	}
	var generation uint64
	err := tx.QueryRowContext(ctx, `SELECT generation FROM flow_worker_deployments WHERE worker_id=? ORDER BY generation DESC LIMIT 1 FOR UPDATE`, workerID).Scan(&generation)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if errors.Is(err, sql.ErrNoRows) {
		// v2 shares the compatibility classification version field during the
		// rolling window. Seed its first generation after the largest v1 pair so
		// a worker can install v2 without violating catalog monotonicity.
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(classification_version),0) FROM flow_enrichment_publications`).Scan(&generation); err != nil {
			return "", err
		}
	}
	generation++
	if generation > uint64(^uint32(0)) {
		return "", errors.New("Flow deployment generation exceeds the v1 compatibility range")
	}
	deploymentID := newID()
	references := make([]flowworker.DeploymentArtifactReference, 0, len(artifacts))
	for _, artifact := range artifacts {
		references = append(references, flowworker.DeploymentArtifactReference{ArtifactID: artifact.ID, Kind: artifact.Kind,
			ScopeID: artifact.ScopeID, Version: artifact.Version, Format: artifact.Format, FormatVersion: artifact.FormatVersion,
			Checksum: artifact.Checksum, SizeBytes: artifact.SizeBytes})
	}
	signedAt := time.Now().UTC().Truncate(time.Millisecond)
	envelope := flowworker.SignedWorkerDeploymentManifest{
		SchemaVersion: flowworker.WorkerDeploymentEnvelopeSchemaVersion,
		Manifest: flowworker.WorkerDeploymentManifest{SchemaVersion: flowworker.WorkerDeploymentManifestSchemaVersion,
			DeploymentID: deploymentID, Generation: generation, WorkerID: workerID, EffectiveFrom: effective, Artifacts: references},
		SignatureAlgorithm: flowworker.WorkerDeploymentSignatureAlgorithm, SigningKeyID: s.agentPlanSigner.KeyID,
		SignedAtUnixMilli: signedAt.UnixMilli(),
	}
	payload, err := flowworker.WorkerDeploymentSigningPayload(envelope)
	if err != nil {
		return "", err
	}
	envelope.Signature = ed25519.Sign(s.agentPlanSigner.PrivateKey, payload)
	data, err := flowworker.MarshalSignedWorkerDeploymentManifest(envelope)
	if err != nil {
		return "", err
	}
	object, err := s.addressObjects.SaveDimensionObject(ctx, deploymentID, data)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_worker_deployments SET state='superseded',row_version=row_version+1
		WHERE worker_id=? AND state='desired'`, workerID); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO flow_worker_deployments
		(id,worker_id,generation,effective_from,manifest_checksum,manifest_object_ref,signature_algorithm,signing_key_id,signature,signed_at,state,operation_job_id,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,'desired',?,NULLIF(?,''))`, deploymentID, workerID, generation, effective,
		object.Checksum, object.Ref, envelope.SignatureAlgorithm, envelope.SigningKeyID, envelope.Signature, signedAt, job.ID, job.CreatedBy); err != nil {
		return "", err
	}
	for _, artifact := range artifacts {
		if _, err := tx.ExecContext(ctx, `INSERT INTO flow_worker_deployment_artifacts (deployment_id,artifact_id,kind,scope_id) VALUES (?,?,?,?)`,
			deploymentID, artifact.ID, artifact.Kind, artifact.ScopeID); err != nil {
			return "", err
		}
	}
	return deploymentID, nil
}

func (s *Server) fetchFlowWorkerDeployments(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	after, _ := strconv.ParseUint(c.DefaultQuery("after_generation", "0"), 10, 64)
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil || limit < 1 || limit > flowWorkerDeploymentPageLimit {
		fail(c, http.StatusBadRequest, "invalid_request", "limit must be 1..100")
		return
	}
	waitSeconds, err := strconv.Atoi(c.DefaultQuery("wait_seconds", "0"))
	if err != nil || waitSeconds < 0 || waitSeconds > 30 {
		fail(c, http.StatusBadRequest, "invalid_request", "wait_seconds must be 0..30")
		return
	}
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for {
		items, next, err := s.readFlowWorkerDeploymentEnvelopes(c.Request.Context(), c.Param("id"), after, limit)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if len(items) != 0 || time.Now().After(deadline) || waitSeconds == 0 {
			c.Header("Cache-Control", "no-store")
			c.JSON(http.StatusOK, gin.H{"items": items, "next_generation": next, "has_more": len(items) == limit})
			return
		}
		select {
		case <-c.Request.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Server) readFlowWorkerDeploymentEnvelopes(ctx context.Context, workerID string, after uint64, limit int) ([]json.RawMessage, uint64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT generation,manifest_object_ref FROM flow_worker_deployments
		WHERE worker_id=? AND state='desired' AND generation>? ORDER BY generation LIMIT ?`, workerID, after, limit)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	items := []json.RawMessage{}
	next := after
	for rows.Next() {
		var generation uint64
		var ref string
		if err := rows.Scan(&generation, &ref); err != nil {
			return nil, next, err
		}
		data, err := s.readFlowDeploymentObject(ref, 1<<20)
		if err != nil {
			return nil, next, err
		}
		items = append(items, data)
		next = generation
	}
	return items, next, rows.Err()
}

func (s *Server) fetchFlowWorkerDeployment(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var ref string
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT manifest_object_ref FROM flow_worker_deployments WHERE id=? AND worker_id=?`,
		c.Param("deployment_id"), c.Param("id")).Scan(&ref)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	data, err := s.readFlowDeploymentObject(ref, 1<<20)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_deployment_unavailable", err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "application/json", data)
}

func (s *Server) fetchFlowWorkerDeploymentArtifact(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var ref, checksum, format string
	var size uint64
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT a.object_ref,a.checksum,a.format,a.size_bytes
		FROM flow_worker_deployments d JOIN flow_worker_deployment_artifacts da ON da.deployment_id=d.id
		JOIN flow_artifacts a ON a.id=da.artifact_id WHERE d.id=? AND d.worker_id=? AND a.id=?`,
		c.Param("deployment_id"), c.Param("id"), c.Param("artifact_id")).Scan(&ref, &checksum, &format, &size)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	maximum := int64(16 << 20)
	contentType, filename := "application/json", "artifact.json"
	if format == flowworker.VersionObjectFormatWADS {
		maximum, contentType, filename = flowDimensionObjectMax, "application/octet-stream", "address-snapshot.wads"
	}
	if size == 0 || size > uint64(maximum) {
		fail(c, http.StatusServiceUnavailable, "flow_deployment_artifact_invalid", "artifact size is invalid")
		return
	}
	path, err := s.addressObjects.ResolveDimensionObject(ref)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_deployment_artifact_unavailable", "artifact is unavailable")
		return
	}
	file, err := os.Open(path)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_deployment_artifact_unavailable", "artifact is unavailable")
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != size {
		fail(c, http.StatusServiceUnavailable, "flow_deployment_artifact_invalid", "artifact metadata does not match its object")
		return
	}
	etag := `"` + checksum + `"`
	c.Header("Cache-Control", "private, max-age=31536000, immutable")
	c.Header("Content-Type", contentType)
	c.Header("ETag", etag)
	c.Header("Content-Disposition", `attachment; filename="`+filename+`"`)
	c.Header("X-Watchdog-Object-Checksum", checksum)
	if c.GetHeader("If-None-Match") == etag {
		c.Status(http.StatusNotModified)
		return
	}
	http.ServeContent(c.Writer, c.Request, filename, info.ModTime(), file)
}

func (s *Server) acknowledgeFlowWorkerDeployment(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var request flowWorkerDeploymentACKRequest
	if !decodeStrictBody(c, &request) {
		return
	}
	if err := validateFlowWorkerDeploymentACK(request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var generation uint64
	var checksum string
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT generation,manifest_checksum FROM flow_worker_deployments WHERE id=? AND worker_id=?`,
		c.Param("deployment_id"), c.Param("id")).Scan(&generation, &checksum); err != nil {
		writeSQLError(c, err)
		return
	}
	if generation != request.Generation || checksum != request.ManifestChecksum {
		fail(c, http.StatusConflict, "flow_deployment_ack_conflict", "acknowledgement does not match the deployment")
		return
	}
	installedJSON, _ := json.Marshal(request.InstalledArtifacts)
	now := time.Now().UTC().Truncate(time.Millisecond)
	downloaded, verified, installed := any(nil), any(nil), any(nil)
	if request.State == "downloaded" || request.State == "verified" || request.State == "installed" {
		downloaded = now
	}
	if request.State == "verified" || request.State == "installed" {
		verified = now
	}
	if request.State == "installed" {
		installed = now
	}
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO flow_worker_deployment_acks
		(deployment_id,worker_id,generation,manifest_checksum,boot_id,software_version,state,attempted_at,downloaded_at,verified_at,installed_at,
		 failure_stage,error_code,error_message,installed_artifacts)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,NULLIF(?,''),NULLIF(?,''),NULLIF(?,''),CAST(NULLIF(?,'') AS JSON))
		ON DUPLICATE KEY UPDATE boot_id=VALUES(boot_id),software_version=VALUES(software_version),attempted_at=VALUES(attempted_at),
		installed_artifacts=CASE WHEN state='installed' THEN installed_artifacts ELSE COALESCE(VALUES(installed_artifacts),installed_artifacts) END,
		downloaded_at=COALESCE(downloaded_at,VALUES(downloaded_at)),verified_at=COALESCE(verified_at,VALUES(verified_at)),installed_at=COALESCE(installed_at,VALUES(installed_at)),
		failure_stage=CASE WHEN installed_at IS NOT NULL OR VALUES(state)<>'failed' THEN NULL ELSE VALUES(failure_stage) END,
		error_code=CASE WHEN installed_at IS NOT NULL OR VALUES(state)<>'failed' THEN NULL ELSE VALUES(error_code) END,
		error_message=CASE WHEN installed_at IS NOT NULL OR VALUES(state)<>'failed' THEN NULL ELSE VALUES(error_message) END,
		state=CASE WHEN installed_at IS NOT NULL THEN 'installed' WHEN state='verified' AND VALUES(state)='downloaded' THEN 'verified' ELSE VALUES(state) END,
		row_version=row_version+1`,
		c.Param("deployment_id"), c.Param("id"), generation, checksum, request.BootID, request.SoftwareVersion, request.State,
		now, downloaded, verified, installed, request.FailureStage, request.FailureCode, request.FailureMessage, nullableJSONBytes(installedJSON))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true})
}

func validateFlowWorkerDeploymentACK(request flowWorkerDeploymentACKRequest) error {
	if request.Generation == 0 || !flowSHA256(request.ManifestChecksum) || request.BootID == "" || len(request.BootID) > 128 ||
		request.SoftwareVersion == "" || len(request.SoftwareVersion) > 64 {
		return errors.New("acknowledgement identity is invalid")
	}
	switch request.State {
	case "downloaded", "verified", "installed":
		if request.FailureStage != "" || request.FailureCode != "" || request.FailureMessage != "" {
			return errors.New("successful acknowledgement cannot contain a failure")
		}
	case "failed":
		if !validFlowDeploymentFailureStage(request.FailureStage) || request.FailureCode == "" || len(request.FailureCode) > 64 || len(request.FailureMessage) > 512 {
			return errors.New("failed acknowledgement requires a valid stage and error code")
		}
	default:
		return errors.New("state must be downloaded, verified, installed, or failed")
	}
	for _, item := range request.InstalledArtifacts {
		if item.Version == 0 || !flowSHA256(item.Checksum) {
			return errors.New("installed artifact receipt is invalid")
		}
	}
	return nil
}

func validFlowDeploymentFailureStage(value string) bool {
	switch value {
	case "fetch", "verify", "compile", "persist", "activate", "ack":
		return true
	default:
		return false
	}
}

func (s *Server) listFlowWorkerDeployments(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"worker_id", "state"}, map[string]string{
		"generation": "d.generation", "effective_from": "d.effective_from", "created_at": "d.created_at", "worker": "a.name", "state": "d.state",
	}, "created_at")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if value := strings.TrimSpace(c.Query("worker_id")); value != "" {
		where, args = append(where, "d.worker_id=?"), append(args, value)
	}
	if value := strings.TrimSpace(c.Query("state")); value != "" {
		where, args = append(where, "d.state=?"), append(args, value)
	}
	if value := strings.TrimSpace(c.Query("q")); value != "" {
		where, args = append(where, "(d.id LIKE ? OR a.name LIKE ? OR d.manifest_checksum LIKE ?)"), append(args, "%"+escapeLike(value)+"%", "%"+escapeLike(value)+"%", "%"+escapeLike(value)+"%")
	}
	clause := ` FROM flow_worker_deployments d JOIN agents a ON a.id=d.worker_id
		LEFT JOIN flow_worker_deployment_acks x ON x.deployment_id=d.id AND x.worker_id=d.worker_id WHERE ` + strings.Join(where, " AND ")
	var total uint64
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*)"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT d.id,d.worker_id,a.name,d.generation,d.effective_from,d.manifest_checksum,
		d.signing_key_id,d.state,d.operation_job_id,d.created_at,a.health,a.last_seen_at,x.state,x.attempted_at,x.downloaded_at,
		x.verified_at,x.installed_at,x.failure_stage,x.error_code,x.error_message,
		COALESCE((SELECT GROUP_CONCAT(DISTINCT COALESCE(NULLIF(v.display_name,''),NULLIF(v.sys_name,''),v.host)
			ORDER BY COALESCE(NULLIF(v.display_name,''),NULLIF(v.sys_name,''),v.host) SEPARATOR ' / ')
			FROM flow_worker_deployment_artifacts da JOIN devices v ON v.id=da.scope_id
			WHERE da.deployment_id=d.id AND da.kind='device_boundary'),'')`+clause+` ORDER BY `+page.Sort+` `+page.Order+`,d.id `+page.Order+` LIMIT ? OFFSET ?`,
		append(append([]any{}, args...), page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, workerID, workerName, checksum, keyID, state, health, deviceNames string
		var jobID, ackState, failureStage, errorCode, errorMessage sql.NullString
		var lastSeen, attempted, downloaded, verified, installed sql.NullTime
		var generation uint64
		var effective, created time.Time
		if err := rows.Scan(&id, &workerID, &workerName, &generation, &effective, &checksum, &keyID, &state, &jobID, &created,
			&health, &lastSeen, &ackState, &attempted, &downloaded, &verified, &installed, &failureStage, &errorCode, &errorMessage,
			&deviceNames); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, gin.H{"id": id, "worker_id": workerID, "worker_name": workerName, "generation": generation,
			"effective_from": effective, "manifest_checksum": checksum, "signing_key_id": keyID, "state": state,
			"operation_job_id": nullableSQLString(jobID), "created_at": created, "worker_health": health,
			"device_names":     deviceNames,
			"worker_last_seen": nullableTime(lastSeen), "ack_state": nullableSQLString(ackState), "ack_attempted_at": nullableTime(attempted),
			"downloaded_at": nullableTime(downloaded), "verified_at": nullableTime(verified), "installed_at": nullableTime(installed),
			"failure_stage": nullableSQLString(failureStage), "error_code": nullableSQLString(errorCode), "error_message": nullableSQLString(errorMessage)})
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) readFlowDeploymentObject(ref string, maximum int64) ([]byte, error) {
	path, err := s.addressObjects.ResolveDimensionObject(ref)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || int64(len(data)) > maximum {
		return nil, errors.New("deployment object size is invalid")
	}
	return data, nil
}

func writeFlowWorkerDeploymentError(c *gin.Context, err error) {
	if c.IsAborted() {
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusUnprocessableEntity, "flow_deployment_prerequisite", "Flow deployment prerequisites are incomplete")
		return
	}
	fail(c, http.StatusUnprocessableEntity, "flow_deployment_invalid", err.Error())
}

func sortedUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sqlPlaceholders(count int) string {
	if count < 1 {
		return "NULL"
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func stringSliceAny(values []string) []any {
	result := make([]any, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}

func nullableJSONBytes(data []byte) any {
	if len(data) == 0 || string(data) == "null" || string(data) == "{}" {
		return nil
	}
	return string(data)
}

func nullableSQLString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}
