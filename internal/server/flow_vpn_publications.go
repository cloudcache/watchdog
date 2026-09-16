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
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

const (
	vpnRuleSetPublishJobType       = "flow_vpn_rule_set_publish"
	vpnRuleSetPublishPayloadSchema = 1
	vpnRuleSetBuilderVersion       = "watchdog-vpn-rules-v1"
)

type vpnRuleSetPolicy struct {
	MediumThreshold     uint16  `json:"medium_threshold"`
	HighThreshold       uint16  `json:"high_threshold"`
	CriticalThreshold   uint16  `json:"critical_threshold"`
	ProbeThreshold      uint16  `json:"probe_threshold"`
	MinimumCompleteness float64 `json:"minimum_completeness"`
}

type vpnRuleSetDraft struct {
	SchemaVersion uint32                  `json:"schema_version"`
	Policy        vpnRuleSetPolicy        `json:"policy"`
	Rules         []flowvpn.PublishedRule `json:"rules"`
}

type vpnRuleSetPreview struct {
	DraftDigest          string           `json:"draft_digest"`
	BundleSchemaVersion  uint32           `json:"bundle_schema_version"`
	EffectiveFrom        time.Time        `json:"effective_from"`
	RuleCount            uint64           `json:"rule_count"`
	EstimatedObjectBytes uint64           `json:"estimated_object_bytes"`
	Policy               vpnRuleSetPolicy `json:"policy"`
}

type vpnRuleSetPublishPayload struct {
	EffectiveFrom string           `json:"effective_from"`
	PreviewDigest string           `json:"preview_digest"`
	Policy        vpnRuleSetPolicy `json:"policy"`
}

type vpnRuleSetPublishRequest struct {
	EffectiveFrom time.Time        `json:"effective_from"`
	PreviewDigest string           `json:"preview_digest,omitempty"`
	Policy        vpnRuleSetPolicy `json:"policy"`
}

func (s *Server) registerFlowVPNRuleSetRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("flow.vpn.view")
	manage := s.requirePermission("flow.vpn.manage")
	publish := s.requirePermission("flow.vpn.publish")
	routes := auth.Group("/flow/vpn/rule-sets")
	routes.POST("/preview", manage, s.previewVPNRuleSet)
	routes.POST("/publish", publish, s.publishVPNRuleSet)
	routes.GET("", view, s.listVPNRuleSets)
	routes.GET("/:id", view, s.getVPNRuleSet)
	routes.GET("/:id/object", view, s.downloadVPNRuleSetObject)
	routes.GET("/:id/consumers", view, s.listVPNRuleSetConsumers)
	routes.POST("/:id/actions/approve", publish, s.approveVPNRuleSet)
	routes.POST("/:id/actions/reject", publish, s.rejectVPNRuleSet)
	routes.POST("/:id/actions/activate", publish, s.activateVPNRuleSet)
	routes.POST("/:id/actions/rollback", publish, s.rollbackVPNRuleSet)
	routes.POST("/:id/actions/retire", publish, s.retireVPNRuleSet)
}

func (s *Server) previewVPNRuleSet(c *gin.Context) {
	var request vpnRuleSetPublishRequest
	if !addressDecodeStrict(c, &request, 64<<10) {
		return
	}
	if !flowUTCMinute(request.EffectiveFrom) {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be a UTC minute boundary")
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	draft, digest, err := loadVPNRuleSetDraft(c.Request.Context(), tx, false, request.Policy)
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	data, _, err := encodeVPNRuleSetDraft(draft, "00000000000000000000000000", 1, request.EffectiveFrom.UTC())
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, vpnRuleSetPreview{
		DraftDigest: digest, BundleSchemaVersion: flowvpn.RuleSetBundleSchemaV1,
		EffectiveFrom: request.EffectiveFrom.UTC(), RuleCount: uint64(len(draft.Rules)),
		EstimatedObjectBytes: uint64(len(data)), Policy: draft.Policy,
	})
}

func (s *Server) publishVPNRuleSet(c *gin.Context) {
	var request vpnRuleSetPublishRequest
	if !addressDecodeStrict(c, &request, 64<<10) {
		return
	}
	if !flowUTCMinute(request.EffectiveFrom) || !flowSHA256(request.PreviewDigest) {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from and preview_digest are required")
		return
	}
	payload, err := opjob.EncodePayload(vpnRuleSetPublishPayloadSchema, vpnRuleSetPublishPayload{
		EffectiveFrom: request.EffectiveFrom.UTC().Format(time.RFC3339Nano), PreviewDigest: request.PreviewDigest, Policy: request.Policy,
	})
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType:        vpnRuleSetPublishJobType,
		IdempotencyKey: "flow-vpn-rule-set:" + request.EffectiveFrom.UTC().Format(time.RFC3339) + ":" + strings.TrimPrefix(request.PreviewDigest, "sha256:"),
		RequestHash:    sha256hex(string(payload)), CheckpointJSON: payload, CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job": job})
}

func loadVPNRuleSetDraft(ctx context.Context, tx *sql.Tx, lock bool, policy vpnRuleSetPolicy) (vpnRuleSetDraft, string, error) {
	query := vpnRuleSelect + " WHERE status='active' AND deleted_at IS NULL ORDER BY id"
	if lock {
		query += " FOR SHARE"
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return vpnRuleSetDraft{}, "", err
	}
	defer rows.Close()
	draft := vpnRuleSetDraft{SchemaVersion: flowvpn.RuleSetBundleSchemaV1, Policy: policy, Rules: []flowvpn.PublishedRule{}}
	for rows.Next() {
		item, err := scanVPNRule(rows)
		if err != nil {
			return vpnRuleSetDraft{}, "", err
		}
		draft.Rules = append(draft.Rules, flowvpn.PublishedRule{
			ID: item.ID, Name: item.Name, Kind: flowvpn.RuleKind(item.Kind), Effect: item.Effect,
			Weight: item.Weight, Priority: item.Priority, FamilyHint: item.FamilyHint, Match: item.Match,
		})
	}
	if err := rows.Err(); err != nil {
		return vpnRuleSetDraft{}, "", err
	}
	if len(draft.Rules) == 0 {
		return vpnRuleSetDraft{}, "", errNoActiveVPNRules
	}
	// The bundle encoder is the canonical validator for thresholds and rules.
	if _, _, err := encodeVPNRuleSetDraft(draft, "00000000000000000000000000", 1, time.Unix(60, 0).UTC()); err != nil {
		return vpnRuleSetDraft{}, "", err
	}
	data, err := json.Marshal(draft)
	if err != nil {
		return vpnRuleSetDraft{}, "", err
	}
	digest := sha256.Sum256(data)
	return draft, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func encodeVPNRuleSetDraft(draft vpnRuleSetDraft, snapshotID string, version uint64, effectiveFrom time.Time) ([]byte, string, error) {
	return flowvpn.EncodeRuleSetBundle(flowvpn.RuleSetBundle{
		SchemaVersion: flowvpn.RuleSetBundleSchemaV1, SnapshotID: snapshotID, Version: version, EffectiveFrom: effectiveFrom.UTC(),
		MediumThreshold: draft.Policy.MediumThreshold, HighThreshold: draft.Policy.HighThreshold,
		CriticalThreshold: draft.Policy.CriticalThreshold, ProbeThreshold: draft.Policy.ProbeThreshold,
		MinimumCompleteness: draft.Policy.MinimumCompleteness, Rules: draft.Rules,
	})
}

func (s *Server) vpnRuleSetPublishHandler() opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if s.vpnRuleSetPublisher == nil {
			return "", opjob.TerminalError(errors.New("VPN rule-set publisher is unavailable"))
		}
		var payload vpnRuleSetPublishPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, vpnRuleSetPublishPayloadSchema, &payload); err != nil {
			return "", err
		}
		effectiveFrom, err := time.Parse(time.RFC3339Nano, payload.EffectiveFrom)
		if err != nil || !flowUTCMinute(effectiveFrom) || !flowSHA256(payload.PreviewDigest) || job.ID == "" || job.CreatedBy == "" {
			return "", opjob.TerminalError(errors.New("invalid VPN rule-set publish payload"))
		}
		snapshot, err := s.vpnRuleSetPublisher.BuildScopedPublication(ctx, job.CreatedBy, address.ScopedPublicationBuild{
			SnapshotID: job.ID, BuildJobID: job.ID, EffectiveFrom: effectiveFrom.UTC(), PreviewDigest: payload.PreviewDigest,
			ObjectFormat: "json", BuilderVersion: vpnRuleSetBuilderVersion, BundleSchemaVersion: flowvpn.RuleSetBundleSchemaV1,
		}, func(ctx context.Context, tx *sql.Tx, lock bool, snapshotID address.ID, version uint64, at time.Time) ([]byte, string, uint64, error) {
			draft, digest, err := loadVPNRuleSetDraft(ctx, tx, lock, payload.Policy)
			if err != nil {
				return nil, "", 0, err
			}
			data, _, err := encodeVPNRuleSetDraft(draft, string(snapshotID), version, at)
			return data, digest, uint64(len(draft.Rules)), err
		})
		if errors.Is(err, address.ErrAddressDimensionDraftChanged) || errors.Is(err, address.ErrAddressDimensionInvalid) ||
			errors.Is(err, address.ErrAddressDimensionConflict) || errors.Is(err, errNoActiveVPNRules) {
			return "", opjob.TerminalError(err)
		}
		if err != nil {
			return "", err
		}
		return "vpn-rule-set:" + string(snapshot.ID), nil
	}
}

func (s *Server) listVPNRuleSets(c *gin.Context) {
	limit, offset := pageParams(c)
	items, _, total, err := s.vpnRuleSetPublisher.ListAddressDimensionSnapshots(c.Request.Context(), address.AddressDimensionListFilter{
		Status: c.Query("status"), ApprovalState: c.Query("approval_state"), Search: c.Query("q"), Sort: c.Query("sort"),
		Desc: sortDirection(c) == "DESC", Limit: limit, Offset: offset, TableMode: true,
	})
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getVPNRuleSet(c *gin.Context) {
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}

func (s *Server) downloadVPNRuleSetObject(c *gin.Context) {
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	serveVPNRuleSetObject(c, s.addressObjects, snapshot)
}

func serveVPNRuleSetObject(c *gin.Context, objects address.DiskDimensionObjectStore, snapshot address.DimensionPublicationSnapshot) {
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "vpn_rule_set_object_unavailable", "VPN rule-set object is unavailable")
		return
	}
	file, err := os.Open(path)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "vpn_rule_set_object_unavailable", "VPN rule-set object is unavailable")
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > flowvpn.MaxRuleSetBundleBytes {
		fail(c, http.StatusServiceUnavailable, "vpn_rule_set_object_unavailable", "VPN rule-set object is unavailable")
		return
	}
	c.Header("Cache-Control", "private, max-age=31536000, immutable")
	c.Header("X-Watchdog-Object-Checksum", snapshot.Checksum)
	c.Header("ETag", `"`+snapshot.Checksum+`"`)
	http.ServeContent(c.Writer, c.Request, "vpn-rule-set.json", info.ModTime(), file)
}

type flowWorkerVPNRuleSetAck struct {
	State           string `json:"state"`
	BootID          string `json:"boot_id"`
	SoftwareVersion string `json:"software_version"`
	Checksum        string `json:"checksum"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

func (s *Server) fetchFlowWorkerVPNRuleSet(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	activation, err := s.vpnRuleSetPublisher.GetDimensionPublicationActivationAt(c.Request.Context(), time.Now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		c.Status(http.StatusNoContent)
		return
	}
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(c.Request.Context(), activation.SnapshotID)
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	if snapshot.ApprovalState != address.AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil {
		fail(c, http.StatusServiceUnavailable, "vpn_rule_set_unavailable", "active VPN rule set is not installable")
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"snapshot_id": snapshot.ID, "version": snapshot.Version, "effective_from": activation.EffectiveFrom,
		"checksum": snapshot.Checksum, "schema_version": snapshot.BundleSchemaVersion,
		"object_url": "/api/v1/flow-workers/" + c.Param("id") + "/vpn-rule-sets/" + string(snapshot.ID) + "/object",
	})
}

func (s *Server) fetchFlowWorkerVPNRuleSetObject(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(c.Request.Context(), c.Param("snapshot_id"))
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	if snapshot.ApprovalState != address.AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil {
		fail(c, http.StatusNotFound, "not_found", "VPN rule set not found")
		return
	}
	serveVPNRuleSetObject(c, s.addressObjects, snapshot)
}

func (s *Server) acknowledgeFlowWorkerVPNRuleSet(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var request flowWorkerVPNRuleSetAck
	if !addressDecodeStrict(c, &request, 16<<10) {
		return
	}
	request.State = strings.TrimSpace(request.State)
	request.BootID = strings.TrimSpace(request.BootID)
	request.SoftwareVersion = strings.TrimSpace(request.SoftwareVersion)
	request.ErrorCode = strings.TrimSpace(request.ErrorCode)
	request.ErrorMessage = strings.TrimSpace(request.ErrorMessage)
	ack, err := s.vpnRuleSetPublisher.ReportDimensionPublicationAcknowledgement(c.Request.Context(), address.DimensionPublicationAcknowledgement{
		SnapshotID: c.Param("snapshot_id"), WorkerID: c.Param("id"), BootID: request.BootID,
		SoftwareVersion: request.SoftwareVersion, Checksum: request.Checksum, State: request.State,
		ErrorCode: request.ErrorCode, ErrorMessage: request.ErrorMessage,
	})
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, ack)
}

func (s *Server) listVPNRuleSetConsumers(c *gin.Context) {
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be 1..500")
			return
		}
		limit = parsed
	}
	filter := address.AddressDimensionConsumerFilter{Query: c.Query("q"), State: c.Query("state"), Drift: c.Query("drift"), Cursor: c.Query("cursor"), Limit: limit}
	items, next, err := s.vpnRuleSetPublisher.ListAddressDimensionConsumers(c.Request.Context(), c.Param("id"), filter)
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	summary, err := s.vpnRuleSetPublisher.GetAddressDimensionConsumerSummary(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	response := gin.H{"items": items, "summary": summary}
	if next != "" {
		response["next_cursor"] = next
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) approveVPNRuleSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	snapshot, err := s.vpnRuleSetPublisher.ApproveDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, c.Param("id"), expected)
	writeVPNRuleSetSnapshot(c, snapshot, err)
}

func (s *Server) rejectVPNRuleSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var request struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&request)
	snapshot, err := s.vpnRuleSetPublisher.RejectDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, c.Param("id"), expected, request.Reason)
	writeVPNRuleSetSnapshot(c, snapshot, err)
}

func (s *Server) activateVPNRuleSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	activation, err := s.vpnRuleSetPublisher.ActivateDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, address.DimensionPublicationActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: snapshot.EffectiveFrom, ExpectedRowVersion: expected,
	})
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.JSON(http.StatusOK, activation)
}

func (s *Server) rollbackVPNRuleSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var request struct {
		EffectiveFrom time.Time `json:"effective_from"`
	}
	if !addressDecodeStrict(c, &request, 16<<10) {
		return
	}
	if !flowUTCMinute(request.EffectiveFrom) {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be a UTC minute boundary")
		return
	}
	activation, err := s.vpnRuleSetPublisher.RollbackDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, address.DimensionPublicationRollbackRequest{
		SnapshotID: c.Param("id"), EffectiveFrom: request.EffectiveFrom.UTC(), ExpectedRowVersion: expected,
	})
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.JSON(http.StatusOK, activation)
}

func (s *Server) retireVPNRuleSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var request struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&request)
	snapshot, err := s.vpnRuleSetPublisher.RetireDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, address.DimensionPublicationRetireRequest{
		SnapshotID: c.Param("id"), ExpectedRowVersion: expected, Reason: request.Reason,
	})
	writeVPNRuleSetSnapshot(c, snapshot, err)
}

func writeVPNRuleSetSnapshot(c *gin.Context, snapshot address.DimensionPublicationSnapshot, err error) {
	if err != nil {
		writeVPNRuleSetError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}

func writeVPNRuleSetError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "VPN rule set not found")
	case errors.Is(err, address.ErrAddressDimensionConflict), errors.Is(err, address.ErrAddressSnapshotBuildRace):
		fail(c, http.StatusConflict, "conflict", "VPN rule-set publication conflict")
	case errors.Is(err, address.ErrAddressDimensionDraftChanged):
		fail(c, http.StatusConflict, "draft_changed", "VPN rules changed after preview")
	case errors.Is(err, address.ErrAddressDimensionInvalidTransition):
		fail(c, http.StatusConflict, "invalid_transition", "VPN rule-set lifecycle transition is invalid")
	case errors.Is(err, errNoActiveVPNRules):
		fail(c, http.StatusConflict, "no_active_rules", "at least one active VPN rule is required")
	case errors.Is(err, address.ErrAddressDimensionInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", "VPN rule-set publication is invalid")
	default:
		fail(c, http.StatusInternalServerError, "internal", fmt.Sprint(err))
	}
}
