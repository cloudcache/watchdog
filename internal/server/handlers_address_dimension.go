package server

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

// registerAddressDimensionRoutes wires the address publication lifecycle: preview
// the current draft, enqueue an async WADS build, browse/download published
// versions, and drive approve/activate/rollback/retire. Read is address.view;
// preview is address.manage; publish + every lifecycle verb is address.publish.
func (s *Server) registerAddressDimensionRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("address.view")
	manage := s.requirePermission("address.manage")
	publish := s.requirePermission("address.publish")

	dim := auth.Group("/dimensions/address")
	dim.GET("/versions", view, s.listAddressDimensionVersions)
	dim.GET("/versions/:id", view, s.getAddressDimensionVersion)
	dim.GET("/versions/:id/object", view, s.downloadAddressDimensionObject)
	dim.POST("/preview", manage, s.previewAddressDimension)
	dim.POST("/publish", publish, s.publishAddressDimension)
	dim.POST("/versions/:id/actions/approve", publish, s.approveAddressDimension)
	dim.POST("/versions/:id/actions/reject", publish, s.rejectAddressDimension)
	dim.POST("/versions/:id/actions/activate", publish, s.activateAddressDimension)
	dim.POST("/versions/:id/actions/rollback", publish, s.rollbackAddressDimension)
	dim.POST("/versions/:id/actions/retire", publish, s.retireAddressDimension)
}

// writeAddressDimensionError maps the domain errors to HTTP status codes.
func writeAddressDimensionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "address dimension version not found")
	case errors.Is(err, address.ErrAddressDimensionConflict):
		fail(c, http.StatusConflict, "conflict", "address dimension version conflict")
	case errors.Is(err, address.ErrAddressDimensionDraftChanged):
		fail(c, http.StatusConflict, "draft_changed", "address dimension draft changed after preview")
	case errors.Is(err, address.ErrAddressDimensionInvalidTransition):
		fail(c, http.StatusConflict, "invalid_transition", "address dimension lifecycle transition is invalid")
	case errors.Is(err, address.ErrAddressDimensionInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		fail(c, http.StatusInternalServerError, "internal", err.Error())
	}
}

func parseEffectiveFrom(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Now().UTC().Truncate(time.Minute), true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC().Truncate(time.Minute), true
}

func (s *Server) previewAddressDimension(c *gin.Context) {
	var req struct {
		EffectiveFrom string `json:"effective_from"`
	}
	_ = c.ShouldBindJSON(&req)
	effectiveFrom, ok := parseEffectiveFrom(req.EffectiveFrom)
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be RFC3339")
		return
	}
	preview, err := s.addressPublisher.PreviewAddressDimension(c.Request.Context(), effectiveFrom)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.JSON(http.StatusOK, preview)
}

func (s *Server) publishAddressDimension(c *gin.Context) {
	var req struct {
		EffectiveFrom string `json:"effective_from"`
		PreviewDigest string `json:"preview_digest"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	effectiveFrom, ok := parseEffectiveFrom(req.EffectiveFrom)
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be RFC3339")
		return
	}
	payload, err := address.EncodeAddressDimensionPublishJobPayload(address.AddressDimensionPublishRequest{
		EffectiveFrom: effectiveFrom, PreviewDigest: req.PreviewDigest,
	})
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from or preview_digest is invalid")
		return
	}
	// Idempotency = effective_from + preview digest, so re-publishing identical
	// content converges on one job/snapshot; the opjob id becomes the snapshot id.
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType:        address.AddressSnapshotBuildJob,
		IdempotencyKey: "address-dimension:" + effectiveFrom.UTC().Format(time.RFC3339) + ":" + strings.TrimPrefix(req.PreviewDigest, "sha256:"),
		RequestHash:    sha256hex(string(payload)),
		CheckpointJSON: payload, CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job": job})
}

func (s *Server) listAddressDimensionVersions(c *gin.Context) {
	limit, offset := pageParams(c)
	filter := address.AddressDimensionListFilter{
		Status: c.Query("status"), Search: c.Query("q"), Sort: c.Query("sort"),
		Desc: sortDirection(c) == "DESC", Limit: limit, Offset: offset,
		TableMode: c.Query("sort") != "" || c.Query("order") != "" || c.Query("offset") != "",
	}
	items, next, total, err := s.addressPublisher.ListAddressDimensionSnapshots(c.Request.Context(), filter)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	out := gin.H{"items": items, "total": total, "limit": limit, "offset": offset}
	if next != "" {
		out["next_cursor"] = next
	}
	c.JSON(http.StatusOK, out)
}

func (s *Server) getAddressDimensionVersion(c *gin.Context) {
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}

func (s *Server) downloadAddressDimensionObject(c *gin.Context) {
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	path, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		fail(c, http.StatusNotFound, "not_found", "dimension object is not available")
		return
	}
	c.Header("ETag", `"`+snapshot.Checksum+`"`)
	c.Header("X-Watchdog-Object-Checksum", snapshot.Checksum)
	c.FileAttachment(path, "address-snapshot.wads")
}

func (s *Server) approveAddressDimension(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if err != nil || !supplied {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match row version is required")
		return
	}
	var req struct {
		SigningKeyID string    `json:"signing_key_id"`
		SignedAt     time.Time `json:"signed_at"`
		Signature    string    `json:"signature"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.Signature))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "signature must be base64")
		return
	}
	ctx := c.Request.Context()
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(ctx, c.Param("id"))
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	publicKey, err := s.addressKeys.ResolveAddressDimensionPublicKey(ctx, req.SigningKeyID)
	if err != nil {
		fail(c, http.StatusBadRequest, "trusted_key_not_found", "signing key is not trusted")
		return
	}
	approval, err := address.VerifyAddressDimensionApproval(snapshot, req.SigningKeyID, req.SignedAt, signature, publicKey)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_signature", "approval signature verification failed")
		return
	}
	approved, err := s.addressPublisher.ApproveDimensionPublication(ctx, currentPrincipal(c).UserID, approval, expected)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.Header("ETag", etag(approved.RowVersion))
	c.JSON(http.StatusOK, approved)
}

func (s *Server) rejectAddressDimension(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if err != nil || !supplied {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match row version is required")
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)
	snapshot, err := s.addressPublisher.RejectDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, c.Param("id"), expected, req.Reason)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}

func (s *Server) activateAddressDimension(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if err != nil || !supplied {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match row version is required")
		return
	}
	ctx := c.Request.Context()
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(ctx, c.Param("id"))
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	activation, err := s.addressPublisher.ActivateDimensionPublication(ctx, currentPrincipal(c).UserID, address.AddressDimensionActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: snapshot.EffectiveFrom, ExpectedRowVersion: expected,
	})
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.JSON(http.StatusOK, activation)
}

func (s *Server) rollbackAddressDimension(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if err != nil || !supplied {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match row version is required")
		return
	}
	var req struct {
		EffectiveFrom string `json:"effective_from"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	effectiveFrom, ok := parseEffectiveFrom(req.EffectiveFrom)
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be RFC3339")
		return
	}
	activation, err := s.addressPublisher.RollbackDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, address.AddressDimensionRollbackRequest{
		SnapshotID: c.Param("id"), EffectiveFrom: effectiveFrom, ExpectedRowVersion: expected,
	})
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.JSON(http.StatusOK, activation)
}

func (s *Server) retireAddressDimension(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if err != nil || !supplied {
		fail(c, http.StatusBadRequest, "invalid_if_match", "If-Match row version is required")
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)
	snapshot, err := s.addressPublisher.RetireDimensionPublication(c.Request.Context(), currentPrincipal(c).UserID, address.AddressDimensionRetireRequest{
		SnapshotID: c.Param("id"), ExpectedRowVersion: expected, Reason: req.Reason,
	})
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}
