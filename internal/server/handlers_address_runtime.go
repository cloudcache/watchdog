package server

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/gin-gonic/gin"
)

// Consumer-status (ACK/LKG) + object-GC HTTP endpoints for the address dimension,
// faithful ports of internal/watchdog api_address_dimension_consumers.go +
// api_address_dimension_gc.go over the de-tenanted Publisher.

func (s *Server) addressDimensionRuntimeStatus(c *gin.Context) {
	ctx := c.Request.Context()
	at := time.Now().UTC()
	if raw := strings.TrimSpace(c.Query("at")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "at must be RFC3339")
			return
		}
		at = parsed.UTC()
	}
	activation, err := s.addressPublisher.GetDimensionPublicationActivationAt(ctx, at)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(ctx, activation.SnapshotID)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	summary, err := s.addressPublisher.GetAddressDimensionConsumerSummary(ctx, activation.SnapshotID)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"at": at, "activation": activation, "snapshot": snapshot, "consumers": summary})
}

func (s *Server) listAddressDimensionConsumers(c *gin.Context) {
	filter := address.AddressDimensionConsumerFilter{
		Query: strings.TrimSpace(c.Query("q")), State: strings.TrimSpace(c.Query("state")),
		Drift: strings.TrimSpace(c.Query("drift")), Cursor: strings.TrimSpace(c.Query("cursor")),
	}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		filter.Limit = limit
	}
	ctx := c.Request.Context()
	snapshotID := c.Param("id")
	items, cursor, err := s.addressPublisher.ListAddressDimensionConsumers(ctx, snapshotID, filter)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	summary, err := s.addressPublisher.GetAddressDimensionConsumerSummary(ctx, snapshotID)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	if items == nil {
		items = []address.AddressDimensionConsumerStatus{}
	}
	response := gin.H{"items": items, "summary": summary}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) listAddressDimensionGCCandidates(c *gin.Context) {
	filter := address.AddressDimensionGCFilter{Cursor: strings.TrimSpace(c.Query("cursor"))}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := s.addressPublisher.ListAddressDimensionGCCandidates(c.Request.Context(), time.Now().UTC(), filter)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	if items == nil {
		items = []address.AddressDimensionGCCandidate{}
	}
	response := gin.H{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) scheduleAddressDimensionGC(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var req struct {
		RetentionUntil time.Time `json:"retention_until"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	snapshot, err := s.addressPublisher.ScheduleAddressDimensionObjectGC(c.Request.Context(), currentPrincipal(c).UserID, c.Param("id"), expected, req.RetentionUntil)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	c.Header("ETag", etag(snapshot.RowVersion))
	c.JSON(http.StatusOK, snapshot)
}
