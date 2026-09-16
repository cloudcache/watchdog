// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/gin-gonic/gin"
)

type flowRawDeleteBarrierACK struct {
	Revision        uint64 `json:"revision"`
	BootID          string `json:"boot_id"`
	SoftwareVersion string `json:"software_version"`
	State           string `json:"state"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

func (s *Server) fetchFlowWorkerRawDeleteBarrier(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	store := s.flowLifecycle
	if store == nil {
		store = flowlifecycle.NewStore(s.db)
	}
	barrier, err := store.LatestRawDeleteBarrier(c.Request.Context())
	if errors.Is(err, sql.ErrNoRows) {
		c.Status(http.StatusNoContent)
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, barrier)
}

func (s *Server) acknowledgeFlowWorkerRawDeleteBarrier(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var request flowRawDeleteBarrierACK
	if err := c.ShouldBindJSON(&request); err != nil || !validFlowRawDeleteBarrierACK(request) {
		fail(c, http.StatusBadRequest, "invalid_request", "raw-delete barrier acknowledgement is invalid")
		return
	}
	store := s.flowLifecycle
	if store == nil {
		store = flowlifecycle.NewStore(s.db)
	}
	err := store.AcknowledgeRawDeleteBarrier(c.Request.Context(), c.Param("barrier_id"), c.Param("id"), request.BootID,
		request.SoftwareVersion, request.State, request.ErrorCode, request.ErrorMessage, request.Revision, time.Now().UTC())
	if err != nil {
		if errors.Is(err, flowtombstone.ErrInvalidBarrier) || errors.Is(err, sql.ErrNoRows) {
			fail(c, http.StatusConflict, "raw_delete_barrier_conflict", err.Error())
			return
		}
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true})
}

func validFlowRawDeleteBarrierACK(request flowRawDeleteBarrierACK) bool {
	if request.Revision == 0 || request.BootID == "" || len(request.BootID) > 128 || !printableASCII(request.BootID) ||
		request.SoftwareVersion == "" || len(request.SoftwareVersion) > 64 || !printableASCII(request.SoftwareVersion) ||
		(request.State != "installed" && request.State != "failed") || len(request.ErrorCode) > 64 || len(request.ErrorMessage) > 512 {
		return false
	}
	if request.State == "installed" {
		return request.ErrorCode == "" && request.ErrorMessage == ""
	}
	return strings.TrimSpace(request.ErrorCode) != "" && printableASCII(request.ErrorCode) && printableASCIIText(request.ErrorMessage)
}
