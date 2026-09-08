package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type agentRecord struct {
	ID, Name, Kind, Status, Health, SoftwareVersion, APIVersion string
	Capabilities                                                json.RawMessage
	DeviceID, Role, Mode, Endpoint                              sql.NullString
	LastSeen, LastRun, LastSuccess                              sql.NullTime
	LastError                                                   sql.NullString
	RunCount, FailureCount, DesiredPlan, AckedPlan, RowVersion  uint64
	CreatedAt, UpdatedAt                                        time.Time
}

type agentDTO struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Kind               string          `json:"kind"`
	AgentType          string          `json:"agent_type"`
	DeviceID           string          `json:"device_id,omitempty"`
	TargetID           string          `json:"target_id,omitempty"`
	Role               string          `json:"role,omitempty"`
	Mode               string          `json:"mode,omitempty"`
	Endpoint           string          `json:"endpoint,omitempty"`
	Status             string          `json:"status"`
	Health             string          `json:"health"`
	SoftwareVersion    string          `json:"software_version,omitempty"`
	APIVersion         string          `json:"api_version,omitempty"`
	Capabilities       json.RawMessage `json:"capabilities"`
	DesiredPlanVersion uint64          `json:"desired_plan_version"`
	AckedPlanVersion   uint64          `json:"acked_plan_version"`
	LastSeen           any             `json:"last_seen,omitempty"`
	LastRun            any             `json:"last_run,omitempty"`
	LastSuccess        any             `json:"last_success,omitempty"`
	LastError          string          `json:"last_error,omitempty"`
	RunCount           uint64          `json:"run_count"`
	FailureCount       uint64          `json:"failure_count"`
	RowVersion         uint64          `json:"row_version"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
}

func (r agentRecord) dto() agentDTO {
	capabilities := r.Capabilities
	if len(capabilities) == 0 {
		capabilities = json.RawMessage(`[]`)
	}
	return agentDTO{
		ID: r.ID, Name: r.Name, Kind: r.Kind, AgentType: r.Kind, DeviceID: r.DeviceID.String,
		TargetID: r.DeviceID.String, Role: r.Role.String, Mode: r.Mode.String, Endpoint: r.Endpoint.String,
		Status: r.Status, Health: r.Health, SoftwareVersion: r.SoftwareVersion, APIVersion: r.APIVersion,
		Capabilities: capabilities, DesiredPlanVersion: r.DesiredPlan, AckedPlanVersion: r.AckedPlan,
		LastSeen: nullableTime(r.LastSeen), LastRun: nullableTime(r.LastRun), LastSuccess: nullableTime(r.LastSuccess),
		LastError: r.LastError.String, RunCount: r.RunCount, FailureCount: r.FailureCount,
		RowVersion: r.RowVersion, CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

type agentMutation struct {
	ID              string          `json:"id"`
	Name            *string         `json:"name"`
	Kind            *string         `json:"kind"`
	LegacyAgentType *string         `json:"AgentType"`
	DeviceID        *string         `json:"device_id"`
	LegacyTargetID  *string         `json:"TargetID"`
	Role            *string         `json:"role"`
	Mode            *string         `json:"mode"`
	Endpoint        *string         `json:"endpoint"`
	Status          *string         `json:"status"`
	Token           *string         `json:"token"`
	SoftwareVersion *string         `json:"software_version"`
	APIVersion      *string         `json:"api_version"`
	Capabilities    json.RawMessage `json:"capabilities"`
}

func (s *Server) listAgents(c *gin.Context) {
	limit, offset := pageParams(c)
	where := []string{"1=1"}
	args := []any{}
	kind := strings.TrimSpace(c.Query("kind"))
	if kind == "" {
		kind = strings.TrimSpace(c.Query("agent_type"))
	}
	if kind != "" {
		where = append(where, "a.kind=?")
		args = append(args, kind)
	}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where = append(where, "a.status=?")
		args = append(args, status)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(a.id LIKE ? OR a.name LIKE ? OR a.kind LIKE ? OR COALESCE(b.endpoint,'') LIKE ? OR COALESCE(a.last_error,'') LIKE ? OR COALESCE(b.device_id,'') LIKE ?)")
		like := "%" + escapeLike(q) + "%"
		for range 6 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM agents a LEFT JOIN agent_bindings b ON b.agent_id=a.id`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	sorts := map[string]string{"": "a.updated_at", "id": "a.id", "name": "a.name", "kind": "a.kind", "agent_type": "a.kind", "target_id": "b.device_id", "mode": "b.mode", "status": "a.status", "last_seen_at": "a.last_seen_at", "run_count": "a.run_count", "failure_count": "a.failure_count", "updated_at": "a.updated_at"}
	sortColumn := sorts[c.Query("sort")]
	if sortColumn == "" {
		sortColumn = sorts[""]
	}
	query := agentSelect + clause + fmt.Sprintf(" ORDER BY %s %s, a.id %s LIMIT ? OFFSET ?", sortColumn, sortDirection(c), sortDirection(c))
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]agentDTO, 0, limit)
	for rows.Next() {
		r, err := scanAgent(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, r.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

const agentSelect = `SELECT a.id,a.name,a.kind,a.status,a.health,a.software_version,a.api_version,a.capabilities_json,b.device_id,b.role,b.mode,b.endpoint,a.last_seen_at,a.last_run_at,a.last_success_at,a.last_error,a.run_count,a.failure_count,a.desired_plan_version,a.acked_plan_version,a.row_version,a.created_at,a.updated_at FROM agents a LEFT JOIN agent_bindings b ON b.agent_id=a.id`

type scanner interface{ Scan(...any) error }

func scanAgent(row scanner) (agentRecord, error) {
	var r agentRecord
	err := row.Scan(&r.ID, &r.Name, &r.Kind, &r.Status, &r.Health, &r.SoftwareVersion, &r.APIVersion, &r.Capabilities, &r.DeviceID, &r.Role, &r.Mode, &r.Endpoint, &r.LastSeen, &r.LastRun, &r.LastSuccess, &r.LastError, &r.RunCount, &r.FailureCount, &r.DesiredPlan, &r.AckedPlan, &r.RowVersion, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (s *Server) readAgent(ctx context.Context, id string) (agentRecord, error) {
	return scanAgent(s.db.QueryRowContext(ctx, agentSelect+` WHERE a.id=?`, id))
}

func (s *Server) getAgent(c *gin.Context) {
	r, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) createAgent(c *gin.Context) {
	var req agentMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	kind := mutationKind(req)
	status := valueOr(req.Status, "pending")
	mode := valueOr(req.Mode, "push")
	if !validAgentKind(kind) || !validAgentStatus(status) || !validAgentMode(mode) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid agent kind, status, or mode")
		return
	}
	token := valueOr(req.Token, "")
	if token == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "token is required and is stored only as SHA-256")
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = newID()
	}
	if len(id) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "id must not exceed 26 characters")
		return
	}
	deviceID := firstPointer(req.DeviceID, req.LegacyTargetID)
	capabilities, err := normalizeCapabilities(req.Capabilities, false)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agents (id,name,kind,status,health,software_version,api_version,capabilities_json,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?,?)`, id, valueOr(req.Name, id), kind, status, "unknown", valueOr(req.SoftwareVersion, ""), valueOr(req.APIVersion, ""), capabilities, principalUserID(c), principalUserID(c))
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`, newID(), id, sha256hex(token))
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_bindings (id,agent_id,device_id,role,mode,endpoint) VALUES (?,?,?,?,?,?)`, newID(), id, nullableString(deviceID), valueOr(req.Role, kind), mode, valueOr(req.Endpoint, ""))
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	r, err := s.readAgent(c.Request.Context(), id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.Header("Location", "/api/v1/agents/"+id)
	c.JSON(http.StatusCreated, r.dto())
}

func (s *Server) updateAgent(c *gin.Context) {
	var req agentMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	old, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil || (supplied && expected != old.RowVersion) {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "agent changed since it was loaded")
		return
	}
	kind := old.Kind
	if req.Kind != nil || req.LegacyAgentType != nil {
		kind = mutationKind(req)
	}
	status, mode := patchString(req.Status, old.Status), patchString(req.Mode, old.Mode.String)
	if !validAgentKind(kind) || !validAgentStatus(status) || !validAgentMode(mode) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid agent kind, status, or mode")
		return
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, true)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		var result sql.Result
		if capabilities == "" {
			capabilities = string(old.Capabilities)
		}
		result, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET name=?,kind=?,status=?,software_version=?,api_version=?,capabilities_json=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`, patchString(req.Name, old.Name), kind, status, patchString(req.SoftwareVersion, old.SoftwareVersion), patchString(req.APIVersion, old.APIVersion), capabilities, principalUserID(c), old.ID, old.RowVersion)
		if err == nil {
			if changed, _ := result.RowsAffected(); changed != 1 {
				err = errVersionConflict
			}
		}
	}
	deviceID := old.DeviceID
	if v := firstPointer(req.DeviceID, req.LegacyTargetID); v != nil {
		deviceID = sql.NullString{String: strings.TrimSpace(*v), Valid: strings.TrimSpace(*v) != ""}
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_bindings (id,agent_id,device_id,role,mode,endpoint) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE device_id=VALUES(device_id),role=VALUES(role),mode=VALUES(mode),endpoint=VALUES(endpoint),row_version=row_version+1`, newID(), old.ID, nullStringValue(deviceID), patchString(req.Role, old.Role.String), mode, patchString(req.Endpoint, old.Endpoint.String))
	}
	if err == nil && req.Token != nil && strings.TrimSpace(*req.Token) != "" {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET status='revoked',revoked_at=NOW(3) WHERE agent_id=? AND status='active'`, old.ID)
		if err == nil {
			_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`, newID(), old.ID, sha256hex(strings.TrimSpace(*req.Token)))
		}
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	r, err := s.readAgent(c.Request.Context(), old.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) deleteAgent(c *gin.Context) {
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM agents WHERE id=?`, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if n, _ := result.RowsAffected(); n == 0 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) agentHeartbeat(c *gin.Context) {
	if !s.authenticateAgent(c, c.Param("id")) {
		return
	}
	var req struct {
		SoftwareVersion string          `json:"software_version"`
		APIVersion      string          `json:"api_version"`
		Capabilities    json.RawMessage `json:"capabilities"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "invalid heartbeat body")
			return
		}
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, true)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	_, err = s.db.ExecContext(c.Request.Context(), `UPDATE agents SET last_seen_at=NOW(3),status=IF(status='disabled',status,'up'),health=IF(status='disabled',health,'ok'),software_version=IF(?='',software_version,?),api_version=IF(?='',api_version,?),capabilities_json=IF(?='',capabilities_json,CAST(? AS JSON)),row_version=row_version+1 WHERE id=?`, req.SoftwareVersion, req.SoftwareVersion, req.APIVersion, req.APIVersion, capabilities, capabilities, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true})
}

func (s *Server) authenticateAgent(c *gin.Context, agentID string) bool {
	token := strings.TrimSpace(c.GetHeader("X-Watchdog-Agent-Token"))
	if token == "" {
		if value := c.GetHeader("Authorization"); strings.HasPrefix(value, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		}
	}
	if token == "" {
		fail(c, http.StatusUnauthorized, "unauthorized", "agent token is required")
		return false
	}
	var status string
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT a.status FROM agent_credentials ac JOIN agents a ON a.id=ac.agent_id WHERE ac.agent_id=? AND ac.token_sha256=? AND ac.status='active' AND ac.revoked_at IS NULL AND (ac.expires_at IS NULL OR ac.expires_at>NOW(3))`, agentID, sha256hex(token)).Scan(&status)
	if err != nil || status == "disabled" {
		fail(c, http.StatusUnauthorized, "unauthorized", "invalid or disabled agent credential")
		return false
	}
	_, _ = s.db.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET last_used_at=NOW(3) WHERE agent_id=? AND token_sha256=?`, agentID, sha256hex(token))
	return true
}

type agentRunMutation struct {
	Status          string `json:"status"`
	Error           string `json:"error"`
	Seen            bool   `json:"seen"`
	StartedAt       string `json:"started_at"`
	LegacyStartedAt string `json:"StartedAt"`
	EndedAt         string `json:"ended_at"`
	LegacyEndedAt   string `json:"EndedAt"`
	DurationMS      uint64 `json:"duration_ms"`
}

func (s *Server) recordAgentStatus(c *gin.Context) {
	if !s.authenticateAgent(c, c.Param("id")) {
		return
	}
	var req agentRunMutation
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "invalid run body")
			return
		}
	}
	if req.Status == "" {
		req.Status = "success"
	}
	if c.FullPath() == "/api/v1/agents/:id/errors" {
		req.Status = "failure"
	}
	if req.Status != "success" && req.Status != "failure" {
		fail(c, http.StatusBadRequest, "invalid_request", "status must be success or failure")
		return
	}
	now := time.Now().UTC()
	started, err := parseRunTime(firstNonEmpty(req.StartedAt, req.LegacyStartedAt), now)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "started_at must be RFC3339")
		return
	}
	ended, err := parseRunTime(firstNonEmpty(req.EndedAt, req.LegacyEndedAt), now)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "ended_at must be RFC3339")
		return
	}
	if req.DurationMS == 0 && ended.After(started) {
		req.DurationMS = uint64(ended.Sub(started).Milliseconds())
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_runs (id,agent_id,status,error_text,seen,started_at,ended_at,duration_ms) VALUES (?,?,?,NULLIF(?,''),?,?,?,?)`, newID(), c.Param("id"), req.Status, req.Error, req.Seen, started, ended, req.DurationMS)
	}
	if err == nil {
		failure := req.Status == "failure"
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET last_seen_at=NOW(3),last_run_at=?,last_success_at=IF(?,last_success_at,?),last_error=IF(?,NULLIF(?,''),NULL),run_count=run_count+1,failure_count=failure_count+IF(?,1,0),health=IF(?,'error','ok'),status=IF(status='disabled',status,IF(?,'error','up')),row_version=row_version+1 WHERE id=?`, ended, failure, ended, failure, req.Error, failure, failure, failure, c.Param("id"))
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true})
}

func (s *Server) listAgentRuns(c *gin.Context) {
	limit, offset := pageParams(c)
	where := []string{"agent_id=?"}
	args := []any{c.Param("id")}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where = append(where, "status=?")
		args = append(args, status)
	}
	if seen := strings.TrimSpace(c.Query("seen")); seen != "" {
		v, err := strconv.ParseBool(seen)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "seen must be true or false")
			return
		}
		where = append(where, "seen=?")
		args = append(args, v)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(id LIKE ? OR COALESCE(error_text,'') LIKE ?)")
		like := "%" + escapeLike(q) + "%"
		args = append(args, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM agent_runs`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	sorts := map[string]string{"": "ended_at", "id": "id", "status": "status", "started": "started_at", "ended": "ended_at", "duration": "duration_ms", "seen": "seen"}
	sortColumn := sorts[c.Query("sort")]
	if sortColumn == "" {
		sortColumn = sorts[""]
	}
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,status,COALESCE(error_text,''),seen,started_at,ended_at,duration_ms FROM agent_runs`+clause+fmt.Sprintf(" ORDER BY %s %s,id %s LIMIT ? OFFSET ?", sortColumn, sortDirection(c), sortDirection(c)), queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, status, errorText string
		var seen bool
		var started, ended time.Time
		var duration uint64
		if err := rows.Scan(&id, &status, &errorText, &seen, &started, &ended, &duration); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, gin.H{"id": id, "status": status, "error": errorText, "seen": seen, "started_at": started.UTC().Format(time.RFC3339Nano), "ended_at": ended.UTC().Format(time.RFC3339Nano), "duration_ms": duration})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

func (s *Server) createEnrollmentToken(c *gin.Context) {
	var req struct {
		Kind             string `json:"kind"`
		ExpiresInSeconds int    `json:"expires_in_seconds"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
			return
		}
	}
	if req.Kind != "" && !validAgentKind(req.Kind) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid agent kind")
		return
	}
	if req.ExpiresInSeconds <= 0 {
		req.ExpiresInSeconds = 900
	}
	if req.ExpiresInSeconds > 86400 {
		req.ExpiresInSeconds = 86400
	}
	id, token := newID(), "wde_"+randomToken()
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO agent_enrollment_tokens (id,token_sha256,allowed_kind,expires_at,created_by) VALUES (?,?,?,DATE_ADD(NOW(3),INTERVAL ? SECOND),?)`, id, sha256hex(token), req.Kind, req.ExpiresInSeconds, principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"id": id, "token": token, "expires_in_seconds": req.ExpiresInSeconds})
}

func (s *Server) enrollAgent(c *gin.Context) {
	var req struct {
		EnrollmentToken string          `json:"enrollment_token"`
		ID              string          `json:"id"`
		Name            string          `json:"name"`
		Kind            string          `json:"kind"`
		DeviceID        string          `json:"device_id"`
		Role            string          `json:"role"`
		Mode            string          `json:"mode"`
		Endpoint        string          `json:"endpoint"`
		SoftwareVersion string          `json:"software_version"`
		APIVersion      string          `json:"api_version"`
		Capabilities    json.RawMessage `json:"capabilities"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.EnrollmentToken == "" || !validAgentKind(req.Kind) {
		fail(c, http.StatusBadRequest, "invalid_request", "enrollment_token and valid kind are required")
		return
	}
	if req.Mode == "" {
		req.Mode = "push"
	}
	if !validAgentMode(req.Mode) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid mode")
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = newID()
	}
	if len(id) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "id must not exceed 26 characters")
		return
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, false)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	credential := "wda_" + randomToken()
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	var enrollmentID, allowedKind string
	if err == nil {
		err = tx.QueryRowContext(c.Request.Context(), `SELECT id,allowed_kind FROM agent_enrollment_tokens WHERE token_sha256=? AND consumed_at IS NULL AND expires_at>NOW(3) FOR UPDATE`, sha256hex(req.EnrollmentToken)).Scan(&enrollmentID, &allowedKind)
	}
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		fail(c, http.StatusUnauthorized, "invalid_enrollment", "enrollment token is invalid, expired, or already used")
		return
	}
	if err == nil && allowedKind != "" && allowedKind != req.Kind {
		_ = tx.Rollback()
		fail(c, http.StatusBadRequest, "invalid_enrollment", "enrollment token does not allow this agent kind")
		return
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agent_enrollment_tokens SET consumed_at=NOW(3) WHERE id=? AND consumed_at IS NULL`, enrollmentID)
	}
	if err == nil {
		name := strings.TrimSpace(req.Name)
		if name == "" {
			name = id
		}
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agents (id,name,kind,status,health,software_version,api_version,capabilities_json) VALUES (?,?,?,'pending','unknown',?,?,?)`, id, name, req.Kind, req.SoftwareVersion, req.APIVersion, capabilities)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`, newID(), id, sha256hex(credential))
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_bindings (id,agent_id,device_id,role,mode,endpoint) VALUES (?,?,?,?,?,?)`, newID(), id, nullableText(req.DeviceID), req.Role, req.Mode, req.Endpoint)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	r, err := s.readAgent(c.Request.Context(), id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"agent": r.dto(), "credential": gin.H{"token": credential}})
}

func validAgentKind(v string) bool {
	switch v {
	case "snmp", "system", "flow_collect", "flow_worker", "probe":
		return true
	}
	return false
}
func validAgentStatus(v string) bool {
	switch v {
	case "pending", "up", "down", "error", "disabled":
		return true
	}
	return false
}
func validAgentMode(v string) bool { return v == "push" || v == "pull" }
func mutationKind(r agentMutation) string {
	if r.Kind != nil {
		return strings.TrimSpace(*r.Kind)
	}
	return valueOr(r.LegacyAgentType, "")
}
func normalizeCapabilities(v json.RawMessage, keepExistingWhenEmpty bool) (string, error) {
	if len(v) == 0 {
		if keepExistingWhenEmpty {
			return "", nil
		}
		return `[]`, nil
	}
	if len(v) > 64*1024 || !json.Valid(v) {
		return "", errors.New("capabilities must be valid JSON no larger than 64 KiB")
	}
	var values []string
	if err := json.Unmarshal(v, &values); err != nil {
		return "", errors.New("capabilities must be an array of versioned strings")
	}
	if len(values) > 128 {
		return "", errors.New("capabilities must contain at most 128 entries")
	}
	for _, value := range values {
		if len(value) == 0 || len(value) > 128 || !strings.Contains(value, "/v") {
			return "", errors.New("each capability must be a non-empty versioned string such as snmp.poll/v2")
		}
	}
	return string(v), nil
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func nullStringValue(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}
func nullableText(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return strings.TrimSpace(v)
}
func parseRunTime(v string, fallback time.Time) (time.Time, error) {
	if strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	return t.UTC(), err
}
func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
