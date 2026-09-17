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
	HeartbeatInterval                                           uint32
	ClockSkewSeconds                                            int
	CreatedAt, UpdatedAt                                        time.Time
}

var errAgentRevoked = errors.New("agent is revoked")

type agentDTO struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Kind               string          `json:"kind"`
	DeviceID           string          `json:"device_id,omitempty"`
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
	HeartbeatInterval  uint32          `json:"heartbeat_interval_seconds"`
	ClockSkewSeconds   int             `json:"clock_skew_seconds"`
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
		ID: r.ID, Name: r.Name, Kind: r.Kind, DeviceID: r.DeviceID.String,
		Role: r.Role.String, Mode: r.Mode.String, Endpoint: r.Endpoint.String,
		Status: r.Status, Health: r.effectiveHealth(time.Now().UTC()), SoftwareVersion: r.SoftwareVersion, APIVersion: r.APIVersion,
		Capabilities: capabilities, DesiredPlanVersion: r.DesiredPlan, AckedPlanVersion: r.AckedPlan,
		LastSeen: nullableTime(r.LastSeen), LastRun: nullableTime(r.LastRun), LastSuccess: nullableTime(r.LastSuccess),
		LastError: r.LastError.String, RunCount: r.RunCount, FailureCount: r.FailureCount,
		HeartbeatInterval: r.HeartbeatInterval, ClockSkewSeconds: r.ClockSkewSeconds,
		RowVersion: r.RowVersion, CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (r agentRecord) effectiveHealth(now time.Time) string {
	if r.Status == "revoked" {
		return "revoked"
	}
	if (r.Status == "active" || r.Status == "draining") && r.LastSeen.Valid {
		deadline := time.Duration(r.HeartbeatInterval*3) * time.Second
		if deadline < 3*time.Minute {
			deadline = 3 * time.Minute
		}
		if now.Sub(r.LastSeen.Time) > deadline {
			return "offline"
		}
	}
	return r.Health
}

type agentMutation struct {
	ID              string          `json:"id"`
	Name            *string         `json:"name"`
	Kind            *string         `json:"kind"`
	DeviceID        *string         `json:"device_id"`
	Role            *string         `json:"role"`
	Mode            *string         `json:"mode"`
	Endpoint        *string         `json:"endpoint"`
	Status          *string         `json:"status"`
	Token           *string         `json:"token"`
	SoftwareVersion *string         `json:"software_version"`
	APIVersion      *string         `json:"api_version"`
	Capabilities    json.RawMessage `json:"capabilities"`
	MTLSFingerprint *string         `json:"mtls_fingerprint"`
}

func (s *Server) listAgents(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"kind", "status"}, map[string]string{
		"id": "a.id", "name": "a.name", "kind": "a.kind", "device_id": "b.device_id",
		"mode": "b.mode", "status": "a.status", "health": "a.health",
		"last_seen_at": "a.last_seen_at", "run_count": "a.run_count", "failure_count": "a.failure_count", "updated_at": "a.updated_at",
	}, "updated_at")
	if !ok {
		return
	}
	where := []string{"1=1"}
	args := []any{}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, `(b.device_id IS NULL OR EXISTS (
			SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=b.device_id
		) OR EXISTS (
			SELECT 1 FROM user_device_group_permissions udgp
			JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id
			WHERE udgp.user_id=? AND dgm.device_id=b.device_id
		))`)
		args = append(args, p.UserID, p.UserID)
	}
	kind := strings.TrimSpace(c.Query("kind"))
	if kind != "" {
		if !validAgentKind(kind) {
			fail(c, http.StatusBadRequest, "invalid_filter", "invalid agent kind")
			return
		}
		where = append(where, "a.kind=?")
		args = append(args, kind)
	}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		status = normalizeAgentStatus(status)
		if !validAgentStatus(status) {
			fail(c, http.StatusBadRequest, "invalid_filter", "invalid agent status")
			return
		}
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
	query := agentSelect + clause + fmt.Sprintf(" ORDER BY %s %s, a.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order)
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]agentDTO, 0, page.Limit)
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

const agentSelect = `SELECT a.id,a.name,a.kind,a.status,a.health,a.software_version,a.api_version,a.capabilities_json,b.device_id,b.role,b.mode,b.endpoint,a.last_seen_at,a.last_run_at,a.last_success_at,a.last_error,a.run_count,a.failure_count,a.desired_plan_version,a.acked_plan_version,a.row_version,a.heartbeat_interval_seconds,a.clock_skew_seconds,a.created_at,a.updated_at FROM agents a LEFT JOIN agent_bindings b ON b.agent_id=a.id`

type scanner interface{ Scan(...any) error }

func scanAgent(row scanner) (agentRecord, error) {
	var r agentRecord
	err := row.Scan(&r.ID, &r.Name, &r.Kind, &r.Status, &r.Health, &r.SoftwareVersion, &r.APIVersion, &r.Capabilities, &r.DeviceID, &r.Role, &r.Mode, &r.Endpoint, &r.LastSeen, &r.LastRun, &r.LastSuccess, &r.LastError, &r.RunCount, &r.FailureCount, &r.DesiredPlan, &r.AckedPlan, &r.RowVersion, &r.HeartbeatInterval, &r.ClockSkewSeconds, &r.CreatedAt, &r.UpdatedAt)
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
	if !s.requireAgentAccess(c, r) {
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) createAgent(c *gin.Context) {
	var req agentMutation
	if !decodeStrictBody(c, &req) {
		return
	}
	kind := valueOr(req.Kind, "")
	status := normalizeAgentStatus(valueOr(req.Status, "registered"))
	mode := valueOr(req.Mode, "push")
	if !validAgentKind(kind) || !validAgentStatus(status) || !validAgentMode(mode) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid agent kind, status, or mode")
		return
	}
	token := strings.TrimSpace(valueOr(req.Token, ""))
	fingerprint := normalizeFingerprint(valueOr(req.MTLSFingerprint, ""))
	if req.MTLSFingerprint != nil && strings.TrimSpace(*req.MTLSFingerprint) != "" && fingerprint == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "mtls_fingerprint must be a SHA-256 fingerprint")
		return
	}
	if token == "" && fingerprint == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "token or mTLS fingerprint is required")
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
	deviceID := req.DeviceID
	if !s.validateAgentBindingDeviceForKind(c, valueOr(deviceID, ""), kind) {
		return
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, false)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	apiVersion := valueOr(req.APIVersion, "v1")
	if err := validateAgentFields(id, valueOr(req.Name, id), kind, valueOr(req.Role, kind), mode, valueOr(req.Endpoint, ""), valueOr(req.SoftwareVersion, ""), apiVersion); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateAgentCompatibility(kind, apiVersion, json.RawMessage(capabilities)); err != nil {
		fail(c, http.StatusBadRequest, "incompatible_agent", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agents (id,name,kind,status,health,software_version,api_version,capabilities_json,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?,?)`, id, valueOr(req.Name, id), kind, status, "unknown", valueOr(req.SoftwareVersion, ""), apiVersion, capabilities, principalUserID(c), principalUserID(c))
	}
	if err == nil && token != "" {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`, newID(), id, sha256hex(token))
	}
	if err == nil && fingerprint != "" {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,mtls_fingerprint) VALUES (?,?,'mtls',?)`, newID(), id, fingerprint)
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
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "agent.create", "agent", id)
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
	if !decodeStrictBody(c, &req) {
		return
	}
	if req.Token != nil || req.MTLSFingerprint != nil {
		fail(c, http.StatusBadRequest, "use_credential_rotation", "use the credential rotation operation to replace agent credentials")
		return
	}
	old, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, old) {
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil || (supplied && expected != old.RowVersion) {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "agent changed since it was loaded")
		return
	}
	kind := old.Kind
	if req.Kind != nil {
		kind = strings.TrimSpace(*req.Kind)
		if kind != old.Kind {
			fail(c, http.StatusConflict, "immutable_kind", "agent kind cannot be changed after registration")
			return
		}
	}
	status, mode := normalizeAgentStatus(patchString(req.Status, old.Status)), patchString(req.Mode, old.Mode.String)
	if !validAgentKind(kind) || !validAgentStatus(status) || !validAgentMode(mode) {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid agent kind, status, or mode")
		return
	}
	if old.Status == "revoked" && status != "revoked" {
		fail(c, http.StatusConflict, "agent_revoked", "a revoked agent cannot be reactivated")
		return
	}
	if old.Status != "revoked" && status == "revoked" {
		fail(c, http.StatusConflict, "use_revoke", "use the revoke operation so credentials are revoked atomically")
		return
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, true)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if capabilities == "" {
		capabilities = string(old.Capabilities)
	}
	apiVersion := patchString(req.APIVersion, old.APIVersion)
	if err := validateAgentCompatibility(kind, apiVersion, json.RawMessage(capabilities)); err != nil {
		fail(c, http.StatusBadRequest, "incompatible_agent", err.Error())
		return
	}
	deviceID := old.DeviceID
	if v := req.DeviceID; v != nil {
		deviceID = sql.NullString{String: strings.TrimSpace(*v), Valid: strings.TrimSpace(*v) != ""}
	}
	if !s.validateAgentBindingDeviceForKind(c, deviceID.String, kind) {
		return
	}
	if err := validateAgentFields(old.ID, patchString(req.Name, old.Name), kind, patchString(req.Role, old.Role.String), mode, patchString(req.Endpoint, old.Endpoint.String), patchString(req.SoftwareVersion, old.SoftwareVersion), apiVersion); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		var result sql.Result
		result, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET name=?,kind=?,status=?,software_version=?,api_version=?,capabilities_json=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`, patchString(req.Name, old.Name), kind, status, patchString(req.SoftwareVersion, old.SoftwareVersion), apiVersion, capabilities, principalUserID(c), old.ID, old.RowVersion)
		if err == nil {
			if changed, _ := result.RowsAffected(); changed != 1 {
				err = errVersionConflict
			}
		}
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_bindings (id,agent_id,device_id,role,mode,endpoint) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE device_id=VALUES(device_id),role=VALUES(role),mode=VALUES(mode),endpoint=VALUES(endpoint),row_version=row_version+1`, newID(), old.ID, nullStringValue(deviceID), patchString(req.Role, old.Role.String), mode, patchString(req.Endpoint, old.Endpoint.String))
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
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "agent.update", "agent", old.ID)
	r, err := s.readAgent(c.Request.Context(), old.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto())
}

func (s *Server) deleteAgent(c *gin.Context) {
	current, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, current) || !checkVersion(c, current.RowVersion, "agent") {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM agents WHERE id=? AND row_version=?`, current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if n, _ := result.RowsAffected(); n != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "agent.delete", "agent", current.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) rotateAgentCredential(c *gin.Context) {
	var req struct {
		AuthType         string `json:"auth_type"`
		MTLSFingerprint  string `json:"mtls_fingerprint"`
		ExpiresInSeconds int64  `json:"expires_in_seconds"`
	}
	if c.Request.ContentLength > 0 {
		if !decodeStrictBody(c, &req) {
			return
		}
	}
	if req.AuthType == "" {
		req.AuthType = "token"
	}
	if req.AuthType != "token" && req.AuthType != "mtls" {
		fail(c, http.StatusBadRequest, "invalid_request", "auth_type must be token or mtls")
		return
	}
	fingerprint := normalizeFingerprint(req.MTLSFingerprint)
	if req.AuthType == "mtls" && fingerprint == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "a SHA-256 mTLS fingerprint is required")
		return
	}
	if req.ExpiresInSeconds < 0 || req.ExpiresInSeconds > 365*86400 {
		fail(c, http.StatusBadRequest, "invalid_request", "expires_in_seconds must be 0..31536000")
		return
	}
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, agent) || !checkVersion(c, agent.RowVersion, "agent") {
		return
	}
	if agent.Status == "revoked" {
		fail(c, http.StatusConflict, "agent_revoked", "revoked agents cannot receive a new credential")
		return
	}
	token := ""
	credentialID := newID()
	var tokenHash any
	var fingerprintValue any
	if req.AuthType == "token" {
		token = "wda_" + randomToken()
		tokenHash = sha256hex(token)
	} else {
		fingerprintValue = fingerprint
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		var status string
		var rowVersion uint64
		err = tx.QueryRowContext(c.Request.Context(), `SELECT status,row_version FROM agents WHERE id=? FOR UPDATE`, agent.ID).Scan(&status, &rowVersion)
		if err == nil && rowVersion != agent.RowVersion {
			err = errVersionConflict
		}
		if err == nil && status == "revoked" {
			err = errAgentRevoked
		}
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET status='revoked',revoked_at=NOW(3) WHERE agent_id=? AND auth_type=? AND status='active'`, c.Param("id"), req.AuthType)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256,mtls_fingerprint,expires_at) VALUES (?,?,?,?,?,IF(?=0,NULL,DATE_ADD(NOW(3),INTERVAL ? SECOND)))`, credentialID, c.Param("id"), req.AuthType, tokenHash, fingerprintValue, req.ExpiresInSeconds, req.ExpiresInSeconds)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET row_version=row_version+1,updated_by=? WHERE id=? AND row_version=?`, principalUserID(c), agent.ID, agent.RowVersion)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		if errors.Is(err, errAgentRevoked) {
			fail(c, http.StatusConflict, "agent_revoked", "revoked agents cannot receive a new credential")
			return
		}
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "agent.credential.rotate", "agent", agent.ID)
	response := gin.H{"id": credentialID, "auth_type": req.AuthType}
	if token != "" {
		response["token"] = token
	}
	c.Header("ETag", etag(agent.RowVersion+1))
	c.JSON(http.StatusCreated, response)
}

func (s *Server) revokeAgent(c *gin.Context) {
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, agent) || !checkVersion(c, agent.RowVersion, "agent") {
		return
	}
	if agent.Status == "revoked" {
		c.Header("ETag", etag(agent.RowVersion))
		c.JSON(http.StatusOK, gin.H{"revoked": true})
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err == nil {
		var status string
		err = tx.QueryRowContext(c.Request.Context(), `SELECT status FROM agents WHERE id=? AND row_version=? FOR UPDATE`, agent.ID, agent.RowVersion).Scan(&status)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET status='revoked',revoked_at=COALESCE(revoked_at,NOW(3)) WHERE agent_id=? AND status='active'`, agent.ID)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET status='revoked',health='revoked',row_version=row_version+1,updated_by=? WHERE id=? AND row_version=?`, principalUserID(c), agent.ID, agent.RowVersion)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeSQLError(c, errVersionConflict)
			return
		}
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "agent.revoke", "agent", agent.ID)
	c.Header("ETag", etag(agent.RowVersion+1))
	c.JSON(http.StatusOK, gin.H{"revoked": true})
}

func (s *Server) agentHeartbeat(c *gin.Context) {
	if !s.authenticateAgent(c, c.Param("id")) {
		return
	}
	var req struct {
		SoftwareVersion string          `json:"software_version"`
		APIVersion      string          `json:"api_version"`
		Capabilities    json.RawMessage `json:"capabilities"`
		SentAt          string          `json:"sent_at"`
	}
	if c.Request.ContentLength > 0 {
		if !decodeStrictBody(c, &req) {
			return
		}
	}
	capabilities, err := normalizeCapabilities(req.Capabilities, true)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	current, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	apiVersion := strings.TrimSpace(req.APIVersion)
	if apiVersion == "" {
		apiVersion = current.APIVersion
	}
	capabilitiesForValidation := capabilities
	if capabilitiesForValidation == "" {
		capabilitiesForValidation = string(current.Capabilities)
	}
	if err := validateAgentCompatibility(current.Kind, apiVersion, json.RawMessage(capabilitiesForValidation)); err != nil {
		fail(c, http.StatusBadRequest, "incompatible_agent", err.Error())
		return
	}
	clockSkew := 0
	if strings.TrimSpace(req.SentAt) != "" {
		sentAt, parseErr := time.Parse(time.RFC3339Nano, req.SentAt)
		if parseErr != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "sent_at must be RFC3339")
			return
		}
		now := time.Now().UTC()
		if sentAt.Before(now.Add(-300*time.Second)) || sentAt.After(now.Add(300*time.Second)) {
			fail(c, http.StatusBadRequest, "clock_skew", "agent clock differs by more than 300 seconds")
			return
		}
		clockSkew = int(now.Sub(sentAt).Seconds())
	}
	_, err = s.db.ExecContext(c.Request.Context(), `UPDATE agents SET row_version=row_version+IF(status='registered',1,0),last_seen_at=NOW(3),status=IF(status='registered','active',status),health=IF(status='revoked',health,'ok'),software_version=IF(?='',software_version,?),api_version=?,capabilities_json=CAST(? AS JSON),clock_skew_seconds=? WHERE id=?`, req.SoftwareVersion, req.SoftwareVersion, apiVersion, capabilitiesForValidation, clockSkew, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"accepted": true, "desired_plan_version": current.DesiredPlan, "acked_plan_version": current.AckedPlan,
	})
}

func (s *Server) authenticateAgent(c *gin.Context, agentID string) bool {
	token := strings.TrimSpace(c.GetHeader("X-Watchdog-Agent-Token"))
	if token == "" {
		if value := c.GetHeader("Authorization"); strings.HasPrefix(value, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		}
	}
	fingerprint := requestCertificateFingerprint(c)
	if token == "" && fingerprint == "" {
		fail(c, http.StatusUnauthorized, "unauthorized", "agent token or client certificate is required")
		return false
	}
	var status string
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT a.status FROM agent_credentials ac JOIN agents a ON a.id=ac.agent_id WHERE ac.agent_id=? AND ((?<>'' AND ac.token_sha256=?) OR (?<>'' AND ac.mtls_fingerprint=?)) AND ac.status='active' AND ac.revoked_at IS NULL AND (ac.expires_at IS NULL OR ac.expires_at>NOW(3))`, agentID, token, sha256hex(token), fingerprint, fingerprint).Scan(&status)
	if err != nil || status == "revoked" {
		fail(c, http.StatusUnauthorized, "unauthorized", "invalid or revoked agent credential")
		return false
	}
	_, _ = s.db.ExecContext(c.Request.Context(), `UPDATE agent_credentials SET last_used_at=NOW(3) WHERE agent_id=? AND ((?<>'' AND token_sha256=?) OR (?<>'' AND mtls_fingerprint=?))`, agentID, token, sha256hex(token), fingerprint, fingerprint)
	return true
}

type agentRunMutation struct {
	LegacyAgentID   string `json:"AgentID"`
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
		if !decodeStrictBody(c, &req) {
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
	if req.LegacyAgentID != "" && req.LegacyAgentID != c.Param("id") {
		fail(c, http.StatusBadRequest, "invalid_request", "AgentID must match the route agent id")
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
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agents SET row_version=row_version+IF(status='registered',1,0),last_seen_at=NOW(3),last_run_at=?,last_success_at=IF(?,last_success_at,?),last_error=IF(?,NULLIF(?,''),NULL),run_count=run_count+1,failure_count=failure_count+IF(?,1,0),health=IF(?,'error','ok'),status=IF(status='registered','active',status) WHERE id=?`, ended, failure, ended, failure, req.Error, failure, failure, c.Param("id"))
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
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, agent) {
		return
	}
	page, ok := parseInventoryPage(c, []string{"status", "seen"}, map[string]string{
		"id": "id", "status": "status", "started": "started_at", "ended": "ended_at", "duration": "duration_ms", "seen": "seen",
	}, "ended")
	if !ok {
		return
	}
	where := []string{"agent_id=?"}
	args := []any{c.Param("id")}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		if status != "success" && status != "failure" {
			fail(c, http.StatusBadRequest, "invalid_filter", "status must be success or failure")
			return
		}
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
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,status,COALESCE(error_text,''),seen,started_at,ended_at,duration_ms FROM agent_runs`+clause+fmt.Sprintf(" ORDER BY %s %s,id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), queryArgs...)
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
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

func (s *Server) createEnrollmentToken(c *gin.Context) {
	var req struct {
		Kind             string `json:"kind"`
		DeviceID         string `json:"device_id"`
		ExpiresInSeconds int    `json:"expires_in_seconds"`
	}
	if c.Request.ContentLength > 0 {
		if !decodeStrictBody(c, &req) {
			return
		}
	}
	if !validAgentKind(req.Kind) {
		fail(c, http.StatusBadRequest, "invalid_request", "a valid agent kind is required")
		return
	}
	if !s.validateAgentBindingDeviceForKind(c, req.DeviceID, req.Kind) {
		return
	}
	if strings.TrimSpace(req.DeviceID) == "" && !currentPrincipal(c).can("device.viewAll") {
		fail(c, http.StatusForbidden, "forbidden", "unbound enrollment tokens require device.viewAll")
		return
	}
	if req.ExpiresInSeconds <= 0 {
		req.ExpiresInSeconds = 900
	}
	if req.ExpiresInSeconds > 86400 {
		req.ExpiresInSeconds = 86400
	}
	id, token := newID(), "wde_"+randomToken()
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO agent_enrollment_tokens (id,token_sha256,allowed_kind,device_id,expires_at,created_by) VALUES (?,?,?,?,DATE_ADD(NOW(3),INTERVAL ? SECOND),?)`, id, sha256hex(token), req.Kind, nullableText(req.DeviceID), req.ExpiresInSeconds, principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"id": id, "token": token, "device_id": strings.TrimSpace(req.DeviceID), "expires_in_seconds": req.ExpiresInSeconds})
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
	if !decodeStrictBody(c, &req) {
		return
	}
	if req.EnrollmentToken == "" || !validAgentKind(req.Kind) {
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
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = id
	}
	apiVersion := strings.TrimSpace(req.APIVersion)
	if apiVersion == "" {
		apiVersion = "v1"
	}
	role := strings.TrimSpace(req.Role)
	if role == "" {
		role = req.Kind
	}
	if err := validateAgentFields(id, name, req.Kind, role, req.Mode, req.Endpoint, req.SoftwareVersion, apiVersion); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateAgentCompatibility(req.Kind, apiVersion, json.RawMessage(capabilities)); err != nil {
		fail(c, http.StatusBadRequest, "incompatible_agent", err.Error())
		return
	}
	credential := "wda_" + randomToken()
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	var enrollmentID, allowedKind string
	var allowedDeviceID, createdBy sql.NullString
	if err == nil {
		err = tx.QueryRowContext(c.Request.Context(), `SELECT id,allowed_kind,device_id,created_by FROM agent_enrollment_tokens WHERE token_sha256=? AND consumed_at IS NULL AND expires_at>NOW(3) FOR UPDATE`, sha256hex(req.EnrollmentToken)).Scan(&enrollmentID, &allowedKind, &allowedDeviceID, &createdBy)
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
	if err == nil && strings.TrimSpace(req.DeviceID) != allowedDeviceID.String {
		_ = tx.Rollback()
		fail(c, http.StatusBadRequest, "invalid_enrollment", "enrollment token does not allow this device binding")
		return
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE agent_enrollment_tokens SET consumed_at=NOW(3) WHERE id=? AND consumed_at IS NULL`, enrollmentID)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agents (id,name,kind,status,health,software_version,api_version,capabilities_json,created_by,updated_by) VALUES (?,?,?,'registered','unknown',?,?,?,?,?)`, id, name, req.Kind, req.SoftwareVersion, apiVersion, capabilities, nullStringValue(createdBy), nullStringValue(createdBy))
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`, newID(), id, sha256hex(credential))
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO agent_bindings (id,agent_id,device_id,role,mode,endpoint) VALUES (?,?,?,?,?,?)`, newID(), id, nullableText(req.DeviceID), role, req.Mode, strings.TrimSpace(req.Endpoint))
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
	if createdBy.Valid {
		s.audit(c.Request.Context(), createdBy.String, "agent.enroll", "agent", id)
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
	case "registered", "active", "draining", "revoked":
		return true
	}
	return false
}

func normalizeAgentStatus(value string) string {
	switch strings.TrimSpace(value) {
	case "pending":
		return "registered"
	case "up", "down", "error":
		return "active"
	case "disabled":
		return "revoked"
	default:
		return strings.TrimSpace(value)
	}
}

func normalizeFingerprint(value string) string {
	value = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), ":", ""))
	if !validSHA256(value) {
		return ""
	}
	return value
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateAgentCompatibility(kind, apiVersion string, capabilities json.RawMessage) error {
	if apiVersion != "v1" {
		return fmt.Errorf("unsupported API version %q; supported version is v1", apiVersion)
	}
	var values []string
	if err := json.Unmarshal(capabilities, &values); err != nil {
		return errors.New("capabilities must be an array of versioned strings")
	}
	requiredPrefixes := map[string][]string{
		"system":       {"system.samples/"},
		"snmp":         {"snmp.poll/"},
		"flow_collect": {"flow.receive.sflow/", "flow.receive.netflow/"},
		"flow_worker":  {"flow.write.clickhouse/"},
		"probe":        {"probe.execute/"},
	}
	prefixes, ok := requiredPrefixes[kind]
	if !ok {
		return fmt.Errorf("unsupported agent kind %q", kind)
	}
	for _, capability := range values {
		for _, prefix := range prefixes {
			if strings.HasPrefix(capability, prefix) {
				return nil
			}
		}
	}
	return fmt.Errorf("agent kind %s requires one of capabilities %s", kind, strings.Join(prefixes, ", "))
}

func validateAgentFields(id, name, kind, role, mode, endpoint, softwareVersion, apiVersion string) error {
	switch {
	case strings.TrimSpace(id) == "" || len(id) > 26:
		return errors.New("id is required and must not exceed 26 characters")
	case strings.TrimSpace(name) == "" || len(name) > 190:
		return errors.New("name is required and must not exceed 190 characters")
	case strings.TrimSpace(kind) == "" || len(kind) > 32:
		return errors.New("kind is required and must not exceed 32 characters")
	case len(role) > 32:
		return errors.New("role must not exceed 32 characters")
	case !validAgentMode(mode):
		return errors.New("mode must be push or pull")
	case len(endpoint) > 512:
		return errors.New("endpoint must not exceed 512 characters")
	case len(softwareVersion) > 64:
		return errors.New("software_version must not exceed 64 characters")
	case strings.TrimSpace(apiVersion) == "" || len(apiVersion) > 32:
		return errors.New("api_version is required and must not exceed 32 characters")
	}
	return nil
}

func (s *Server) requireAgentAccess(c *gin.Context, agent agentRecord) bool {
	if !agent.DeviceID.Valid || strings.TrimSpace(agent.DeviceID.String) == "" {
		return true
	}
	return s.requireDeviceAccess(c, agent.DeviceID.String)
}

func (s *Server) agentBindingDeviceExists(c *gin.Context, deviceID string) bool {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return true
	}
	if len(deviceID) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "device_id must not exceed 26 characters")
		return false
	}
	var exists bool
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM devices WHERE id=?)`, deviceID).Scan(&exists); err != nil {
		writeSQLError(c, err)
		return false
	}
	if !exists {
		fail(c, http.StatusBadRequest, "invalid_reference", "referenced device does not exist")
		return false
	}
	return true
}

func (s *Server) validateAgentBindingDevice(c *gin.Context, deviceID string) bool {
	if !s.agentBindingDeviceExists(c, deviceID) {
		return false
	}
	deviceID = strings.TrimSpace(deviceID)
	return deviceID == "" || s.requireDeviceAccess(c, deviceID)
}

func (s *Server) validateAgentBindingDeviceForKind(c *gin.Context, deviceID, agentKind string) bool {
	if !s.validateAgentBindingDevice(c, deviceID) {
		return false
	}
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" || (agentKind != "system" && agentKind != "snmp") {
		return true
	}
	var deviceKind string
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT kind FROM devices WHERE id=?`, deviceID).Scan(&deviceKind); err != nil {
		writeSQLError(c, err)
		return false
	}
	want := "network"
	if agentKind == "system" {
		want = "host"
	}
	if deviceKind != want {
		fail(c, http.StatusBadRequest, "invalid_binding", fmt.Sprintf("%s agents require a %s device binding", agentKind, want))
		return false
	}
	return true
}

func requestCertificateFingerprint(c *gin.Context) string {
	if c.Request.TLS == nil || len(c.Request.TLS.PeerCertificates) == 0 {
		return ""
	}
	digest := sha256.Sum256(c.Request.TLS.PeerCertificates[0].Raw)
	return hex.EncodeToString(digest[:])
}
func validAgentMode(v string) bool { return v == "push" || v == "pull" }
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
