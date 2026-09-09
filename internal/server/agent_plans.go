package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

const (
	agentPlanRolloutJobType       = "agent.plan.rollout"
	agentPlanRolloutSchemaVersion = 1
)

type agentPlanRecord struct {
	ID, AgentID, Kind, SourceJobID, APIVersion, PayloadSHA256, SigningKeyID string
	PlanVersion, SupersedesPlanVersion                                      uint64
	SchemaVersion                                                           uint16
	Payload                                                                 json.RawMessage
	Signature                                                               []byte
	NotBefore                                                               sql.NullTime
	ExpiresAt, CreatedAt                                                    time.Time
	CreatedBy                                                               sql.NullString
}

type agentPlanDTO struct {
	ID                    string          `json:"id"`
	AgentID               string          `json:"agent_id"`
	Kind                  string          `json:"kind"`
	PlanVersion           uint64          `json:"plan_version"`
	SchemaVersion         uint16          `json:"schema_version"`
	APIVersion            string          `json:"api_version"`
	Payload               json.RawMessage `json:"payload"`
	PayloadSHA256         string          `json:"payload_sha256"`
	SigningKeyID          string          `json:"signing_key_id"`
	NotBefore             any             `json:"not_before,omitempty"`
	ExpiresAt             string          `json:"expires_at"`
	SupersedesPlanVersion uint64          `json:"supersedes_plan_version"`
	CreatedAt             string          `json:"created_at"`
}

func (p agentPlanRecord) dto() agentPlanDTO {
	return agentPlanDTO{
		ID: p.ID, AgentID: p.AgentID, Kind: p.Kind, PlanVersion: p.PlanVersion,
		SchemaVersion: p.SchemaVersion, APIVersion: p.APIVersion, Payload: p.Payload,
		PayloadSHA256: p.PayloadSHA256, SigningKeyID: p.SigningKeyID,
		NotBefore: nullableTime(p.NotBefore), ExpiresAt: p.ExpiresAt.UTC().Format(time.RFC3339Nano),
		SupersedesPlanVersion: p.SupersedesPlanVersion, CreatedAt: p.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

type createAgentPlanRequest struct {
	SchemaVersion              uint16          `json:"schema_version"`
	RequiredCapabilities       []string        `json:"required_capabilities"`
	Config                     json.RawMessage `json:"config"`
	NotBefore                  string          `json:"not_before"`
	ExpiresAt                  string          `json:"expires_at"`
	ExpiresInSeconds           int64           `json:"expires_in_seconds"`
	ExpectedDesiredPlanVersion *uint64         `json:"expected_desired_plan_version"`
}

type agentPlanCreateResult struct {
	Plan     agentPlanRecord
	Envelope []byte
}

func (s *Server) startAgentPlans() error {
	if strings.TrimSpace(s.cfg.AgentPlans.SigningPrivateKey) == "" {
		return nil
	}
	signer, publicKey, err := agentplan.LoadOrCreateSigner(s.cfg.AgentPlans.SigningPrivateKey, s.cfg.AgentPlans.SigningKeyID)
	if err != nil {
		return err
	}
	s.agentPlanSigner = signer
	s.agentPlanPublic = publicKey
	if s.jobs == nil {
		s.jobs = opjob.NewStore(s.db)
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	s.agentPlanCancel = cancel
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: agentPlanRolloutJobType, Owner: "watchdog-server/agent-plan",
		Handler: s.agentPlanRolloutHandler(),
	}
	go worker.Run(workerCtx)
	return nil
}

func (s *Server) requireAgentPlanSigner(c *gin.Context) bool {
	if len(s.agentPlanSigner.PrivateKey) == 0 {
		fail(c, http.StatusServiceUnavailable, "plan_signer_unavailable", "agent plan signing is not configured")
		return false
	}
	return true
}

func (s *Server) getAgentPlanPublicKey(c *gin.Context) {
	if !s.requireAgentPlanSigner(c) {
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"key_id":     s.agentPlanSigner.KeyID,
		"algorithm":  "ed25519",
		"public_key": base64.StdEncoding.EncodeToString(s.agentPlanPublic),
	})
}

func (s *Server) listAgentPlans(c *gin.Context) {
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, agent) {
		return
	}
	page, ok := parseInventoryPage(c, []string{"schema_version", "signing_key_id"}, map[string]string{
		"plan_version": "plan_version", "created_at": "created_at", "expires_at": "expires_at", "signing_key_id": "signing_key_id",
	}, "plan_version")
	if !ok {
		return
	}
	where := []string{"agent_id=?"}
	args := []any{agent.ID}
	if raw := strings.TrimSpace(c.Query("schema_version")); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || value == 0 {
			fail(c, http.StatusBadRequest, "invalid_filter", "schema_version must be a positive integer")
			return
		}
		where = append(where, "schema_version=?")
		args = append(args, value)
	}
	if value := strings.TrimSpace(c.Query("signing_key_id")); value != "" {
		if len(value) > 64 {
			fail(c, http.StatusBadRequest, "invalid_filter", "signing_key_id must not exceed 64 characters")
			return
		}
		where = append(where, "signing_key_id=?")
		args = append(args, value)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(id LIKE ? OR payload_sha256 LIKE ? OR signing_key_id LIKE ?)")
		like := "%" + escapeLike(q) + "%"
		args = append(args, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM agent_plans"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), agentPlanSelect+clause+fmt.Sprintf(" ORDER BY %s %s,id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order), append(append([]any{}, args...), page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]agentPlanDTO, 0, page.Limit)
	for rows.Next() {
		plan, err := scanAgentPlan(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, plan.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}

func (s *Server) createAgentPlan(c *gin.Context) {
	if !s.requireAgentPlanSigner(c) {
		return
	}
	var request createAgentPlanRequest
	if !decodeStrictBody(c, &request) {
		return
	}
	result, err := s.createAgentPlanRecord(c.Request.Context(), c.Param("id"), request, stringValue(principalUserID(c)), "")
	if err != nil {
		writeAgentPlanError(c, err)
		return
	}
	s.audit(c.Request.Context(), stringValue(principalUserID(c)), "agent.plan.publish", "agent", c.Param("id"))
	c.Header("ETag", agentPlanETag(result.Plan.PlanVersion, result.Plan.PayloadSHA256))
	c.Header("Location", fmt.Sprintf("/api/v1/agents/%s/plans/%d", c.Param("id"), result.Plan.PlanVersion))
	c.JSON(http.StatusCreated, result.Plan.dto())
}

func (s *Server) getAgentPlan(c *gin.Context) {
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireAgentAccess(c, agent) {
		return
	}
	version, err := strconv.ParseUint(c.Param("version"), 10, 64)
	if err != nil || version == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "plan version must be a positive integer")
		return
	}
	plan, err := s.readAgentPlan(c.Request.Context(), agent.ID, version)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", agentPlanETag(plan.PlanVersion, plan.PayloadSHA256))
	c.JSON(http.StatusOK, plan.dto())
}

func (s *Server) fetchAgentPlan(c *gin.Context) {
	if !s.authenticateAgent(c, c.Param("id")) {
		return
	}
	agent, err := s.readAgent(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if agent.DesiredPlan == 0 {
		c.Status(http.StatusNoContent)
		return
	}
	plan, err := s.readAgentPlan(c.Request.Context(), agent.ID, agent.DesiredPlan)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	now := time.Now().UTC()
	if (plan.NotBefore.Valid && now.Before(plan.NotBefore.Time)) || !now.Before(plan.ExpiresAt) {
		fail(c, http.StatusConflict, "plan_unavailable", "desired agent plan is outside its validity window")
		return
	}
	envelope, err := encodeAgentPlanEnvelope(plan)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	planETag := agentPlanETag(plan.PlanVersion, plan.PayloadSHA256)
	if strings.TrimSpace(c.GetHeader("If-None-Match")) == planETag {
		c.Status(http.StatusNotModified)
		return
	}
	c.Header("ETag", planETag)
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "application/json", envelope)
}

type agentPlanAckRequest struct {
	PlanVersion   uint64 `json:"plan_version"`
	PayloadSHA256 string `json:"payload_sha256"`
	Status        string `json:"status"`
	BootID        string `json:"boot_id"`
	Software      string `json:"software_version"`
	ErrorCode     string `json:"error_code"`
	Error         string `json:"error"`
}

func (s *Server) acknowledgeAgentPlan(c *gin.Context) {
	if !s.authenticateAgent(c, c.Param("id")) {
		return
	}
	var request agentPlanAckRequest
	if !decodeStrictBody(c, &request) {
		return
	}
	if err := validateAgentPlanAck(request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	created, err := s.persistAgentPlanAck(c.Request.Context(), c.Param("id"), request)
	if err != nil {
		writeAgentPlanError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "created": created})
}

func (s *Server) createAgentPlanRecord(ctx context.Context, agentID string, request createAgentPlanRequest, actor, sourceJobID string) (agentPlanCreateResult, error) {
	if len(s.agentPlanSigner.PrivateKey) == 0 {
		return agentPlanCreateResult{}, errors.New("agent plan signing is not configured")
	}
	if request.SchemaVersion == 0 {
		request.SchemaVersion = agentplan.SchemaVersion
	}
	if request.SchemaVersion != agentplan.SchemaVersion {
		return agentPlanCreateResult{}, errAgentPlanIncompatible
	}
	if len(request.Config) == 0 {
		request.Config = json.RawMessage(`{}`)
	}
	required := append([]string(nil), request.RequiredCapabilities...)
	slices.Sort(required)
	required = slices.Compact(required)
	now := time.Now().UTC().Truncate(time.Millisecond)
	notBefore, expiresAt, err := s.agentPlanValidity(request, now)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	defer tx.Rollback()
	var kind, apiVersion, status string
	var capabilitiesJSON []byte
	var desired uint64
	err = tx.QueryRowContext(ctx, `SELECT kind,api_version,status,capabilities_json,desired_plan_version FROM agents WHERE id=? FOR UPDATE`, agentID).Scan(&kind, &apiVersion, &status, &capabilitiesJSON, &desired)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	if status == "revoked" {
		return agentPlanCreateResult{}, errAgentRevoked
	}
	if request.ExpectedDesiredPlanVersion != nil && *request.ExpectedDesiredPlanVersion != desired {
		return agentPlanCreateResult{}, errVersionConflict
	}
	if sourceJobID != "" {
		if existing, findErr := readAgentPlanBySourceJob(ctx, tx, agentID, sourceJobID); findErr == nil {
			envelope, encodeErr := encodeAgentPlanEnvelope(existing)
			return agentPlanCreateResult{Plan: existing, Envelope: envelope}, encodeErr
		} else if !errors.Is(findErr, sql.ErrNoRows) {
			return agentPlanCreateResult{}, findErr
		}
	}
	var capabilities []string
	if err := json.Unmarshal(capabilitiesJSON, &capabilities); err != nil {
		return agentPlanCreateResult{}, err
	}
	spec := agentplan.Spec{Kind: kind, APIVersion: apiVersion, RequiredCapabilities: required, Config: request.Config}
	payloadInput, err := json.Marshal(spec)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	canonical, digest, normalized, err := agentplan.CanonicalSpec(payloadInput)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	if err := agentplan.ValidateCompatibility(normalized, agentID, kind, apiVersion, capabilities); err != nil {
		return agentPlanCreateResult{}, fmt.Errorf("%w: %v", errAgentPlanIncompatible, err)
	}
	version := desired + 1
	planID := newID()
	metadata := agentplan.Metadata{
		PlanID: planID, AgentID: agentID, PlanVersion: version, PlanSchemaVersion: request.SchemaVersion,
		Kind: kind, APIVersion: apiVersion, PayloadSHA256: digest, SigningKeyID: s.agentPlanSigner.KeyID,
		ExpiresAtUnixMilli: expiresAt.UnixMilli(), SupersedesPlanVersion: desired,
	}
	if !notBefore.IsZero() {
		metadata.NotBeforeUnixMilli = notBefore.UnixMilli()
	}
	envelope, signature, err := s.agentPlanSigner.Sign(metadata, canonical)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_plans (id,agent_id,kind,source_job_id,plan_version,schema_version,api_version,required_capabilities_json,payload_json,payload_sha256,signing_key_id,signature,not_before,expires_at,supersedes_plan_version,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		planID, agentID, kind, nullableText(sourceJobID), version, request.SchemaVersion, apiVersion, mustJSON(required), string(canonical), digest, s.agentPlanSigner.KeyID, signature, nullableTimeValue(notBefore), expiresAt, desired, nullableText(actor))
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE agents SET desired_plan_version=?,row_version=row_version+1,updated_by=? WHERE id=? AND desired_plan_version=?`, version, nullableText(actor), agentID, desired)
	if err != nil {
		return agentPlanCreateResult{}, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return agentPlanCreateResult{}, errVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return agentPlanCreateResult{}, err
	}
	return agentPlanCreateResult{Plan: agentPlanRecord{
		ID: planID, AgentID: agentID, Kind: kind, SourceJobID: sourceJobID, APIVersion: apiVersion,
		PlanVersion: version, SchemaVersion: request.SchemaVersion, Payload: canonical,
		PayloadSHA256: digest, SigningKeyID: s.agentPlanSigner.KeyID, Signature: signature,
		NotBefore: sql.NullTime{Time: notBefore, Valid: !notBefore.IsZero()}, ExpiresAt: expiresAt,
		SupersedesPlanVersion: desired, CreatedBy: sql.NullString{String: actor, Valid: actor != ""}, CreatedAt: now,
	}, Envelope: envelope}, nil
}

func (s *Server) agentPlanValidity(request createAgentPlanRequest, now time.Time) (time.Time, time.Time, error) {
	var notBefore time.Time
	var expiresAt time.Time
	var err error
	if strings.TrimSpace(request.NotBefore) != "" {
		notBefore, err = time.Parse(time.RFC3339Nano, request.NotBefore)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("not_before must be RFC3339")
		}
		notBefore = notBefore.UTC().Truncate(time.Millisecond)
	}
	if strings.TrimSpace(request.ExpiresAt) != "" && request.ExpiresInSeconds != 0 {
		return time.Time{}, time.Time{}, errors.New("expires_at and expires_in_seconds are mutually exclusive")
	}
	if strings.TrimSpace(request.ExpiresAt) != "" {
		expiresAt, err = time.Parse(time.RFC3339Nano, request.ExpiresAt)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("expires_at must be RFC3339")
		}
		expiresAt = expiresAt.UTC().Truncate(time.Millisecond)
	} else {
		ttl := s.cfg.AgentPlans.DefaultTTL
		if request.ExpiresInSeconds != 0 {
			ttl = time.Duration(request.ExpiresInSeconds) * time.Second
		}
		if ttl <= 0 {
			ttl = 365 * 24 * time.Hour
		}
		expiresAt = now.Add(ttl)
	}
	activation := now
	if !notBefore.IsZero() {
		activation = notBefore
	}
	if !expiresAt.After(activation) || expiresAt.After(now.Add(5*365*24*time.Hour)) {
		return time.Time{}, time.Time{}, errors.New("agent plan validity must be positive and no longer than five years")
	}
	return notBefore, expiresAt, nil
}

const agentPlanSelect = `SELECT id,agent_id,kind,COALESCE(source_job_id,''),plan_version,schema_version,api_version,payload_json,payload_sha256,signing_key_id,signature,not_before,expires_at,supersedes_plan_version,created_by,created_at FROM agent_plans`

func scanAgentPlan(row scanner) (agentPlanRecord, error) {
	var plan agentPlanRecord
	err := row.Scan(&plan.ID, &plan.AgentID, &plan.Kind, &plan.SourceJobID, &plan.PlanVersion, &plan.SchemaVersion, &plan.APIVersion, &plan.Payload, &plan.PayloadSHA256, &plan.SigningKeyID, &plan.Signature, &plan.NotBefore, &plan.ExpiresAt, &plan.SupersedesPlanVersion, &plan.CreatedBy, &plan.CreatedAt)
	return plan, err
}

func (s *Server) readAgentPlan(ctx context.Context, agentID string, version uint64) (agentPlanRecord, error) {
	return scanAgentPlan(s.db.QueryRowContext(ctx, agentPlanSelect+` WHERE agent_id=? AND plan_version=?`, agentID, version))
}

func readAgentPlanBySourceJob(ctx context.Context, tx *sql.Tx, agentID, sourceJobID string) (agentPlanRecord, error) {
	return scanAgentPlan(tx.QueryRowContext(ctx, agentPlanSelect+` WHERE agent_id=? AND source_job_id=?`, agentID, sourceJobID))
}

func encodeAgentPlanEnvelope(plan agentPlanRecord) ([]byte, error) {
	canonical, digest, _, err := agentplan.CanonicalSpec(plan.Payload)
	if err != nil {
		return nil, err
	}
	if digest != plan.PayloadSHA256 {
		return nil, errors.New("stored agent plan payload digest mismatch")
	}
	metadata := agentplan.Metadata{
		PlanID: plan.ID, AgentID: plan.AgentID, PlanVersion: plan.PlanVersion, PlanSchemaVersion: plan.SchemaVersion,
		Kind: plan.Kind, APIVersion: plan.APIVersion, PayloadSHA256: plan.PayloadSHA256,
		SigningKeyID: plan.SigningKeyID, ExpiresAtUnixMilli: plan.ExpiresAt.UTC().UnixMilli(),
		SupersedesPlanVersion: plan.SupersedesPlanVersion,
	}
	if plan.NotBefore.Valid {
		metadata.NotBeforeUnixMilli = plan.NotBefore.Time.UTC().UnixMilli()
	}
	return json.Marshal(agentplan.Envelope{
		SchemaVersion: agentplan.SchemaVersion, Metadata: metadata, Payload: canonical,
		Signature: base64.StdEncoding.EncodeToString(plan.Signature),
	})
}

func (s *Server) persistAgentPlanAck(ctx context.Context, agentID string, request agentPlanAckRequest) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var status string
	var desired, acked uint64
	if err := tx.QueryRowContext(ctx, `SELECT status,desired_plan_version,acked_plan_version FROM agents WHERE id=? FOR UPDATE`, agentID).Scan(&status, &desired, &acked); err != nil {
		return false, err
	}
	if status == "revoked" {
		return false, errAgentRevoked
	}
	var digest string
	if err := tx.QueryRowContext(ctx, `SELECT payload_sha256 FROM agent_plans WHERE agent_id=? AND plan_version=?`, agentID, request.PlanVersion).Scan(&digest); err != nil {
		return false, err
	}
	if digest != request.PayloadSHA256 || request.PlanVersion > desired || (request.Status == "applied" && request.PlanVersion < acked) {
		return false, errAgentPlanDowngrade
	}
	var existing agentPlanAckRequest
	err = tx.QueryRowContext(ctx, `SELECT plan_version,payload_sha256,status,boot_id,software_version,error_code,COALESCE(error_text,'') FROM agent_plan_acks WHERE agent_id=? AND plan_version=? AND boot_id=? FOR UPDATE`, agentID, request.PlanVersion, request.BootID).
		Scan(&existing.PlanVersion, &existing.PayloadSHA256, &existing.Status, &existing.BootID, &existing.Software, &existing.ErrorCode, &existing.Error)
	if err == nil {
		if existing != request {
			return false, errAgentPlanAckConflict
		}
		return false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_plan_acks (id,agent_id,plan_version,payload_sha256,status,boot_id,software_version,error_code,error_text) VALUES (?,?,?,?,?,?,?,?,NULLIF(?,''))`, newID(), agentID, request.PlanVersion, request.PayloadSHA256, request.Status, request.BootID, request.Software, request.ErrorCode, request.Error)
	if err != nil {
		return false, err
	}
	if request.Status == "applied" {
		_, err = tx.ExecContext(ctx, `UPDATE agents SET acked_plan_version=GREATEST(acked_plan_version,?),last_seen_at=NOW(3),status=IF(status='registered','active',status),health='ok',last_error=NULL,row_version=row_version+IF(status='registered',1,0) WHERE id=?`, request.PlanVersion, agentID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE agents SET last_seen_at=NOW(3),status=IF(status='registered','active',status),health='error',last_error=?,failure_count=failure_count+1,row_version=row_version+IF(status='registered',1,0) WHERE id=?`, request.Error, agentID)
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func validateAgentPlanAck(request agentPlanAckRequest) error {
	if request.PlanVersion == 0 || !validSHA256(request.PayloadSHA256) || (request.Status != "applied" && request.Status != "rejected") || request.BootID == "" || len(request.BootID) > 64 || !printableASCII(request.BootID) || len(request.Software) > 64 || !printableASCII(request.Software) || len(request.Error) > 1024 || !printableASCIIText(request.Error) {
		return errors.New("agent plan acknowledgement is invalid")
	}
	if request.Status == "applied" && (request.ErrorCode != "" || request.Error != "") {
		return errors.New("an applied acknowledgement cannot contain an error")
	}
	if request.Status == "rejected" && (!validAgentPlanErrorCode(request.ErrorCode) || request.Error == "") {
		return errors.New("a rejected acknowledgement requires an error code and detail")
	}
	return nil
}

func validAgentPlanErrorCode(value string) bool {
	if value == "" || len(value) > 64 || value != strings.ToUpper(value) {
		return false
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func printableASCII(value string) bool {
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func printableASCIIText(value string) bool {
	for _, character := range value {
		if character < 0x20 || character > 0x7e {
			return false
		}
	}
	return true
}

var (
	errAgentPlanIncompatible = errors.New("agent plan is incompatible")
	errAgentPlanDowngrade    = errors.New("agent plan acknowledgement is stale or does not match the desired plan")
	errAgentPlanAckConflict  = errors.New("agent plan acknowledgement replay conflicts with the stored acknowledgement")
)

func writeAgentPlanError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errVersionConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", err.Error())
	case errors.Is(err, errAgentRevoked):
		fail(c, http.StatusConflict, "agent_revoked", err.Error())
	case errors.Is(err, errAgentPlanIncompatible):
		fail(c, http.StatusUnprocessableEntity, "incompatible_plan", err.Error())
	case errors.Is(err, errAgentPlanDowngrade):
		fail(c, http.StatusConflict, "plan_downgrade", err.Error())
	case errors.Is(err, errAgentPlanAckConflict):
		fail(c, http.StatusConflict, "ack_conflict", err.Error())
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "agent or plan not found")
	default:
		writeSQLError(c, err)
	}
}

func agentPlanETag(version uint64, digest string) string {
	return fmt.Sprintf(`"p%d-%s"`, version, digest)
}

func nullableTimeValue(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

type agentPlanRolloutPayload struct {
	AgentIDs             []string        `json:"agent_ids"`
	RequiredCapabilities []string        `json:"required_capabilities"`
	Config               json.RawMessage `json:"config"`
	ExpiresInSeconds     int64           `json:"expires_in_seconds"`
	CreatedBy            string          `json:"created_by"`
	NextIndex            int             `json:"next_index"`
}

func (s *Server) enqueueAgentPlanRollout(c *gin.Context) {
	if !s.requireAgentPlanSigner(c) {
		return
	}
	var request agentPlanRolloutPayload
	if !decodeStrictBody(c, &request) {
		return
	}
	request.CreatedBy = stringValue(principalUserID(c))
	if len(request.AgentIDs) == 0 || len(request.AgentIDs) > 5000 {
		fail(c, http.StatusBadRequest, "invalid_request", "agent_ids must contain 1..5000 agents")
		return
	}
	slices.Sort(request.AgentIDs)
	request.AgentIDs = slices.Compact(request.AgentIDs)
	for _, id := range request.AgentIDs {
		if strings.TrimSpace(id) == "" || len(id) > 26 {
			fail(c, http.StatusBadRequest, "invalid_request", "agent_ids contains an invalid id")
			return
		}
	}
	request.NextIndex = 0
	checkpoint, err := opjob.EncodePayload(agentPlanRolloutSchemaVersion, request)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	digest := sha256.Sum256(checkpoint)
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" || len(idempotencyKey) > 190 {
		fail(c, http.StatusBadRequest, "invalid_request", "Idempotency-Key is required and must not exceed 190 characters")
		return
	}
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType: agentPlanRolloutJobType, IdempotencyKey: idempotencyKey,
		RequestHash: hex.EncodeToString(digest[:]), ProgressTotal: uint64(len(request.AgentIDs)),
		CheckpointJSON: checkpoint, CreatedBy: request.CreatedBy,
	})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Location", "/api/v1/agents/plan-rollouts/"+job.ID)
	c.JSON(http.StatusAccepted, job)
}

func (s *Server) getAgentPlanRollout(c *gin.Context) {
	job, err := s.jobs.Get(c.Request.Context(), c.Param("job_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if job.JobType != agentPlanRolloutJobType {
		fail(c, http.StatusNotFound, "not_found", "agent plan rollout not found")
		return
	}
	c.JSON(http.StatusOK, job)
}

func (s *Server) cancelAgentPlanRollout(c *gin.Context) {
	job, err := s.jobs.Get(c.Request.Context(), c.Param("job_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if job.JobType != agentPlanRolloutJobType {
		fail(c, http.StatusNotFound, "not_found", "agent plan rollout not found")
		return
	}
	if err := s.jobs.RequestCancel(c.Request.Context(), job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"cancel_requested": true})
}

func (s *Server) agentPlanRolloutHandler() opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload agentPlanRolloutPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, agentPlanRolloutSchemaVersion, &payload); err != nil {
			return "", err
		}
		if payload.NextIndex < 0 || payload.NextIndex > len(payload.AgentIDs) {
			return "", opjob.TerminalError(errors.New("agent plan rollout checkpoint is invalid"))
		}
		for index := payload.NextIndex; index < len(payload.AgentIDs); index++ {
			_, err := s.createAgentPlanRecord(ctx, payload.AgentIDs[index], createAgentPlanRequest{
				SchemaVersion: agentplan.SchemaVersion, RequiredCapabilities: payload.RequiredCapabilities,
				Config: payload.Config, ExpiresInSeconds: payload.ExpiresInSeconds,
			}, payload.CreatedBy, job.ID)
			if errors.Is(err, errAgentPlanIncompatible) || errors.Is(err, errAgentRevoked) || errors.Is(err, sql.ErrNoRows) {
				return "", opjob.TerminalError(fmt.Errorf("agent %s: %w", payload.AgentIDs[index], err))
			}
			if err != nil {
				return "", err
			}
			payload.NextIndex = index + 1
			checkpoint, err := opjob.EncodePayload(agentPlanRolloutSchemaVersion, payload)
			if err != nil {
				return "", opjob.TerminalError(err)
			}
			if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
				if err := reporter.Report(ctx, uint64(payload.NextIndex), checkpoint); err != nil {
					return "", err
				}
			}
		}
		return fmt.Sprintf("agents:%d", len(payload.AgentIDs)), nil
	}
}
