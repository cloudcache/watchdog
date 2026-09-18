package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type snmpProfileRecord struct {
	ID, Name, Version string
	Security          json.RawMessage
	Port              uint16
	TimeoutMS         uint32
	Retries           uint8
	RowVersion        uint64
}

type snmpProfileDTO struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Version    string            `json:"version"`
	Security   map[string]string `json:"security,omitempty"`
	Port       uint16            `json:"port"`
	Timeout    uint64            `json:"timeout"`
	TimeoutMS  uint32            `json:"timeout_ms"`
	Retries    uint8             `json:"retries"`
	RowVersion uint64            `json:"row_version"`
}

func (r snmpProfileRecord) dto(includeSecurity bool) snmpProfileDTO {
	d := snmpProfileDTO{
		ID: r.ID, Name: r.Name, Version: r.Version, Port: r.Port,
		Timeout: uint64(r.TimeoutMS) * 1_000_000, TimeoutMS: r.TimeoutMS,
		Retries: r.Retries, RowVersion: r.RowVersion,
	}
	if includeSecurity {
		d.Security = map[string]string{}
		_ = json.Unmarshal(r.Security, &d.Security)
	}
	return d
}

type snmpProfileMutation struct {
	ID        string            `json:"id"`
	Name      *string           `json:"name"`
	Version   *string           `json:"version"`
	Security  map[string]string `json:"security"`
	Port      *uint16           `json:"port"`
	Timeout   *uint64           `json:"timeout"`
	TimeoutMS *uint32           `json:"timeout_ms"`
	Retries   *uint8            `json:"retries"`
}

func (s *Server) listSNMPProfiles(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,name,version,security_json,port,timeout_ms,retries,row_version FROM snmp_profiles ORDER BY name,id`)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []snmpProfileDTO{}
	for rows.Next() {
		r, err := scanSNMPProfile(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, r.dto(false))
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
}

func scanSNMPProfile(row scanner) (snmpProfileRecord, error) {
	var r snmpProfileRecord
	err := row.Scan(&r.ID, &r.Name, &r.Version, &r.Security, &r.Port, &r.TimeoutMS, &r.Retries, &r.RowVersion)
	return r, err
}

func (s *Server) readSNMPProfile(c *gin.Context, id string) (snmpProfileRecord, error) {
	return s.readSNMPProfileByID(c.Request.Context(), id)
}

// readSNMPProfileByID is the context-based profile read used by non-HTTP callers
// such as the SNMP discovery reconcile loop.
func (s *Server) readSNMPProfileByID(ctx context.Context, id string) (snmpProfileRecord, error) {
	return scanSNMPProfile(s.db.QueryRowContext(ctx, `SELECT id,name,version,security_json,port,timeout_ms,retries,row_version FROM snmp_profiles WHERE id=?`, id))
}

func (s *Server) getSNMPProfile(c *gin.Context) {
	r, err := s.readSNMPProfile(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto(true))
}

func (s *Server) createSNMPProfile(c *gin.Context) {
	var req snmpProfileMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
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
	name, version := valueOr(req.Name, ""), valueOr(req.Version, "2c")
	if !validSNMPTimeout(req) {
		fail(c, http.StatusBadRequest, "invalid_request", "timeout is outside the supported range")
		return
	}
	port, timeoutMS, retries := snmpProfileValues(req, 161, 3000, 2)
	if name == "" || !validSNMPVersion(version) || port == 0 || timeoutMS == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "name, valid version, port, and timeout are required")
		return
	}
	security := req.Security
	if security == nil {
		security = map[string]string{}
	}
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO snmp_profiles (id,name,version,port,security_json,timeout_ms,retries,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?)`, id, name, version, port, mustJSON(security), timeoutMS, retries, principalUserID(c), principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	r, err := s.readSNMPProfile(c, id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.Header("Location", "/api/v1/snmp/profiles/"+id)
	c.JSON(http.StatusCreated, r.dto(true))
}

func (s *Server) updateSNMPProfile(c *gin.Context) {
	var req snmpProfileMutation
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	old, err := s.readSNMPProfile(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != old.RowVersion {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "SNMP profile changed since it was loaded")
		return
	}
	name, version := patchString(req.Name, old.Name), patchString(req.Version, old.Version)
	if !validSNMPTimeout(req) {
		fail(c, http.StatusBadRequest, "invalid_request", "timeout is outside the supported range")
		return
	}
	port, timeoutMS, retries := snmpProfileValues(req, old.Port, old.TimeoutMS, old.Retries)
	if name == "" || !validSNMPVersion(version) || port == 0 || timeoutMS == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "name, valid version, port, and timeout are required")
		return
	}
	security := string(old.Security)
	if req.Security != nil {
		security = mustJSON(req.Security)
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE snmp_profiles SET name=?,version=?,port=?,security_json=?,timeout_ms=?,retries=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`, name, version, port, security, timeoutMS, retries, principalUserID(c), old.ID, old.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	r, err := s.readSNMPProfile(c, old.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(r.RowVersion))
	c.JSON(http.StatusOK, r.dto(true))
}

func (s *Server) deleteSNMPProfile(c *gin.Context) {
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM snmp_profiles WHERE id=?`, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed == 0 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	c.Status(http.StatusNoContent)
}

func snmpProfileValues(req snmpProfileMutation, defaultPort uint16, defaultTimeoutMS uint32, defaultRetries uint8) (uint16, uint32, uint8) {
	port, timeoutMS, retries := defaultPort, defaultTimeoutMS, defaultRetries
	if req.Port != nil {
		port = *req.Port
	}
	if req.TimeoutMS != nil {
		timeoutMS = *req.TimeoutMS
	} else if req.Timeout != nil {
		timeoutMS = uint32(*req.Timeout / 1_000_000)
	}
	if req.Retries != nil {
		retries = *req.Retries
	}
	return port, timeoutMS, retries
}

func validSNMPVersion(version string) bool {
	return version == "1" || version == "2c" || version == "3"
}

func validSNMPTimeout(req snmpProfileMutation) bool {
	if req.TimeoutMS != nil {
		return *req.TimeoutMS > 0
	}
	if req.Timeout == nil {
		return true
	}
	milliseconds := *req.Timeout / 1_000_000
	return milliseconds > 0 && milliseconds <= math.MaxUint32
}
