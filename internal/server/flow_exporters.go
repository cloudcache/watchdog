package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/gin-gonic/gin"
)

type flowExporterRecord struct {
	ID, DeviceID, DeviceHost, DeviceName                  string
	CollectorAgentID, CollectorName, ObservationDomain    sql.NullString
	SourcePrefix, Protocol, SamplingMode                  string
	DefaultSamplingRate, OwnershipEpoch                   uint64
	PublishedRowVersion, PublishedPlanVersion, RowVersion uint64
	SamplingRules, Observations                           json.RawMessage
	Enabled                                               bool
	CreatedAt, UpdatedAt                                  time.Time
}

type flowExporterDTO struct {
	ID                   string                          `json:"id"`
	DeviceID             string                          `json:"device_id"`
	DeviceHost           string                          `json:"device_host"`
	DeviceName           string                          `json:"device_name"`
	CollectorAgentID     string                          `json:"collector_agent_id,omitempty"`
	CollectorName        string                          `json:"collector_name,omitempty"`
	SourcePrefix         string                          `json:"source_prefix"`
	Protocol             string                          `json:"protocol"`
	ObservationDomainID  *uint64                         `json:"observation_domain_id"`
	SamplingMode         string                          `json:"sampling_mode"`
	DefaultSamplingRate  uint64                          `json:"default_sampling_rate"`
	SamplingRules        []flowplan.SamplingRule         `json:"sampling_rules"`
	Observations         map[uint32]flowplan.Observation `json:"observations"`
	Enabled              bool                            `json:"enabled"`
	OwnershipEpoch       uint64                          `json:"ownership_epoch"`
	PublishedRowVersion  uint64                          `json:"published_row_version"`
	PublishedPlanVersion uint64                          `json:"published_plan_version"`
	DeploymentState      string                          `json:"deployment_state"`
	RowVersion           uint64                          `json:"row_version"`
	CreatedAt            string                          `json:"created_at"`
	UpdatedAt            string                          `json:"updated_at"`
}

func (r flowExporterRecord) dto() flowExporterDTO {
	var domain *uint64
	if r.ObservationDomain.Valid {
		if value, err := strconv.ParseUint(r.ObservationDomain.String, 10, 64); err == nil {
			domain = &value
		}
	}
	rules := []flowplan.SamplingRule{}
	observations := map[uint32]flowplan.Observation{}
	_ = json.Unmarshal(r.SamplingRules, &rules)
	_ = json.Unmarshal(r.Observations, &observations)
	return flowExporterDTO{
		ID: r.ID, DeviceID: r.DeviceID, DeviceHost: r.DeviceHost, DeviceName: r.DeviceName,
		CollectorAgentID: r.CollectorAgentID.String, CollectorName: r.CollectorName.String,
		SourcePrefix: r.SourcePrefix, Protocol: r.Protocol, ObservationDomainID: domain,
		SamplingMode: r.SamplingMode, DefaultSamplingRate: r.DefaultSamplingRate,
		SamplingRules: rules, Observations: observations, Enabled: r.Enabled,
		OwnershipEpoch: r.OwnershipEpoch, PublishedRowVersion: r.PublishedRowVersion,
		PublishedPlanVersion: r.PublishedPlanVersion, DeploymentState: r.deploymentState(),
		RowVersion: r.RowVersion, CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (r flowExporterRecord) deploymentState() string {
	if r.PublishedRowVersion == 0 {
		return "unpublished"
	}
	if r.PublishedRowVersion != r.RowVersion {
		return "pending_publish"
	}
	if r.Enabled {
		return "active"
	}
	return "inactive"
}

type optionalUint64 struct {
	Set   bool
	Value *uint64
}

func (v *optionalUint64) UnmarshalJSON(data []byte) error {
	v.Set = true
	if string(data) == "null" {
		v.Value = nil
		return nil
	}
	var value uint64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	v.Value = &value
	return nil
}

type optionalString struct {
	Set   bool
	Value *string
}

func (v *optionalString) UnmarshalJSON(data []byte) error {
	v.Set = true
	if string(data) == "null" {
		v.Value = nil
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	v.Value = &value
	return nil
}

type flowExporterMutation struct {
	ID                  string          `json:"id"`
	DeviceID            *string         `json:"device_id"`
	CollectorAgentID    optionalString  `json:"collector_agent_id"`
	SourcePrefix        *string         `json:"source_prefix"`
	Protocol            *string         `json:"protocol"`
	ObservationDomainID optionalUint64  `json:"observation_domain_id"`
	SamplingMode        *string         `json:"sampling_mode"`
	DefaultSamplingRate *uint64         `json:"default_sampling_rate"`
	SamplingRules       json.RawMessage `json:"sampling_rules"`
	Observations        json.RawMessage `json:"observations"`
	Enabled             *bool           `json:"enabled"`
	OwnershipEpoch      *uint64         `json:"ownership_epoch"`
}

type flowExporterValues struct {
	DeviceID, CollectorAgentID, SourcePrefix, Protocol, SamplingMode string
	ObservationDomainID                                              *uint64
	DefaultSamplingRate, OwnershipEpoch                              uint64
	SamplingRules, Observations                                      string
	Enabled                                                          bool
}

func (s *Server) listFlowExporters(c *gin.Context) {
	limit, offset := pageParams(c)
	where := []string{"1=1"}
	args := []any{}
	if p := currentPrincipal(c); p != nil && !p.can("device.viewAll") {
		where = append(where, `(EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=f.device_id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=f.device_id))`)
		args = append(args, p.UserID, p.UserID)
	}
	for _, filter := range []struct {
		param, column string
	}{
		{"device_id", "f.device_id"}, {"collector_agent_id", "f.collector_agent_id"},
	} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			if len(value) > 26 {
				fail(c, http.StatusBadRequest, "invalid_filter", filter.param+" must not exceed 26 characters")
				return
			}
			where = append(where, filter.column+"=?")
			args = append(args, value)
		}
	}
	if protocol := strings.TrimSpace(c.Query("protocol")); protocol != "" {
		if _, err := flowProtocol(protocol); err != nil {
			fail(c, http.StatusBadRequest, "invalid_filter", err.Error())
			return
		}
		where = append(where, "f.protocol=?")
		args = append(args, strings.ToLower(protocol))
	}
	if mode := strings.TrimSpace(c.Query("sampling_mode")); mode != "" {
		mode = strings.ToLower(mode)
		if mode != "sampled" && mode != "pre_scaled" {
			fail(c, http.StatusBadRequest, "invalid_filter", "sampling_mode must be sampled or pre_scaled")
			return
		}
		where = append(where, "f.sampling_mode=?")
		args = append(args, mode)
	}
	if raw := strings.TrimSpace(c.Query("enabled")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_filter", "enabled must be true or false")
			return
		}
		where = append(where, "f.enabled=?")
		args = append(args, enabled)
	}
	if state := strings.TrimSpace(c.Query("deployment_state")); state != "" {
		switch state {
		case "unpublished":
			where = append(where, "f.published_row_version=0")
		case "pending_publish":
			where = append(where, "f.published_row_version>0 AND f.published_row_version<>f.row_version")
		case "active":
			where = append(where, "f.published_row_version=f.row_version AND f.enabled=1")
		case "inactive":
			where = append(where, "f.published_row_version=f.row_version AND f.enabled=0")
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "invalid deployment_state")
			return
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(f.source_prefix LIKE ? OR d.host LIKE ? OR d.display_name LIKE ? OR d.sys_name LIKE ? OR a.name LIKE ?)")
		for range 5 {
			args = append(args, like)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_exporter_bindings f JOIN devices d ON d.id=f.device_id LEFT JOIN agents a ON a.id=f.collector_agent_id`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	sorts := map[string]string{
		"": "f.updated_at", "id": "f.id", "device": "COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host)",
		"device_host": "d.host", "source_prefix": "f.source_prefix", "protocol": "f.protocol",
		"collector": "a.name", "sampling_mode": "f.sampling_mode", "enabled": "f.enabled", "updated_at": "f.updated_at",
	}
	sortColumn := sorts[c.Query("sort")]
	if sortColumn == "" {
		sortColumn = sorts[""]
	}
	query := flowExporterSelect + clause + fmt.Sprintf(" ORDER BY %s %s, f.id %s LIMIT ? OFFSET ?", sortColumn, sortDirection(c), sortDirection(c))
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowExporterDTO, 0, limit)
	for rows.Next() {
		record, err := scanFlowExporter(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, record.dto())
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

const flowExporterSelect = `SELECT f.id,f.device_id,d.host,COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host),
	f.collector_agent_id,a.name,f.source_prefix,f.protocol,CAST(f.observation_domain_id AS CHAR),f.sampling_mode,
	f.default_sampling_rate,f.sampling_rules_json,f.observations_json,f.enabled,f.ownership_epoch,
	f.published_row_version,f.published_plan_version,f.row_version,f.created_at,f.updated_at
	FROM flow_exporter_bindings f JOIN devices d ON d.id=f.device_id LEFT JOIN agents a ON a.id=f.collector_agent_id`

type rowScanner interface{ Scan(...any) error }

func scanFlowExporter(row rowScanner) (flowExporterRecord, error) {
	var r flowExporterRecord
	err := row.Scan(&r.ID, &r.DeviceID, &r.DeviceHost, &r.DeviceName, &r.CollectorAgentID, &r.CollectorName,
		&r.SourcePrefix, &r.Protocol, &r.ObservationDomain, &r.SamplingMode, &r.DefaultSamplingRate,
		&r.SamplingRules, &r.Observations, &r.Enabled, &r.OwnershipEpoch, &r.PublishedRowVersion,
		&r.PublishedPlanVersion, &r.RowVersion, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (s *Server) readFlowExporter(c *gin.Context, id string) (flowExporterRecord, error) {
	return scanFlowExporter(s.db.QueryRowContext(c.Request.Context(), flowExporterSelect+" WHERE f.id=?", id))
}

func (s *Server) getFlowExporter(c *gin.Context) {
	record, err := s.readFlowExporter(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireDeviceAccess(c, record.DeviceID) {
		return
	}
	c.Header("ETag", etag(record.RowVersion))
	c.JSON(http.StatusOK, record.dto())
}

func (s *Server) createFlowExporter(c *gin.Context) {
	var req flowExporterMutation
	if err := decodeFlowExporterMutation(c, &req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	values, err := newFlowExporterValues(req)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !s.requireDeviceAccess(c, values.DeviceID) {
		return
	}
	if err := s.validateFlowCollector(c, values.CollectorAgentID); err != nil {
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
	_, err = s.db.ExecContext(c.Request.Context(), `INSERT INTO flow_exporter_bindings
		(id,device_id,collector_agent_id,source_prefix,protocol,observation_domain_id,observation_domain_key,
		sampling_mode,default_sampling_rate,sampling_rules_json,observations_json,enabled,ownership_epoch,created_by,updated_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, values.DeviceID, nullableTextValue(values.CollectorAgentID),
		values.SourcePrefix, values.Protocol, values.ObservationDomainID, observationDomainKey(values.ObservationDomainID),
		values.SamplingMode, values.DefaultSamplingRate, values.SamplingRules, values.Observations, values.Enabled,
		values.OwnershipEpoch, principalUserID(c), principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	record, err := s.readFlowExporter(c, id)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.exporter.create", "flow_exporter_binding", id)
	c.Header("ETag", etag(record.RowVersion))
	c.Header("Location", "/api/v1/flow/exporter-bindings/"+id)
	c.JSON(http.StatusCreated, record.dto())
}

func (s *Server) updateFlowExporter(c *gin.Context) {
	current, err := s.readFlowExporter(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireDeviceAccess(c, current.DeviceID) {
		return
	}
	var req flowExporterMutation
	if err := decodeFlowExporterMutation(c, &req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != current.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	values, err := patchFlowExporterValues(req, current)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if values.DeviceID != current.DeviceID && !s.requireDeviceAccess(c, values.DeviceID) {
		return
	}
	if err := s.validateFlowCollector(c, values.CollectorAgentID); err != nil {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE flow_exporter_bindings SET
		device_id=?,collector_agent_id=?,source_prefix=?,protocol=?,observation_domain_id=?,observation_domain_key=?,
		sampling_mode=?,default_sampling_rate=?,sampling_rules_json=?,observations_json=?,enabled=?,ownership_epoch=?,
		updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`, values.DeviceID,
		nullableTextValue(values.CollectorAgentID), values.SourcePrefix, values.Protocol, values.ObservationDomainID,
		observationDomainKey(values.ObservationDomainID), values.SamplingMode, values.DefaultSamplingRate,
		values.SamplingRules, values.Observations, values.Enabled, values.OwnershipEpoch, principalUserID(c),
		current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	record, err := s.readFlowExporter(c, current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.exporter.update", "flow_exporter_binding", current.ID)
	c.Header("ETag", etag(record.RowVersion))
	c.JSON(http.StatusOK, record.dto())
}

func (s *Server) deleteFlowExporter(c *gin.Context) {
	current, err := s.readFlowExporter(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !s.requireDeviceAccess(c, current.DeviceID) {
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != current.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	if current.PublishedPlanVersion > 0 && (current.Enabled || current.PublishedRowVersion != current.RowVersion) {
		fail(c, http.StatusConflict, "binding_still_published", "disable and publish the binding removal before deleting it")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM flow_exporter_bindings WHERE id=? AND row_version=?`, current.ID, current.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.exporter.delete", "flow_exporter_binding", current.ID)
	c.Status(http.StatusNoContent)
}

func decodeFlowExporterMutation(c *gin.Context, out *flowExporterMutation) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return errors.New("invalid JSON body: " + err.Error())
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("body must contain one JSON object")
	}
	return nil
}

func newFlowExporterValues(req flowExporterMutation) (flowExporterValues, error) {
	values := flowExporterValues{
		DeviceID: valueOr(req.DeviceID, ""), CollectorAgentID: optionalStringValue(req.CollectorAgentID, ""),
		SourcePrefix: valueOr(req.SourcePrefix, ""), Protocol: valueOr(req.Protocol, ""),
		SamplingMode: valueOr(req.SamplingMode, "sampled"), DefaultSamplingRate: valueOrUint64(req.DefaultSamplingRate, 0),
		Enabled: valueOrBool(req.Enabled, true), OwnershipEpoch: valueOrUint64(req.OwnershipEpoch, 1),
		ObservationDomainID: req.ObservationDomainID.Value, SamplingRules: rawOrDefault(req.SamplingRules, `[]`),
		Observations: rawOrDefault(req.Observations, `{}`),
	}
	return validateFlowExporterValues(values)
}

func patchFlowExporterValues(req flowExporterMutation, current flowExporterRecord) (flowExporterValues, error) {
	domain := nullableUint64Value(current.ObservationDomain)
	if req.ObservationDomainID.Set {
		domain = req.ObservationDomainID.Value
	}
	values := flowExporterValues{
		DeviceID:         patchString(req.DeviceID, current.DeviceID),
		CollectorAgentID: optionalStringValue(req.CollectorAgentID, current.CollectorAgentID.String),
		SourcePrefix:     patchString(req.SourcePrefix, current.SourcePrefix), Protocol: patchString(req.Protocol, current.Protocol),
		ObservationDomainID: domain, SamplingMode: patchString(req.SamplingMode, current.SamplingMode),
		DefaultSamplingRate: valueOrUint64(req.DefaultSamplingRate, current.DefaultSamplingRate),
		SamplingRules:       rawOrDefault(req.SamplingRules, string(current.SamplingRules)),
		Observations:        rawOrDefault(req.Observations, string(current.Observations)),
		Enabled:             valueOrBool(req.Enabled, current.Enabled), OwnershipEpoch: valueOrUint64(req.OwnershipEpoch, current.OwnershipEpoch),
	}
	return validateFlowExporterValues(values)
}

func validateFlowExporterValues(values flowExporterValues) (flowExporterValues, error) {
	if values.DeviceID == "" || len(values.DeviceID) > 26 {
		return values, errors.New("device_id is required and must not exceed 26 characters")
	}
	prefix, err := canonicalFlowSourcePrefix(values.SourcePrefix)
	if err != nil {
		return values, err
	}
	values.SourcePrefix = prefix
	protocol, err := flowProtocol(values.Protocol)
	if err != nil {
		return values, err
	}
	values.Protocol = strings.ToLower(strings.TrimSpace(values.Protocol))
	values.SamplingMode = strings.ToLower(strings.TrimSpace(values.SamplingMode))
	mode := flowplan.SamplingModeSampled
	if values.SamplingMode == "pre_scaled" {
		mode = flowplan.SamplingModePreScaled
	} else if values.SamplingMode != "sampled" {
		return values, errors.New("sampling_mode must be sampled or pre_scaled")
	}
	if values.OwnershipEpoch == 0 {
		return values, errors.New("ownership_epoch must be greater than zero")
	}
	var rules []flowplan.SamplingRule
	if len(values.SamplingRules) > 256*1024 || json.Unmarshal([]byte(values.SamplingRules), &rules) != nil || len(rules) > 4096 {
		return values, errors.New("sampling_rules must be an array of at most 4096 rules")
	}
	var observations map[uint32]flowplan.Observation
	if len(values.Observations) > 256*1024 || json.Unmarshal([]byte(values.Observations), &observations) != nil || len(observations) > 4096 {
		return values, errors.New("observations must be an object of at most 4096 interface mappings")
	}
	if observations == nil {
		observations = map[uint32]flowplan.Observation{}
	}
	now := time.Now().UTC()
	_, err = flowplan.CompilePlan(flowplan.Plan{
		SchemaVersion: 2, Revision: 1, CollectorID: "validation", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute),
		Sources: []flowplan.SourceBinding{{
			Protocol: protocol, SourcePrefix: values.SourcePrefix, ObservationDomainID: values.ObservationDomainID,
			TenantID: "global", ExporterID: "validation", TargetID: values.DeviceID, DeviceID: values.DeviceID,
			OwnershipEpoch: values.OwnershipEpoch, SamplingMode: mode, DefaultSamplingRate: values.DefaultSamplingRate,
			SamplingRules: rules, Observations: observations, Enabled: values.Enabled,
		}},
	}, now)
	if err != nil {
		return values, err
	}
	rulesJSON, _ := json.Marshal(rules)
	observationsJSON, _ := json.Marshal(observations)
	values.SamplingRules, values.Observations = string(rulesJSON), string(observationsJSON)
	return values, nil
}

func canonicalFlowSourcePrefix(value string) (string, error) {
	value = strings.TrimSpace(value)
	if addr, err := netip.ParseAddr(strings.Trim(value, "[]")); err == nil {
		return netip.PrefixFrom(addr, addr.BitLen()).String(), nil
	}
	prefix, err := netip.ParsePrefix(value)
	if err != nil {
		return "", errors.New("source_prefix must be an IPv4/IPv6 address or CIDR")
	}
	return prefix.Masked().String(), nil
}

func flowProtocol(value string) (flowplan.Protocol, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "sflow5":
		return flowplan.ProtocolSFlow5, nil
	case "netflow5":
		return flowplan.ProtocolNetFlow5, nil
	case "netflow9":
		return flowplan.ProtocolNetFlow9, nil
	case "ipfix":
		return flowplan.ProtocolIPFIX, nil
	default:
		return 0, errors.New("protocol must be sflow5, netflow5, netflow9, or ipfix")
	}
}

func (s *Server) validateFlowCollector(c *gin.Context, collectorID string) error {
	if collectorID == "" {
		return nil
	}
	if len(collectorID) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "collector_agent_id must not exceed 26 characters")
		return errors.New("invalid collector")
	}
	var kind, status string
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT kind,status FROM agents WHERE id=?`, collectorID).Scan(&kind, &status)
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusBadRequest, "invalid_request", "collector_agent_id does not exist")
		return err
	}
	if err != nil {
		writeSQLError(c, err)
		return err
	}
	if kind != "flow_collect" || status == "disabled" {
		fail(c, http.StatusBadRequest, "invalid_request", "collector_agent_id must reference an enabled flow_collect agent")
		return errors.New("invalid collector")
	}
	return nil
}

func optionalStringValue(value optionalString, fallback string) string {
	if !value.Set {
		return strings.TrimSpace(fallback)
	}
	if value.Value == nil {
		return ""
	}
	return strings.TrimSpace(*value.Value)
}

func valueOrUint64(value *uint64, fallback uint64) uint64 {
	if value == nil {
		return fallback
	}
	return *value
}

func rawOrDefault(value json.RawMessage, fallback string) string {
	if len(value) == 0 {
		return fallback
	}
	return string(value)
}

func nullableUint64Value(value sql.NullString) *uint64 {
	if !value.Valid {
		return nil
	}
	parsed, err := strconv.ParseUint(value.String, 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func observationDomainKey(value *uint64) string {
	if value == nil {
		return "*"
	}
	return strconv.FormatUint(*value, 10)
}

func nullableTextValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}
