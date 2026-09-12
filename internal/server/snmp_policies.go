package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

func (s *Server) getPortPolicy(c *gin.Context) {
	port, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, port.DeviceID, port.ID); !ok {
		return
	}
	policy, err := s.readPortPolicy(c, port.ID)
	if errors.Is(err, sql.ErrNoRows) {
		defaults, defaultsErr := s.readTrafficPolicyDefaults(c)
		if defaultsErr != nil {
			writeSQLError(c, defaultsErr)
			return
		}
		side := watchdog.PortSideCustomer
		metadata := map[string]any{}
		_ = json.Unmarshal(port.Metadata, &metadata)
		if metadata["side_type"] == string(watchdog.PortSideProvider) {
			side = watchdog.PortSideProvider
		}
		policy = watchdog.DefaultPortPolicyWithDefaults("", watchdog.ID(port.ID), side, defaults)
		policy.ID = watchdog.ID(stableManagementID("port-policy", port.ID))
		err = nil
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, policy)
}

func (s *Server) patchPortPolicy(c *gin.Context) {
	port, err := s.readPort(c, c.Param("port_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if _, ok := s.portScope(c, port.DeviceID, port.ID); !ok {
		return
	}
	var policy watchdog.PortPolicy
	if err := c.ShouldBindJSON(&policy); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	policy.PortID = watchdog.ID(port.ID)
	policy.TenantID = ""
	if policy.ID == "" {
		policy.ID = watchdog.ID(stableManagementID("port-policy", port.ID))
	}
	policy = policy.Normalize()
	if err := validatePortPolicy(policy); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	_, err = s.db.ExecContext(c.Request.Context(), `INSERT INTO port_policies
		(id,port_id,side_type,billing_base_bps,sample_step_seconds,correction_direction,correction_min,correction_max,enabled)
		VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE
		side_type=VALUES(side_type),billing_base_bps=VALUES(billing_base_bps),sample_step_seconds=VALUES(sample_step_seconds),
		correction_direction=VALUES(correction_direction),correction_min=VALUES(correction_min),correction_max=VALUES(correction_max),
		enabled=VALUES(enabled),row_version=row_version+1`, policy.ID, port.ID, policy.SideType, policy.BillingBaseBps,
		uint16(policy.SampleStep/time.Second), policy.CorrectionDirection, policy.CorrectionMin, policy.CorrectionMax, policy.Enabled)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	updated, err := s.readPortPolicy(c, port.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.port_policy.update", "port", port.ID)
	c.JSON(http.StatusOK, updated)
}

func (s *Server) readPortPolicy(c *gin.Context, portID string) (watchdog.PortPolicy, error) {
	var p watchdog.PortPolicy
	var seconds uint16
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT id,port_id,side_type,billing_base_bps,sample_step_seconds,
		correction_direction,correction_min,correction_max,enabled FROM port_policies WHERE port_id=?`, portID).
		Scan(&p.ID, &p.PortID, &p.SideType, &p.BillingBaseBps, &seconds, &p.CorrectionDirection, &p.CorrectionMin, &p.CorrectionMax, &p.Enabled)
	p.SampleStep = time.Duration(seconds) * time.Second
	return p, err
}

func (s *Server) getTrafficPolicyDefaults(c *gin.Context) {
	defaults, err := s.readTrafficPolicyDefaults(c)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, defaults)
}

func (s *Server) putTrafficPolicyDefaults(c *gin.Context) {
	var defaults watchdog.TrafficPolicyDefaults
	if err := c.ShouldBindJSON(&defaults); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	defaults.Provider = defaults.Provider.Normalize(watchdog.PortSideProvider)
	defaults.Customer = defaults.Customer.Normalize(watchdog.PortSideCustomer)
	defaults.Provider.ID = watchdog.ID("snmp-default-provider")
	defaults.Customer.ID = watchdog.ID("snmp-default-customer")
	if err := validateTrafficPolicyDefault(defaults.Provider); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateTrafficPolicyDefault(defaults.Customer); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	for _, value := range []watchdog.TrafficPolicyDefault{defaults.Provider, defaults.Customer} {
		if _, err = tx.ExecContext(c.Request.Context(), `INSERT INTO traffic_policy_defaults
			(id,side_type,billing_base_bps,sample_step_seconds,correction_direction,correction_min,correction_max)
			VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE billing_base_bps=VALUES(billing_base_bps),
			sample_step_seconds=VALUES(sample_step_seconds),correction_direction=VALUES(correction_direction),
			correction_min=VALUES(correction_min),correction_max=VALUES(correction_max),row_version=row_version+1`,
			value.ID, value.SideType, value.BillingBaseBps, uint16(value.SampleStep/time.Second), value.CorrectionDirection, value.CorrectionMin, value.CorrectionMax); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.traffic_policy_defaults.update", "configuration", "snmp-traffic-policy-defaults")
	s.getTrafficPolicyDefaults(c)
}

func (s *Server) readTrafficPolicyDefaults(c *gin.Context) (watchdog.TrafficPolicyDefaults, error) {
	return s.readTrafficPolicyDefaultsContext(c.Request.Context())
}

func (s *Server) readTrafficPolicyDefaultsContext(ctx context.Context) (watchdog.TrafficPolicyDefaults, error) {
	defaults := watchdog.BuiltinTrafficPolicyDefaults
	if s == nil || s.db == nil {
		return defaults, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,side_type,billing_base_bps,sample_step_seconds,
		correction_direction,correction_min,correction_max FROM traffic_policy_defaults ORDER BY side_type`)
	if err != nil {
		return defaults, err
	}
	defer rows.Close()
	for rows.Next() {
		var value watchdog.TrafficPolicyDefault
		var seconds uint16
		if err := rows.Scan(&value.ID, &value.SideType, &value.BillingBaseBps, &seconds, &value.CorrectionDirection, &value.CorrectionMin, &value.CorrectionMax); err != nil {
			return defaults, err
		}
		value.SampleStep = time.Duration(seconds) * time.Second
		switch value.SideType {
		case watchdog.PortSideProvider:
			defaults.Provider = value.Normalize(watchdog.PortSideProvider)
		case watchdog.PortSideCustomer:
			defaults.Customer = value.Normalize(watchdog.PortSideCustomer)
		}
	}
	return defaults, rows.Err()
}

func (s *Server) readPortPoliciesContext(ctx context.Context, portIDs []string) (map[string]watchdog.PortPolicy, error) {
	result := make(map[string]watchdog.PortPolicy, len(portIDs))
	if len(portIDs) == 0 {
		return result, nil
	}
	defaults, err := s.readTrafficPolicyDefaultsContext(ctx)
	if err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		for _, portID := range portIDs {
			policy := watchdog.DefaultPortPolicyWithDefaults("", watchdog.ID(portID), watchdog.PortSideCustomer, defaults)
			policy.ID = watchdog.ID(stableManagementID("port-policy", portID))
			result[portID] = policy
		}
		return result, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.id,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(p.metadata_json,'$.side_type')),''),
		pp.id,pp.side_type,pp.billing_base_bps,pp.sample_step_seconds,pp.correction_direction,
		pp.correction_min,pp.correction_max,pp.enabled
		FROM ports p LEFT JOIN port_policies pp ON pp.port_id=p.id WHERE p.id IN (`+placeholders(len(portIDs))+`)`, stringsToAny(portIDs)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var portID, metadataSide string
		var policyID, side, direction sql.NullString
		var base, step sql.NullInt64
		var minimum, maximum sql.NullInt64
		var enabled sql.NullBool
		if err := rows.Scan(&portID, &metadataSide, &policyID, &side, &base, &step, &direction, &minimum, &maximum, &enabled); err != nil {
			return nil, err
		}
		defaultSide := watchdog.PortSideCustomer
		if metadataSide == string(watchdog.PortSideProvider) {
			defaultSide = watchdog.PortSideProvider
		}
		policy := watchdog.DefaultPortPolicyWithDefaults("", watchdog.ID(portID), defaultSide, defaults)
		policy.ID = watchdog.ID(stableManagementID("port-policy", portID))
		if policyID.Valid {
			policy.ID = watchdog.ID(policyID.String)
			policy.SideType = watchdog.PortSideType(side.String)
			policy.BillingBaseBps = uint64(base.Int64)
			policy.SampleStep = time.Duration(step.Int64) * time.Second
			policy.CorrectionDirection = watchdog.CorrectionDirection(direction.String)
			policy.CorrectionMin = minimum.Int64
			policy.CorrectionMax = maximum.Int64
			policy.Enabled = enabled.Valid && enabled.Bool
			policy = policy.Normalize()
		}
		result[portID] = policy
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePortPolicy(value watchdog.PortPolicy) error {
	if len(value.ID) > 26 {
		return errors.New("port policy id must not exceed 26 characters")
	}
	if value.SideType != watchdog.PortSideProvider && value.SideType != watchdog.PortSideCustomer {
		return errors.New("side type must be provider or customer")
	}
	return validatePolicyValues(value.BillingBaseBps, value.SampleStep, value.CorrectionDirection, value.CorrectionMin, value.CorrectionMax)
}

func validateTrafficPolicyDefault(value watchdog.TrafficPolicyDefault) error {
	return validatePolicyValues(value.BillingBaseBps, value.SampleStep, value.CorrectionDirection, value.CorrectionMin, value.CorrectionMax)
}

func validatePolicyValues(base uint64, step time.Duration, direction watchdog.CorrectionDirection, minimum, maximum int64) error {
	if base == 0 {
		return errors.New("billing base bps must be positive")
	}
	// MySQL stores this value as BIGINT UNSIGNED, but the bulk reader uses the
	// database/sql nullable signed representation. Reject an unrepresentable
	// management value instead of allowing a later read to fail.
	if base > math.MaxInt64 {
		return errors.New("billing base bps exceeds the supported range")
	}
	if step != time.Minute && step != 5*time.Minute {
		return errors.New("sample step must be 1m or 5m")
	}
	if direction != watchdog.CorrectionNone && direction != watchdog.CorrectionUp && direction != watchdog.CorrectionDown {
		return errors.New("correction direction must be none, up or down")
	}
	if minimum < 0 || maximum < minimum {
		return errors.New("correction range is invalid")
	}
	return nil
}

func stableManagementID(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])[:26]
}
