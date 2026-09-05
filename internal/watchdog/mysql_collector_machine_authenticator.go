package watchdog

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

type MySQLCollectorMachineAuthenticator struct {
	db *sql.DB
}

func NewMySQLCollectorMachineAuthenticator(db *sql.DB) (*MySQLCollectorMachineAuthenticator, error) {
	if db == nil {
		return nil, errors.New("collector machine authentication database is required")
	}
	return &MySQLCollectorMachineAuthenticator{db: db}, nil
}

func (a *MySQLCollectorMachineAuthenticator) AuthenticateCollector(ctx context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorMachineIdentity, error) {
	if a == nil || a.db == nil || ctx == nil || collectorID == "" || len(collectorID) > 26 || !validCollectorMachineCredential(credential) {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	var identity CollectorMachineIdentity
	var agentType, moduleKey, authType, tokenHash, certificateFingerprint, status string
	var pendingTokenHash, pendingFingerprint string
	var pendingExpires sql.NullTime
	err := a.db.QueryRowContext(ctx, `
		SELECT tenant_id, id, boot_id, agent_type, module_key, auth_type,
			COALESCE(token_hash, ''), COALESCE(certificate_fingerprint, ''), status,
			COALESCE(pending_token_hash, ''), COALESCE(pending_certificate_fingerprint, ''),
			pending_credential_expires_at
		FROM collector_agents
		WHERE id = ? AND deleted_at IS NULL
	`, collectorID).Scan(
		&identity.TenantID, &identity.CollectorID, &identity.BootID,
		&agentType, &moduleKey, &authType, &tokenHash, &certificateFingerprint, &status,
		&pendingTokenHash, &pendingFingerprint, &pendingExpires,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	if err != nil {
		return CollectorMachineIdentity{}, err
	}
	if identity.CollectorID != collectorID || identity.TenantID == "" || agentType != "flow_collect" || moduleKey != "flow" || status != "active" {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	// PLAT-03C2 dual window: an unexpired staged credential authenticates
	// alongside the active one until the rotation is committed or aborted.
	pendingValid := pendingExpires.Valid && pendingExpires.Time.After(time.Now().UTC())
	switch authType {
	case "token":
		if credential.Token == "" || credential.CertificateFingerprint != "" {
			return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
		}
		if !AgentTokenMatches(credential.Token, tokenHash) &&
			!(pendingValid && pendingTokenHash != "" && AgentTokenMatches(credential.Token, pendingTokenHash)) {
			return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
		}
	case "mtls":
		if credential.Token != "" || credential.CertificateFingerprint == "" {
			return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
		}
		activeMatch := subtle.ConstantTimeCompare([]byte(credential.CertificateFingerprint), []byte(certificateFingerprint)) == 1
		pendingMatch := pendingValid && pendingFingerprint != "" &&
			subtle.ConstantTimeCompare([]byte(credential.CertificateFingerprint), []byte(pendingFingerprint)) == 1
		if !activeMatch && !pendingMatch {
			return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
		}
	default:
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	return identity, nil
}

func validCollectorMachineCredential(credential CollectorMachineCredential) bool {
	if (credential.Token == "") == (credential.CertificateFingerprint == "") {
		return false
	}
	if credential.Token != "" {
		return len(credential.Token) <= 256
	}
	return validCollectorCertificateFingerprint(credential.CertificateFingerprint)
}

func validCollectorCertificateFingerprint(value string) bool {
	const prefix = "sha256:"
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value[len(prefix):])
	return err == nil && len(decoded) == 32
}

var _ CollectorMachineAuthenticator = (*MySQLCollectorMachineAuthenticator)(nil)
