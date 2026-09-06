package watchdog

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

const emptyCollectorPlanTrustChecksum = "0000000000000000000000000000000000000000000000000000000000000000"

func (s *MySQLStore) ActivateCollectorPlanSigningKey(ctx context.Context, keyID string, publicKey ed25519.PublicKey, activatedAt time.Time) (CollectorPlanTrustBundlePublication, error) {
	if ctx == nil || !validCollectorPlanSigningKeyID(keyID) || len(publicKey) != ed25519.PublicKeySize || activatedAt.IsZero() {
		return CollectorPlanTrustBundlePublication{}, errors.New("collector plan signing key activation is invalid")
	}
	activatedAt = activatedAt.UTC().Truncate(time.Millisecond)
	if err := s.ensureCollectorPlanTrustState(ctx); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	defer tx.Rollback()
	state, err := lockCollectorPlanTrustState(ctx, tx)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	changed, err := expireRetiringCollectorPlanSigningKeysTx(ctx, tx, activatedAt)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}

	desired, desiredErr := getCollectorPlanSigningKeyTx(ctx, tx, keyID, true)
	if desiredErr == nil {
		if desired.Status != CollectorPlanSigningKeyActive || !bytes.Equal(desired.PublicKey, publicKey) {
			return CollectorPlanTrustBundlePublication{}, ErrCollectorPlanSigningKeyReuse
		}
		if !changed && state.Generation > 0 {
			if err := tx.Commit(); err != nil {
				return CollectorPlanTrustBundlePublication{}, err
			}
			return state, nil
		}
	} else if !errors.Is(desiredErr, sql.ErrNoRows) {
		return CollectorPlanTrustBundlePublication{}, desiredErr
	} else {
		active, activeErr := getActiveCollectorPlanSigningKeyTx(ctx, tx, true)
		if activeErr != nil && !errors.Is(activeErr, sql.ErrNoRows) {
			return CollectorPlanTrustBundlePublication{}, activeErr
		}
		if activeErr == nil {
			trustUntil, err := maxStartableCollectorPlanExpiryTx(ctx, tx, active.KeyID, activatedAt)
			if err != nil {
				return CollectorPlanTrustBundlePublication{}, err
			}
			if trustUntil.After(activatedAt) {
				_, err = tx.ExecContext(ctx, `
					UPDATE collector_plan_signing_keys
					SET status = 'retiring', retiring_at = ?, trust_until = ?,
						row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
					WHERE key_id = ? AND status = 'active'
				`, activatedAt, trustUntil, active.KeyID)
			} else {
				_, err = tx.ExecContext(ctx, `
					UPDATE collector_plan_signing_keys
					SET status = 'revoked', retiring_at = ?, trust_until = ?, revoked_at = ?,
						revocation_reason = 'rotated-without-startable-plans',
						row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
					WHERE key_id = ? AND status = 'active'
				`, activatedAt, activatedAt, activatedAt, active.KeyID)
			}
			if err != nil {
				return CollectorPlanTrustBundlePublication{}, err
			}
		}
		digest := sha256.Sum256(publicKey)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO collector_plan_signing_keys (
				key_id, algorithm, public_key, public_key_sha256, status, activated_at
			) VALUES (?, 'ed25519', ?, ?, 'active', ?)
		`, keyID, []byte(publicKey), hex.EncodeToString(digest[:]), activatedAt); err != nil {
			return CollectorPlanTrustBundlePublication{}, mapCollectorPlanSigningKeyWriteError(err)
		}
	}

	publication, err := publishCollectorPlanTrustBundleTx(ctx, tx, state, activatedAt)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	return publication, nil
}

func (s *MySQLStore) GetCollectorPlanSigningKey(ctx context.Context, keyID string) (CollectorPlanSigningKey, error) {
	if ctx == nil || !validCollectorPlanSigningKeyID(keyID) {
		return CollectorPlanSigningKey{}, ErrCollectorPlanSigningKeyUnavailable
	}
	key, err := scanCollectorPlanSigningKey(s.db.QueryRowContext(ctx, `
		SELECT key_id, algorithm, public_key, public_key_sha256, status,
			activated_at, retiring_at, trust_until, revoked_at,
			COALESCE(revocation_reason, ''), row_version, created_at, updated_at
		FROM collector_plan_signing_keys WHERE key_id = ?
	`, keyID))
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorPlanSigningKey{}, ErrCollectorPlanSigningKeyUnavailable
	}
	return key, err
}

func (s *MySQLStore) GetCollectorPlanTrustBundle(ctx context.Context) (CollectorPlanTrustBundlePublication, error) {
	if ctx == nil {
		return CollectorPlanTrustBundlePublication{}, ErrCollectorPlanTrustBundleUnavailable
	}
	publication, err := scanCollectorPlanTrustState(s.db.QueryRowContext(ctx, `
		SELECT generation, bundle_json, checksum_sha256, issued_at, row_version
		FROM collector_plan_trust_state WHERE singleton_id = 1
	`))
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorPlanTrustBundlePublication{}, ErrCollectorPlanTrustBundleUnavailable
	}
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	if publication.Generation == 0 {
		return CollectorPlanTrustBundlePublication{}, ErrCollectorPlanTrustBundleUnavailable
	}
	_, checksum, err := flowplan.ParseTrustBundle(publication.BundleJSON)
	if err != nil || checksum != publication.Checksum {
		return CollectorPlanTrustBundlePublication{}, errors.New("stored collector plan trust bundle integrity check failed")
	}
	return publication, nil
}

func (s *MySQLStore) RevokeCollectorPlanSigningKey(ctx context.Context, keyID, reason string, revokedAt time.Time) (CollectorPlanTrustBundlePublication, error) {
	reason = strings.TrimSpace(reason)
	if ctx == nil || !validCollectorPlanSigningKeyID(keyID) || reason == "" || len(reason) > 512 || !isPrintableASCIIText(reason) || revokedAt.IsZero() {
		return CollectorPlanTrustBundlePublication{}, errors.New("collector plan signing key revocation is invalid")
	}
	revokedAt = revokedAt.UTC().Truncate(time.Millisecond)
	if err := s.ensureCollectorPlanTrustState(ctx); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	defer tx.Rollback()
	state, err := lockCollectorPlanTrustState(ctx, tx)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	key, err := getCollectorPlanSigningKeyTx(ctx, tx, keyID, true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CollectorPlanTrustBundlePublication{}, ErrCollectorPlanSigningKeyUnavailable
		}
		return CollectorPlanTrustBundlePublication{}, err
	}
	if key.Status == CollectorPlanSigningKeyRevoked {
		if err := tx.Commit(); err != nil {
			return CollectorPlanTrustBundlePublication{}, err
		}
		return state, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE collector_plan_signing_keys
		SET status = 'revoked', retiring_at = COALESCE(retiring_at, ?),
			trust_until = COALESCE(trust_until, ?), revoked_at = ?, revocation_reason = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE key_id = ? AND status IN ('active','retiring')
	`, revokedAt, revokedAt, revokedAt, reason, keyID); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	publication, err := publishCollectorPlanTrustBundleTx(ctx, tx, state, revokedAt)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	return publication, nil
}

func (s *MySQLStore) ExpireRetiringCollectorPlanSigningKeys(ctx context.Context, now time.Time) (CollectorPlanTrustBundlePublication, bool, error) {
	if ctx == nil || now.IsZero() {
		return CollectorPlanTrustBundlePublication{}, false, errors.New("collector plan trust expiry time is required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	if err := s.ensureCollectorPlanTrustState(ctx); err != nil {
		return CollectorPlanTrustBundlePublication{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, false, err
	}
	defer tx.Rollback()
	state, err := lockCollectorPlanTrustState(ctx, tx)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, false, err
	}
	changed, err := expireRetiringCollectorPlanSigningKeysTx(ctx, tx, now)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, false, err
	}
	publication := state
	if changed {
		publication, err = publishCollectorPlanTrustBundleTx(ctx, tx, state, now)
		if err != nil {
			return CollectorPlanTrustBundlePublication{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanTrustBundlePublication{}, false, err
	}
	if publication.Generation == 0 {
		return CollectorPlanTrustBundlePublication{}, changed, ErrCollectorPlanTrustBundleUnavailable
	}
	return publication, changed, nil
}

func (s *MySQLStore) ensureCollectorPlanTrustState(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT IGNORE INTO collector_plan_trust_state (
			singleton_id, generation, bundle_json, checksum_sha256, issued_at
		) VALUES (1, 0, JSON_OBJECT(), ?, NULL)
	`, emptyCollectorPlanTrustChecksum)
	return err
}

func lockCollectorPlanTrustState(ctx context.Context, tx *sql.Tx) (CollectorPlanTrustBundlePublication, error) {
	return scanCollectorPlanTrustState(tx.QueryRowContext(ctx, `
		SELECT generation, bundle_json, checksum_sha256, issued_at, row_version
		FROM collector_plan_trust_state WHERE singleton_id = 1 FOR UPDATE
	`))
}

func scanCollectorPlanTrustState(row collectorPlanRow) (CollectorPlanTrustBundlePublication, error) {
	var publication CollectorPlanTrustBundlePublication
	var issuedAt sql.NullTime
	if err := row.Scan(&publication.Generation, &publication.BundleJSON, &publication.Checksum, &issuedAt, &publication.RowVersion); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	publication.BundleJSON = append([]byte(nil), publication.BundleJSON...)
	publication.IssuedAt = issuedAt.Time
	if publication.RowVersion == 0 || (publication.Generation == 0 && !publication.IssuedAt.IsZero()) || (publication.Generation > 0 && publication.IssuedAt.IsZero()) {
		return CollectorPlanTrustBundlePublication{}, errors.New("stored collector plan trust state is invalid")
	}
	return publication, nil
}

func getActiveCollectorPlanSigningKeyTx(ctx context.Context, tx *sql.Tx, lock bool) (CollectorPlanSigningKey, error) {
	query := `SELECT key_id, algorithm, public_key, public_key_sha256, status,
		activated_at, retiring_at, trust_until, revoked_at,
		COALESCE(revocation_reason, ''), row_version, created_at, updated_at
		FROM collector_plan_signing_keys WHERE status = 'active'`
	if lock {
		query += " FOR UPDATE"
	}
	return scanCollectorPlanSigningKey(tx.QueryRowContext(ctx, query))
}

func getCollectorPlanSigningKeyTx(ctx context.Context, tx *sql.Tx, keyID string, lock bool) (CollectorPlanSigningKey, error) {
	query := `SELECT key_id, algorithm, public_key, public_key_sha256, status,
		activated_at, retiring_at, trust_until, revoked_at,
		COALESCE(revocation_reason, ''), row_version, created_at, updated_at
		FROM collector_plan_signing_keys WHERE key_id = ?`
	if lock {
		query += " FOR UPDATE"
	}
	return scanCollectorPlanSigningKey(tx.QueryRowContext(ctx, query, keyID))
}

func scanCollectorPlanSigningKey(row collectorPlanRow) (CollectorPlanSigningKey, error) {
	var key CollectorPlanSigningKey
	var publicKey []byte
	var status string
	var retiringAt, trustUntil, revokedAt sql.NullTime
	if err := row.Scan(
		&key.KeyID, &key.Algorithm, &publicKey, &key.PublicKeySHA256, &status,
		&key.ActivatedAt, &retiringAt, &trustUntil, &revokedAt,
		&key.RevocationReason, &key.RowVersion, &key.CreatedAt, &key.UpdatedAt,
	); err != nil {
		return CollectorPlanSigningKey{}, err
	}
	key.Status = CollectorPlanSigningKeyStatus(status)
	key.PublicKey = append(ed25519.PublicKey(nil), publicKey...)
	key.RetiringAt, key.TrustUntil, key.RevokedAt = retiringAt.Time, trustUntil.Time, revokedAt.Time
	digest := sha256.Sum256(key.PublicKey)
	if !validCollectorPlanSigningKeyID(key.KeyID) || key.Algorithm != "ed25519" || len(key.PublicKey) != ed25519.PublicKeySize ||
		key.PublicKeySHA256 != hex.EncodeToString(digest[:]) || key.RowVersion == 0 || key.ActivatedAt.IsZero() || key.CreatedAt.IsZero() || key.UpdatedAt.Before(key.CreatedAt) {
		return CollectorPlanSigningKey{}, errors.New("stored collector plan signing key is invalid")
	}
	switch key.Status {
	case CollectorPlanSigningKeyActive:
		if !key.RetiringAt.IsZero() || !key.TrustUntil.IsZero() || !key.RevokedAt.IsZero() || key.RevocationReason != "" {
			return CollectorPlanSigningKey{}, errors.New("stored active collector plan signing key lifecycle is invalid")
		}
	case CollectorPlanSigningKeyRetiring:
		if key.RetiringAt.IsZero() || key.TrustUntil.Before(key.RetiringAt) || !key.RevokedAt.IsZero() || key.RevocationReason != "" {
			return CollectorPlanSigningKey{}, errors.New("stored retiring collector plan signing key lifecycle is invalid")
		}
	case CollectorPlanSigningKeyRevoked:
		if key.RetiringAt.IsZero() || key.TrustUntil.IsZero() || key.RevokedAt.IsZero() || key.RevocationReason == "" {
			return CollectorPlanSigningKey{}, errors.New("stored revoked collector plan signing key lifecycle is invalid")
		}
	default:
		return CollectorPlanSigningKey{}, errors.New("stored collector plan signing key status is invalid")
	}
	return key, nil
}

func maxStartableCollectorPlanExpiryTx(ctx context.Context, tx *sql.Tx, keyID string, now time.Time) (time.Time, error) {
	var expiresAt sql.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT MAX(expires_at) FROM collector_plan_revisions
		WHERE signing_key_id = ? AND status IN ('validated','active') AND expires_at > ?
	`, keyID, now).Scan(&expiresAt); err != nil {
		return time.Time{}, err
	}
	return expiresAt.Time.UTC().Truncate(time.Millisecond), nil
}

func expireRetiringCollectorPlanSigningKeysTx(ctx context.Context, tx *sql.Tx, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_plan_signing_keys
		SET status = 'revoked', revoked_at = ?, revocation_reason = 'retirement-overlap-complete',
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE status = 'retiring' AND trust_until <= ?
	`, now, now)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func publishCollectorPlanTrustBundleTx(ctx context.Context, tx *sql.Tx, current CollectorPlanTrustBundlePublication, issuedAt time.Time) (CollectorPlanTrustBundlePublication, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT key_id, algorithm, public_key, status, trust_until
		FROM collector_plan_signing_keys ORDER BY key_id
	`)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	defer rows.Close()
	bundle := flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion,
		Generation:    current.Generation + 1, IssuedAtUnixMilli: issuedAt.UnixMilli(),
		Keys: []flowplan.TrustBundleKey{}, RevokedKeyIDs: []string{},
	}
	for rows.Next() {
		var keyID, algorithm, status string
		var publicKey []byte
		var trustUntil sql.NullTime
		if err := rows.Scan(&keyID, &algorithm, &publicKey, &status, &trustUntil); err != nil {
			return CollectorPlanTrustBundlePublication{}, err
		}
		switch CollectorPlanSigningKeyStatus(status) {
		case CollectorPlanSigningKeyActive:
			bundle.Keys = append(bundle.Keys, flowplan.TrustBundleKey{
				KeyID: keyID, Algorithm: algorithm, PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: status,
			})
		case CollectorPlanSigningKeyRetiring:
			if !trustUntil.Valid || !trustUntil.Time.After(issuedAt) {
				return CollectorPlanTrustBundlePublication{}, errors.New("expired retiring collector plan key reached trust bundle publication")
			}
			bundle.Keys = append(bundle.Keys, flowplan.TrustBundleKey{
				KeyID: keyID, Algorithm: algorithm, PublicKey: base64.StdEncoding.EncodeToString(publicKey),
				Status: status, TrustUntilUnixMilli: trustUntil.Time.UnixMilli(),
			})
		case CollectorPlanSigningKeyRevoked:
			bundle.RevokedKeyIDs = append(bundle.RevokedKeyIDs, keyID)
		default:
			return CollectorPlanTrustBundlePublication{}, errors.New("unsupported collector plan signing key state")
		}
	}
	if err := rows.Err(); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	flowplan.SortTrustBundle(&bundle)
	bundleJSON, checksum, err := flowplan.MarshalTrustBundle(bundle)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_plan_trust_state
		SET generation = ?, bundle_json = ?, checksum_sha256 = ?, issued_at = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE singleton_id = 1 AND row_version = ?
	`, bundle.Generation, string(bundleJSON), checksum, issuedAt, current.RowVersion)
	if err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return CollectorPlanTrustBundlePublication{}, err
	}
	return CollectorPlanTrustBundlePublication{
		Generation: bundle.Generation, BundleJSON: bundleJSON, Checksum: checksum,
		IssuedAt: issuedAt, RowVersion: current.RowVersion + 1,
	}, nil
}

func mapCollectorPlanSigningKeyWriteError(err error) error {
	if isMySQLDuplicateKey(err) {
		return ErrCollectorPlanSigningKeyReuse
	}
	return err
}

func isMySQLDuplicateKey(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}

var _ CollectorPlanTrustRepository = (*MySQLStore)(nil)
