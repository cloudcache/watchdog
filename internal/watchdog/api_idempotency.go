package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// PLAT-00A idempotent writes: POST/action handlers wrapped in WithIdempotency
// replay the stored response for a repeated (Idempotency-Key, request body)
// pair and reject the same key with a different body as a conflict. Requests
// without the header keep today's behavior (compatibility window).

const idempotencyKeyHeader = "Idempotency-Key"

type IdempotencyRecord struct {
	TenantID       ID
	Key            string
	RequestHash    string
	ResponseStatus int
	ResponseBody   []byte
	ExpiresAt      time.Time
}

type IdempotencyRepository interface {
	GetIdempotencyRecord(ctx context.Context, tenantID ID, key string) (IdempotencyRecord, error)
	SaveIdempotencyRecord(ctx context.Context, record IdempotencyRecord) error
}

func (s *MySQLStore) GetIdempotencyRecord(ctx context.Context, tenantID ID, key string) (IdempotencyRecord, error) {
	var record IdempotencyRecord
	err := s.db.QueryRowContext(ctx, `
		SELECT tenant_id, idempotency_key, request_hash, response_status, response_body, expires_at
		FROM idempotency_records
		WHERE tenant_id = ? AND idempotency_key = ? AND expires_at > NOW(3)
	`, tenantID, key).Scan(&record.TenantID, &record.Key, &record.RequestHash,
		&record.ResponseStatus, &record.ResponseBody, &record.ExpiresAt)
	return record, err
}

func (s *MySQLStore) SaveIdempotencyRecord(ctx context.Context, record IdempotencyRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO idempotency_records
			(tenant_id, idempotency_key, request_hash, response_status, response_body, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			request_hash = VALUES(request_hash),
			response_status = VALUES(response_status),
			response_body = VALUES(response_body),
			expires_at = VALUES(expires_at)
	`, record.TenantID, record.Key, record.RequestHash, record.ResponseStatus, record.ResponseBody, record.ExpiresAt)
	return err
}

type responseCapture struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (c *responseCapture) WriteHeader(status int) {
	c.status = status
	c.ResponseWriter.WriteHeader(status)
}

func (c *responseCapture) Write(data []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	if c.body.Len() < 1<<20 {
		c.body.Write(data)
	}
	return c.ResponseWriter.Write(data)
}

// WithIdempotency wraps a mutating handler. The stored response replays only
// while the record is unexpired; success and client-error responses are
// recorded, server errors are not (a retry may succeed).
func WithIdempotency(repo IdempotencyRepository, ttl time.Duration, next http.HandlerFunc) http.HandlerFunc {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))
		if repo == nil || key == "" {
			next(w, r)
			return
		}
		if len(key) > 128 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Idempotency-Key must be at most 128 characters", nil)
			return
		}
		auth, _ := AuthFromContext(r.Context())
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "failed to read request body", nil)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		digest := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), body...))
		requestHash := hex.EncodeToString(digest[:])

		record, err := repo.GetIdempotencyRecord(r.Context(), auth.TenantID, key)
		if err == nil {
			if record.RequestHash != requestHash {
				WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest,
					"Idempotency-Key was already used with a different request", map[string]any{"idempotency_key": key})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotency-Replayed", "true")
			w.WriteHeader(record.ResponseStatus)
			_, _ = w.Write(record.ResponseBody)
			return
		}
		if !errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Idempotency storage is unavailable", nil)
			return
		}

		capture := &responseCapture{ResponseWriter: w}
		next(capture, r)
		if capture.status >= 200 && capture.status < 500 {
			_ = repo.SaveIdempotencyRecord(r.Context(), IdempotencyRecord{
				TenantID:       auth.TenantID,
				Key:            key,
				RequestHash:    requestHash,
				ResponseStatus: capture.status,
				ResponseBody:   append([]byte(nil), capture.body.Bytes()...),
				ExpiresAt:      time.Now().UTC().Add(ttl),
			})
		}
	}
}

// Optimistic concurrency (PLAT-00A): entities expose a weak ETag derived from
// their last update; requests carrying If-Match are enforced, requests without
// it pass through during the documented compatibility window.

func WeakETagFromTime(updatedAt time.Time) string {
	return `W/"` + hex.EncodeToString([]byte(updatedAt.UTC().Format(time.RFC3339Nano)))[:16] + `-` +
		strings.TrimLeft(updatedAt.UTC().Format("20060102150405.000"), "0") + `"`
}

func SetEntityETag(w http.ResponseWriter, updatedAt time.Time) {
	w.Header().Set("ETag", WeakETagFromTime(updatedAt))
}

// CheckIfMatch returns false (and writes a 412) when the client presented an
// If-Match header that does not match the entity's current ETag.
func CheckIfMatch(w http.ResponseWriter, r *http.Request, updatedAt time.Time) bool {
	presented := strings.TrimSpace(r.Header.Get("If-Match"))
	if presented == "" || presented == "*" {
		return true
	}
	current := WeakETagFromTime(updatedAt)
	for _, candidate := range strings.Split(presented, ",") {
		if strings.TrimSpace(candidate) == current {
			return true
		}
	}
	WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"),
		"The resource changed since it was read", map[string]any{"current_etag": current})
	return false
}
