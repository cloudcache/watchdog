// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound            = errors.New("billing resource not found")
	ErrConflict            = errors.New("billing resource changed")
	ErrImmutable           = errors.New("billing period is immutable")
	ErrInUse               = errors.New("billing resource has dependent evidence")
	ErrEvidenceUnavailable = errors.New("billing evidence unavailable")
	ErrWindowOffset        = fmt.Errorf("%w: reader window offset", ErrEvidenceUnavailable)
)

var idEncoding = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

func NewID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return idEncoding.EncodeToString(value[:])
}

type PageFilter struct {
	Limit  int
	Offset int
	Query  string
	Sort   string
	Order  string
	Status string
	Kind   string
	Type   string
}

func normalizePage(filter PageFilter, allowedSort map[string]string, fallback string) (PageFilter, string, error) {
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit < 1 || filter.Limit > 500 || filter.Offset < 0 {
		return filter, "", errors.New("pagination must use limit 1..500 and a non-negative offset")
	}
	filter.Query, filter.Status, filter.Kind, filter.Type = strings.TrimSpace(filter.Query), strings.TrimSpace(filter.Status), strings.TrimSpace(filter.Kind), strings.TrimSpace(filter.Type)
	filter.Sort = strings.TrimSpace(filter.Sort)
	if filter.Sort == "" {
		filter.Sort = fallback
	}
	column, ok := allowedSort[filter.Sort]
	if !ok {
		return filter, "", errors.New("unsupported sort field")
	}
	filter.Order = strings.ToLower(strings.TrimSpace(filter.Order))
	if filter.Order == "" {
		filter.Order = "asc"
	}
	if filter.Order != "asc" && filter.Order != "desc" {
		return filter, "", errors.New("sort order must be asc or desc")
	}
	return filter, column + " " + strings.ToUpper(filter.Order), nil
}

func normalizeSQLError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func changed(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrConflict
	}
	return nil
}

func auditTx(ctx context.Context, tx *sql.Tx, actor, action, resource, resourceID string, detail any) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO audit_logs (id,actor_id,action,resource,resource_id,detail_json)
		VALUES (?,NULLIF(?,''),?,?,?,?)`, NewID(), actor, action, resource, resourceID, detail)
	if err != nil {
		return fmt.Errorf("write billing audit: %w", err)
	}
	return nil
}
