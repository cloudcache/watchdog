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
	"time"
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

// These ceilings mirror the current SNMP/Flow billing reader contracts. The
// configurable limits may lower them, but lifting them requires the chunked
// reader/checkpoint work tracked by KISS-07C.
const (
	HardMaxAccountPorts   = 1000
	HardMaxPeriodDuration = 400 * 24 * time.Hour
)

type Limits struct {
	MaxAccountPorts     int
	MaxPageSize         int
	MaxPeriodDuration   time.Duration
	MaxExportRows       int
	MaxPublicationRefs  int
	MaxPublicationBytes int
}

func DefaultLimits() Limits {
	return Limits{
		MaxAccountPorts:     HardMaxAccountPorts,
		MaxPageSize:         500,
		MaxPeriodDuration:   HardMaxPeriodDuration,
		MaxExportRows:       100000,
		MaxPublicationRefs:  10000,
		MaxPublicationBytes: 8 << 20,
	}
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxAccountPorts <= 0 {
		limits.MaxAccountPorts = defaults.MaxAccountPorts
	} else if limits.MaxAccountPorts > HardMaxAccountPorts {
		limits.MaxAccountPorts = HardMaxAccountPorts
	}
	if limits.MaxPageSize <= 0 {
		limits.MaxPageSize = defaults.MaxPageSize
	}
	if limits.MaxPeriodDuration <= 0 {
		limits.MaxPeriodDuration = defaults.MaxPeriodDuration
	} else if limits.MaxPeriodDuration > HardMaxPeriodDuration {
		limits.MaxPeriodDuration = HardMaxPeriodDuration
	}
	if limits.MaxExportRows <= 0 {
		limits.MaxExportRows = defaults.MaxExportRows
	}
	if limits.MaxPublicationRefs <= 0 {
		limits.MaxPublicationRefs = defaults.MaxPublicationRefs
	}
	if limits.MaxPublicationBytes <= 0 {
		limits.MaxPublicationBytes = defaults.MaxPublicationBytes
	}
	return limits
}

type Store struct {
	db     *sql.DB
	limits Limits
}

func NewStore(db *sql.DB) *Store { return NewStoreWithLimits(db, Limits{}) }

func NewStoreWithLimits(db *sql.DB, limits Limits) *Store {
	return &Store{db: db, limits: normalizeLimits(limits)}
}

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

func normalizePage(filter PageFilter, allowedSort map[string]string, fallback string, maxPageSize int) (PageFilter, string, error) {
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit < 1 || filter.Limit > maxPageSize || filter.Offset < 0 {
		return filter, "", fmt.Errorf("pagination must use limit 1..%d and a non-negative offset", maxPageSize)
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
