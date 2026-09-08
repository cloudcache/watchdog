package address

import "database/sql"

// Store is the MySQL-backed address library repository. It is the de-tenanted
// port of the SaaS MySQLStore address methods: identical SQL and normalization,
// with tenant_id scoping removed (single-domain build).
type Store struct{ db *sql.DB }

// NewStore wraps an open *sql.DB. The address schema (deploy/schema/mysql) is
// applied by the server's schema applier before the store is used.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

type rowScanner interface{ Scan(dest ...any) error }
