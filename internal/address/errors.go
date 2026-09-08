package address

import "errors"

// Taxonomy/set validation and lifecycle errors, preserved verbatim from the SaaS
// address library so the ported normalizers and repositories are unchanged.
var (
	ErrAddressTaxonomyInvalid  = errors.New("address taxonomy is invalid")
	ErrAddressTaxonomyConflict = errors.New("address taxonomy version conflict")
	ErrAddressTaxonomyCycle    = errors.New("address taxonomy hierarchy contains a cycle")
	ErrAddressTaxonomyInUse    = errors.New("address taxonomy item is in use")
)
