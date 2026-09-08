package watchdog

import "context"

// recordingAuditRepository is shared by API tests that verify an audited
// mutation. It is intentionally independent of the deleted identity-admin API.
type recordingAuditRepository struct {
	logs []AuditLog
}

func (r *recordingAuditRepository) CreateAuditLog(_ context.Context, log AuditLog) error {
	r.logs = append(r.logs, log)
	return nil
}
