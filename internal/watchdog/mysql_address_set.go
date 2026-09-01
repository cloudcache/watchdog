package watchdog

import (
	"context"
	"encoding/json"
)

func (s *MySQLStore) ListAddressPrefixes(ctx context.Context, tenantID ID) ([]AddressPrefix, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, cidr, labels, source, created_at, updated_at
		FROM address_prefixes WHERE tenant_id = ? ORDER BY cidr
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var prefixes []AddressPrefix
	for rows.Next() {
		var p AddressPrefix
		var labelsJSON []byte
		if err := rows.Scan(&p.ID, &p.TenantID, &p.CIDR, &labelsJSON, &p.Source, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(labelsJSON, &p.Labels); err != nil {
			p.Labels = map[string]string{}
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, rows.Err()
}

func (s *MySQLStore) UpsertAddressPrefix(ctx context.Context, prefix AddressPrefix) (AddressPrefix, error) {
	labelsJSON, err := json.Marshal(prefix.Labels)
	if err != nil {
		return prefix, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO address_prefixes (id, tenant_id, cidr, labels, source)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE labels = VALUES(labels), source = VALUES(source)
	`, prefix.ID, prefix.TenantID, prefix.CIDR, labelsJSON, prefix.Source)
	return prefix, err
}

func (s *MySQLStore) DeleteAddressPrefix(ctx context.Context, tenantID ID, prefixID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM address_prefixes WHERE id = ? AND tenant_id = ?`, prefixID, tenantID)
	return err
}

func (s *MySQLStore) ListAddressSets(ctx context.Context, tenantID ID) ([]AddressSet, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, name, description, selector, match_direction, enabled, created_at, updated_at
		FROM address_sets WHERE tenant_id = ? ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sets []AddressSet
	for rows.Next() {
		var s AddressSet
		var selectorJSON []byte
		var description *string
		var enabled int
		if err := rows.Scan(&s.ID, &s.TenantID, &s.Name, &description, &selectorJSON, &s.MatchDirection, &enabled, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		if description != nil {
			s.Description = *description
		}
		s.Enabled = enabled == 1
		if err := json.Unmarshal(selectorJSON, &s.Selector); err != nil {
			s.Selector = map[string]any{}
		}
		sets = append(sets, s)
	}
	return sets, rows.Err()
}

func (s *MySQLStore) GetAddressSet(ctx context.Context, tenantID ID, setID string) (AddressSet, error) {
	var set AddressSet
	var selectorJSON []byte
	var description *string
	var enabled int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, description, selector, match_direction, enabled, created_at, updated_at
		FROM address_sets WHERE id = ? AND tenant_id = ?
	`, setID, tenantID).Scan(&set.ID, &set.TenantID, &set.Name, &description, &selectorJSON, &set.MatchDirection, &enabled, &set.CreatedAt, &set.UpdatedAt)
	if err != nil {
		return set, err
	}
	if description != nil {
		set.Description = *description
	}
	set.Enabled = enabled == 1
	if err := json.Unmarshal(selectorJSON, &set.Selector); err != nil {
		set.Selector = map[string]any{}
	}
	return set, nil
}

func (s *MySQLStore) UpsertAddressSet(ctx context.Context, set AddressSet) (AddressSet, error) {
	selectorJSON, err := json.Marshal(set.Selector)
	if err != nil {
		return set, err
	}
	enabled := 0
	if set.Enabled {
		enabled = 1
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO address_sets (id, tenant_id, name, description, selector, match_direction, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE description = VALUES(description), selector = VALUES(selector), match_direction = VALUES(match_direction), enabled = VALUES(enabled)
	`, set.ID, set.TenantID, set.Name, set.Description, selectorJSON, set.MatchDirection, enabled)
	return set, err
}

func (s *MySQLStore) DeleteAddressSet(ctx context.Context, tenantID ID, setID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM address_sets WHERE id = ? AND tenant_id = ?`, setID, tenantID)
	return err
}

var _ AddressSetRepository = (*MySQLStore)(nil)
