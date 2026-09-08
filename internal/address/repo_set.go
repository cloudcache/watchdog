package address

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
)

const addressPrefixColumns = `
	id, cidr, family, prefix_length, labels,
	COALESCE(geo_leaf_id, ''), COALESCE(operator_id, ''), asn,
	source, row_version, created_at, updated_at`

func scanAddressPrefix(row rowScanner) (AddressPrefix, error) {
	var prefix AddressPrefix
	var family, prefixLength sql.NullInt64
	var asn sql.NullInt64
	var labelsJSON []byte
	if err := row.Scan(&prefix.ID, &prefix.CIDR, &family, &prefixLength,
		&labelsJSON, &prefix.GeoLeafID, &prefix.OperatorID, &asn, &prefix.Source,
		&prefix.RowVersion, &prefix.CreatedAt, &prefix.UpdatedAt); err != nil {
		return AddressPrefix{}, err
	}
	if err := json.Unmarshal(labelsJSON, &prefix.Labels); err != nil {
		return AddressPrefix{}, err
	}
	if family.Valid && prefixLength.Valid {
		prefix.Family = uint8(family.Int64)
		prefix.PrefixLength = uint8(prefixLength.Int64)
	} else {
		parsed, err := netip.ParsePrefix(prefix.CIDR)
		if err != nil {
			return AddressPrefix{}, fmt.Errorf("legacy address prefix %s: %w", prefix.ID, err)
		}
		prefix.Family = 6
		if parsed.Addr().Is4() {
			prefix.Family = 4
		}
		prefix.PrefixLength = uint8(parsed.Bits())
	}
	if asn.Valid {
		value := uint32(asn.Int64)
		prefix.ASN = &value
	}
	return prefix, nil
}

func (s *Store) ListAddressPrefixes(ctx context.Context) ([]AddressPrefix, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+addressPrefixColumns+`
		FROM address_prefixes ORDER BY cidr
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	prefixes := make([]AddressPrefix, 0)
	for rows.Next() {
		prefix, err := scanAddressPrefix(rows)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, rows.Err()
}

var addressPrefixSortColumns = map[string]string{
	"":              "cidr",
	"cidr":          "cidr",
	"family":        "family",
	"prefix_length": "prefix_length",
	"asn":           "asn",
	"source":        "source",
	"updated":       "updated_at",
}

func applyAddressPrefixListFilters(query string, args []any, filter AddressPrefixListFilter) (string, []any) {
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		query += ` AND (cidr LIKE ? OR CAST(labels AS CHAR) LIKE ?)`
		args = append(args, like, like)
	}
	if filter.Family != 0 {
		query += ` AND family = ?`
		args = append(args, filter.Family)
	}
	if filter.Source != "" {
		query += ` AND source = ?`
		args = append(args, filter.Source)
	}
	if filter.GeoLeafID != "" {
		query += ` AND geo_leaf_id = ?`
		args = append(args, filter.GeoLeafID)
	}
	if filter.OperatorID != "" {
		query += ` AND operator_id = ?`
		args = append(args, filter.OperatorID)
	}
	if filter.ASN != nil {
		query += ` AND asn = ?`
		args = append(args, *filter.ASN)
	}
	return query, args
}

func (s *Store) ListAddressPrefixesPage(ctx context.Context, filter AddressPrefixListFilter) ([]AddressPrefix, string, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Source = strings.TrimSpace(filter.Source)
	_, validSort := addressPrefixSortColumns[filter.Sort]
	if len(filter.Search) > 255 || len(filter.Source) > 32 || filter.Offset < 0 || !validSort || (filter.Family != 0 && filter.Family != 4 && filter.Family != 6) {
		return nil, "", 0, fmt.Errorf("%w: invalid address prefix filter", ErrAddressTaxonomyInvalid)
	}
	where := ` WHERE 1=1`
	args := []any{}
	where, args = applyAddressPrefixListFilters(where, args, filter)
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_prefixes`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressPrefixColumns + ` FROM address_prefixes` + where
	if filter.Cursor != "" {
		cidr, id, err := decodeStringCursor(filter.Cursor)
		if err != nil {
			return nil, "", 0, err
		}
		query += ` AND (cidr > ? OR (cidr = ? AND id > ?))`
		args = append(args, cidr, cidr, id)
	}
	if filter.TableMode {
		sortColumn := addressPrefixSortColumns[filter.Sort]
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		if sortColumn == "cidr" {
			query += fmt.Sprintf(" ORDER BY family %s, ip_start %s, prefix_length %s, id %s LIMIT ? OFFSET ?", direction, direction, direction, direction)
		} else {
			query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, direction, direction)
		}
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY cidr, id LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressPrefix, 0, filter.Limit)
	for rows.Next() {
		item, err := scanAddressPrefix(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	nextCursor := ""
	if !filter.TableMode && len(items) > filter.Limit {
		items = items[:filter.Limit]
		last := items[len(items)-1]
		nextCursor = encodeStringCursor(last.CIDR, ID(last.ID))
	}
	return items, nextCursor, total, nil
}

func (s *Store) GetAddressPrefix(ctx context.Context, prefixID string) (AddressPrefix, error) {
	return scanAddressPrefix(s.db.QueryRowContext(ctx, `SELECT `+addressPrefixColumns+` FROM address_prefixes WHERE id = ?`, prefixID))
}

func (s *Store) UpsertAddressPrefix(ctx context.Context, prefix AddressPrefix) (AddressPrefix, error) {
	prefix, start, end, err := normalizeAddressPrefix(prefix)
	if err != nil {
		return AddressPrefix{}, err
	}
	labelsJSON, err := json.Marshal(prefix.Labels)
	if err != nil {
		return prefix, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO address_prefixes (id, cidr, family, prefix_length, ip_start, ip_end,
			labels, geo_leaf_id, operator_id, asn, source)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)
	`, prefix.ID, prefix.CIDR, prefix.Family, prefix.PrefixLength, start[:], end[:],
		labelsJSON, prefix.GeoLeafID, prefix.OperatorID, prefix.ASN, prefix.Source)
	if err != nil {
		return AddressPrefix{}, err
	}
	return s.GetAddressPrefix(ctx, prefix.ID)
}

func (s *Store) UpdateAddressPrefix(ctx context.Context, prefix AddressPrefix, expectedVersion uint64) (AddressPrefix, error) {
	if expectedVersion == 0 {
		return AddressPrefix{}, ErrAddressTaxonomyConflict
	}
	prefix, start, end, err := normalizeAddressPrefix(prefix)
	if err != nil {
		return AddressPrefix{}, err
	}
	labelsJSON, err := json.Marshal(prefix.Labels)
	if err != nil {
		return AddressPrefix{}, err
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE address_prefixes SET cidr = ?, family = ?, prefix_length = ?, ip_start = ?, ip_end = ?,
			labels = ?, geo_leaf_id = NULLIF(?, ''), operator_id = NULLIF(?, ''), asn = ?, source = ?,
			row_version = row_version + 1
		WHERE id = ? AND row_version = ?
	`, prefix.CIDR, prefix.Family, prefix.PrefixLength, start[:], end[:], labelsJSON,
		prefix.GeoLeafID, prefix.OperatorID, prefix.ASN, prefix.Source, prefix.ID, expectedVersion)
	if err != nil {
		return AddressPrefix{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		if _, getErr := s.GetAddressPrefix(ctx, prefix.ID); getErr != nil {
			return AddressPrefix{}, getErr
		}
		return AddressPrefix{}, ErrAddressTaxonomyConflict
	}
	return s.GetAddressPrefix(ctx, prefix.ID)
}

func (s *Store) DeleteAddressPrefix(ctx context.Context, prefixID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM address_prefixes WHERE id = ?`, prefixID)
	return err
}

func (s *Store) DeleteAddressPrefixVersion(ctx context.Context, prefixID string, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return ErrAddressTaxonomyConflict
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM address_prefixes WHERE id = ? AND row_version = ?`, prefixID, expectedVersion)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 1 {
		return nil
	}
	if _, err := s.GetAddressPrefix(ctx, prefixID); err != nil {
		return err
	}
	return ErrAddressTaxonomyConflict
}

const addressSetColumns = `
	id, name, description, selector, explicit_members,
	explicit_exclude_members, include_set_ids, exclude_set_ids,
	match_direction, enabled, row_version, created_at, updated_at`

func (s *Store) ListAddressSets(ctx context.Context) ([]AddressSet, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+addressSetColumns+`
		FROM address_sets ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sets []AddressSet
	for rows.Next() {
		item, err := scanAddressSet(rows)
		if err != nil {
			return nil, err
		}
		sets = append(sets, item)
	}
	return sets, rows.Err()
}

var addressSetSortColumns = map[string]string{
	"":          "name",
	"name":      "name",
	"direction": "match_direction",
	"enabled":   "enabled",
	"updated":   "updated_at",
}

func applyAddressSetListFilters(query string, args []any, filter AddressSetListFilter) (string, []any) {
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		query += ` AND (name LIKE ? OR COALESCE(description, '') LIKE ?)`
		args = append(args, like, like)
	}
	if filter.MatchDirection != "" {
		query += ` AND match_direction = ?`
		args = append(args, filter.MatchDirection)
	}
	if filter.Enabled != nil {
		query += ` AND enabled = ?`
		args = append(args, *filter.Enabled)
	}
	return query, args
}

func (s *Store) ListAddressSetsPage(ctx context.Context, filter AddressSetListFilter) ([]AddressSet, string, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	filter.Search = strings.TrimSpace(filter.Search)
	filter.MatchDirection = strings.ToLower(strings.TrimSpace(filter.MatchDirection))
	_, validSort := addressSetSortColumns[filter.Sort]
	if len(filter.Search) > 255 || filter.Offset < 0 || !validSort || (filter.MatchDirection != "" && filter.MatchDirection != "in" && filter.MatchDirection != "out" && filter.MatchDirection != "both") {
		return nil, "", 0, fmt.Errorf("%w: invalid address set filter", ErrAddressTaxonomyInvalid)
	}
	where := ` WHERE 1=1`
	args := []any{}
	where, args = applyAddressSetListFilters(where, args, filter)
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_sets`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressSetColumns + ` FROM address_sets` + where
	if filter.Cursor != "" {
		name, id, err := decodeStringCursor(filter.Cursor)
		if err != nil {
			return nil, "", 0, err
		}
		query += ` AND (name > ? OR (name = ? AND id > ?))`
		args = append(args, name, name, id)
	}
	if filter.TableMode {
		sortColumn := addressSetSortColumns[filter.Sort]
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY name, id LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressSet, 0, filter.Limit)
	for rows.Next() {
		item, err := scanAddressSet(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	nextCursor := ""
	if !filter.TableMode && len(items) > filter.Limit {
		items = items[:filter.Limit]
		last := items[len(items)-1]
		nextCursor = encodeStringCursor(last.Name, ID(last.ID))
	}
	return items, nextCursor, total, nil
}

func (s *Store) GetAddressSet(ctx context.Context, setID string) (AddressSet, error) {
	return scanAddressSet(s.db.QueryRowContext(ctx, `
		SELECT `+addressSetColumns+`
		FROM address_sets WHERE id = ?
	`, setID))
}

func (s *Store) UpsertAddressSet(ctx context.Context, set AddressSet) (AddressSet, error) {
	var err error
	set, err = normalizeAddressSet(set)
	if err != nil {
		return set, err
	}
	if err := s.validateAddressSetReferences(ctx, set); err != nil {
		return AddressSet{}, err
	}
	selectorJSON, err := json.Marshal(set.Selector)
	if err != nil {
		return set, err
	}
	explicitMembers, _ := json.Marshal(set.ExplicitMembers)
	explicitExcludeMembers, _ := json.Marshal(set.ExplicitExcludeMembers)
	includeSetIDs, _ := json.Marshal(set.IncludeSetIDs)
	excludeSetIDs, _ := json.Marshal(set.ExcludeSetIDs)
	enabled := 0
	if set.Enabled {
		enabled = 1
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO address_sets (id, name, description, selector, explicit_members,
			explicit_exclude_members, include_set_ids, exclude_set_ids, match_direction, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, set.ID, set.Name, set.Description, selectorJSON, explicitMembers, explicitExcludeMembers,
		includeSetIDs, excludeSetIDs, set.MatchDirection, enabled)
	if err != nil {
		return set, err
	}
	return s.GetAddressSet(ctx, set.ID)
}

func (s *Store) UpdateAddressSet(ctx context.Context, set AddressSet, expectedVersion uint64) (AddressSet, error) {
	if expectedVersion == 0 {
		return AddressSet{}, ErrAddressTaxonomyConflict
	}
	var err error
	set, err = normalizeAddressSet(set)
	if err != nil {
		return AddressSet{}, err
	}
	if _, err := s.GetAddressSet(ctx, set.ID); err != nil {
		return AddressSet{}, err
	}
	if err := s.validateAddressSetReferences(ctx, set); err != nil {
		return AddressSet{}, err
	}
	selectorJSON, _ := json.Marshal(set.Selector)
	explicitMembers, _ := json.Marshal(set.ExplicitMembers)
	explicitExcludeMembers, _ := json.Marshal(set.ExplicitExcludeMembers)
	includeSetIDs, _ := json.Marshal(set.IncludeSetIDs)
	excludeSetIDs, _ := json.Marshal(set.ExcludeSetIDs)
	result, err := s.db.ExecContext(ctx, `
		UPDATE address_sets SET name = ?, description = ?, selector = ?, explicit_members = ?,
			explicit_exclude_members = ?, include_set_ids = ?, exclude_set_ids = ?,
			match_direction = ?, enabled = ?, row_version = row_version + 1
		WHERE id = ? AND row_version = ?
	`, set.Name, set.Description, selectorJSON, explicitMembers, explicitExcludeMembers,
		includeSetIDs, excludeSetIDs, set.MatchDirection, set.Enabled, set.ID, expectedVersion)
	if err != nil {
		return AddressSet{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return AddressSet{}, ErrAddressTaxonomyConflict
	}
	return s.GetAddressSet(ctx, set.ID)
}

func (s *Store) validateAddressSetReferences(ctx context.Context, set AddressSet) error {
	var selector struct {
		GeoNodeIDs  []ID     `json:"geo_node_ids"`
		OperatorIDs []ID     `json:"operator_ids"`
		ASNs        []uint32 `json:"asns"`
		Families    []int    `json:"families"`
		Labels      any      `json:"labels"`
	}
	data, err := json.Marshal(set.Selector)
	if err != nil || json.Unmarshal(data, &selector) != nil {
		return fmt.Errorf("%w: selector value types", ErrAddressTaxonomyInvalid)
	}
	for _, family := range selector.Families {
		if family != 4 && family != 6 {
			return fmt.Errorf("%w: selector family must be 4 or 6", ErrAddressTaxonomyInvalid)
		}
	}
	for _, id := range selector.GeoNodeIDs {
		if _, err := s.GetGeoDictionary(ctx, id); err != nil {
			return fmt.Errorf("selector geo node %s: %w", id, err)
		}
	}
	for _, id := range selector.OperatorIDs {
		if _, err := s.GetISPOperator(ctx, id); err != nil {
			return fmt.Errorf("selector operator %s: %w", id, err)
		}
	}
	visiting := map[string]bool{set.ID: true}
	visited := map[string]bool{}
	nodes := 0
	var walk func(string, int) error
	walk = func(id string, depth int) error {
		if depth > 32 || nodes >= 10_000 {
			return fmt.Errorf("%w: address set dependency budget exceeded", ErrAddressTaxonomyInvalid)
		}
		if visiting[id] {
			return ErrAddressTaxonomyCycle
		}
		if visited[id] {
			return nil
		}
		nodes++
		visiting[id] = true
		item, err := s.GetAddressSet(ctx, id)
		if err != nil {
			return fmt.Errorf("address set %s: %w", id, err)
		}
		for _, child := range append(append([]string(nil), item.IncludeSetIDs...), item.ExcludeSetIDs...) {
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	for _, id := range append(append([]string(nil), set.IncludeSetIDs...), set.ExcludeSetIDs...) {
		if err := walk(id, 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteAddressSet(ctx context.Context, setID string) error {
	set, err := s.GetAddressSet(ctx, setID)
	if err != nil {
		return err
	}
	return s.DeleteAddressSetVersion(ctx, setID, set.RowVersion)
}

func (s *Store) DeleteAddressSetVersion(ctx context.Context, setID string, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return ErrAddressTaxonomyConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT row_version FROM address_sets WHERE id = ? FOR UPDATE`, setID).Scan(&currentVersion); err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return ErrAddressTaxonomyConflict
	}
	var referenced bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM geo_lines WHERE address_set_id = ?
			UNION ALL SELECT 1 FROM address_sets WHERE id <> ?
				AND (JSON_CONTAINS(include_set_ids, JSON_QUOTE(?)) OR JSON_CONTAINS(exclude_set_ids, JSON_QUOTE(?)))
		)
	`, setID, setID, setID, setID).Scan(&referenced); err != nil {
		return err
	}
	if referenced {
		return ErrAddressTaxonomyInUse
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM address_sets WHERE id = ?`, setID); err != nil {
		return err
	}
	return tx.Commit()
}

func scanAddressSet(row rowScanner) (AddressSet, error) {
	var set AddressSet
	var selectorJSON, explicitMembers, explicitExcludeMembers, includeSetIDs, excludeSetIDs []byte
	var description *string
	var enabled int
	if err := row.Scan(&set.ID, &set.Name, &description, &selectorJSON,
		&explicitMembers, &explicitExcludeMembers, &includeSetIDs, &excludeSetIDs,
		&set.MatchDirection, &enabled, &set.RowVersion, &set.CreatedAt, &set.UpdatedAt); err != nil {
		return set, err
	}
	if description != nil {
		set.Description = *description
	}
	set.Enabled = enabled == 1
	if err := json.Unmarshal(selectorJSON, &set.Selector); err != nil {
		return set, err
	}
	selector, err := normalizeAddressSetSelector(set.Selector)
	if err != nil {
		return set, fmt.Errorf("address set %s selector: %w", set.ID, err)
	}
	set.Selector = selector
	if err := json.Unmarshal(explicitMembers, &set.ExplicitMembers); err != nil {
		return set, err
	}
	if err := json.Unmarshal(explicitExcludeMembers, &set.ExplicitExcludeMembers); err != nil {
		return set, err
	}
	if err := json.Unmarshal(includeSetIDs, &set.IncludeSetIDs); err != nil {
		return set, err
	}
	if err := json.Unmarshal(excludeSetIDs, &set.ExcludeSetIDs); err != nil {
		return set, err
	}
	return set, nil
}
