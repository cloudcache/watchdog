package watchdog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

type addressTaxonomyCursor struct {
	SortOrder int    `json:"s"`
	Name      string `json:"n"`
	ID        ID     `json:"i"`
}

func encodeAddressTaxonomyCursor(cursor addressTaxonomyCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeAddressTaxonomyCursor(value string) (addressTaxonomyCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return addressTaxonomyCursor{}, fmt.Errorf("%w: invalid cursor", ErrAddressTaxonomyInvalid)
	}
	var cursor addressTaxonomyCursor
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.ID == "" {
		return addressTaxonomyCursor{}, fmt.Errorf("%w: invalid cursor", ErrAddressTaxonomyInvalid)
	}
	return cursor, nil
}

func normalizeAddressTaxonomyListFilter(filter AddressTaxonomyListFilter) (AddressTaxonomyListFilter, error) {
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Kind = strings.ToLower(strings.TrimSpace(filter.Kind))
	if len(filter.Search) > 255 || filter.Offset < 0 || (filter.Kind != "" && !validGeoKind(filter.Kind)) {
		return filter, fmt.Errorf("%w: invalid list filter", ErrAddressTaxonomyInvalid)
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	return filter, nil
}

var geoDictionarySortColumns = map[string]string{
	"":        "sort_order",
	"order":   "sort_order",
	"code":    "code",
	"name":    "name",
	"kind":    "kind",
	"enabled": "enabled",
}

var ispOperatorSortColumns = map[string]string{
	"":            "sort_order",
	"order":       "sort_order",
	"code":        "code",
	"name":        "name",
	"flow_isp_id": "flow_isp_id",
	"category":    "category",
	"enabled":     "enabled",
}

var geoLineSortColumns = map[string]string{
	"":        "sort_order",
	"order":   "sort_order",
	"code":    "code",
	"name":    "name",
	"parent":  "parent_id",
	"enabled": "enabled",
}

const geoDictionaryColumns = `
	id, tenant_id, kind, code, COALESCE(parent_id, ''), name,
	COALESCE(short_name, ''), sort_order, enabled, row_version, created_at, updated_at`

func scanGeoDictionaryNode(row rowScanner) (GeoDictionaryNode, error) {
	var item GeoDictionaryNode
	err := row.Scan(&item.ID, &item.TenantID, &item.Kind, &item.Code, &item.ParentID, &item.Name,
		&item.ShortName, &item.SortOrder, &item.Enabled, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt)
	return item, err
}

func (s *MySQLStore) GetGeoDictionary(ctx context.Context, tenantID, id ID) (GeoDictionaryNode, error) {
	return scanGeoDictionaryNode(s.db.QueryRowContext(ctx, `SELECT `+geoDictionaryColumns+` FROM geo_dict WHERE tenant_id = ? AND id = ?`, tenantID, id))
}

func (s *MySQLStore) ListGeoDictionary(ctx context.Context, tenantID ID, filter AddressTaxonomyListFilter) ([]GeoDictionaryNode, string, int, error) {
	filter, err := normalizeAddressTaxonomyListFilter(filter)
	if err != nil {
		return nil, "", 0, err
	}
	if _, ok := geoDictionarySortColumns[filter.Sort]; !ok {
		return nil, "", 0, fmt.Errorf("%w: invalid geography sort", ErrAddressTaxonomyInvalid)
	}
	if filter.Cursor != "" {
		if _, err := decodeAddressTaxonomyCursor(filter.Cursor); err != nil {
			return nil, "", 0, err
		}
	}
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	where, args = appendAddressTaxonomyFilters(where, args, filter, true, "COALESCE(short_name, '')")
	countWhere := where
	countArgs := append([]any(nil), args...)
	if filter.Cursor != "" {
		countFilter := filter
		countFilter.Cursor = ""
		countWhere = ` WHERE tenant_id = ?`
		countArgs = []any{tenantID}
		countWhere, countArgs = appendAddressTaxonomyFilters(countWhere, countArgs, countFilter, true, "COALESCE(short_name, '')")
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geo_dict`+countWhere, countArgs...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + geoDictionaryColumns + ` FROM geo_dict` + where
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, name %s, id %s LIMIT ? OFFSET ?", geoDictionarySortColumns[filter.Sort], direction, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY sort_order, name, id LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]GeoDictionaryNode, 0, filter.Limit)
	for rows.Next() {
		item, err := scanGeoDictionaryNode(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	if filter.TableMode {
		return items, "", total, nil
	}
	items, cursor, err := pageGeoDictionary(items, filter.Limit)
	return items, cursor, total, err
}

func appendAddressTaxonomyFilters(query string, args []any, filter AddressTaxonomyListFilter, allowKind bool, thirdSearchColumn string) (string, []any) {
	if allowKind && filter.Kind != "" {
		query += ` AND kind = ?`
		args = append(args, filter.Kind)
	}
	if filter.ParentID != "" {
		query += ` AND parent_id = ?`
		args = append(args, filter.ParentID)
	}
	if filter.Enabled != nil {
		query += ` AND enabled = ?`
		args = append(args, *filter.Enabled)
	}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		query += ` AND (name LIKE ? OR code LIKE ? OR ` + thirdSearchColumn + ` LIKE ?)`
		args = append(args, like, like, like)
	}
	if filter.Cursor != "" {
		cursor, err := decodeAddressTaxonomyCursor(filter.Cursor)
		if err == nil {
			query += ` AND (sort_order > ? OR (sort_order = ? AND (name > ? OR (name = ? AND id > ?))))`
			args = append(args, cursor.SortOrder, cursor.SortOrder, cursor.Name, cursor.Name, cursor.ID)
		} else {
			// Force a deterministic no-row query. The caller validates the cursor
			// before reaching this helper, so this is only a defensive fallback.
			query += ` AND 1 = 0`
		}
	}
	return query, args
}

func pageGeoDictionary(items []GeoDictionaryNode, limit int) ([]GeoDictionaryNode, string, error) {
	if len(items) <= limit {
		return items, "", nil
	}
	items = items[:limit]
	last := items[len(items)-1]
	return items, encodeAddressTaxonomyCursor(addressTaxonomyCursor{SortOrder: last.SortOrder, Name: last.Name, ID: last.ID}), nil
}

func (s *MySQLStore) CreateGeoDictionary(ctx context.Context, node GeoDictionaryNode) (GeoDictionaryNode, error) {
	var err error
	node, err = normalizeGeoDictionaryNode(node)
	if err != nil {
		return GeoDictionaryNode{}, err
	}
	if node.ID == "" {
		node.ID, err = newManagementID()
		if err != nil {
			return GeoDictionaryNode{}, err
		}
	}
	if err := s.validateGeoParent(ctx, node.TenantID, node.ID, node.Kind, node.ParentID); err != nil {
		return GeoDictionaryNode{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO geo_dict (id, tenant_id, kind, code, parent_id, name, short_name, sort_order, enabled)
		VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), ?, ?)
	`, node.ID, node.TenantID, node.Kind, node.Code, node.ParentID, node.Name, node.ShortName, node.SortOrder, node.Enabled)
	if err != nil {
		return GeoDictionaryNode{}, err
	}
	return s.GetGeoDictionary(ctx, node.TenantID, node.ID)
}

func (s *MySQLStore) UpdateGeoDictionary(ctx context.Context, node GeoDictionaryNode, expectedVersion uint64) (GeoDictionaryNode, error) {
	var err error
	node, err = normalizeGeoDictionaryNode(node)
	if err != nil {
		return GeoDictionaryNode{}, err
	}
	if expectedVersion == 0 {
		return GeoDictionaryNode{}, ErrAddressTaxonomyConflict
	}
	if _, err := s.GetGeoDictionary(ctx, node.TenantID, node.ID); err != nil {
		return GeoDictionaryNode{}, err
	}
	if err := s.validateGeoParent(ctx, node.TenantID, node.ID, node.Kind, node.ParentID); err != nil {
		return GeoDictionaryNode{}, err
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE geo_dict SET kind = ?, code = ?, parent_id = NULLIF(?, ''), name = ?,
			short_name = NULLIF(?, ''), sort_order = ?, enabled = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, node.Kind, node.Code, node.ParentID, node.Name, node.ShortName, node.SortOrder, node.Enabled, node.TenantID, node.ID, expectedVersion)
	if err != nil {
		return GeoDictionaryNode{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return GeoDictionaryNode{}, ErrAddressTaxonomyConflict
	}
	return s.GetGeoDictionary(ctx, node.TenantID, node.ID)
}

func (s *MySQLStore) validateGeoParent(ctx context.Context, tenantID, nodeID ID, childKind string, parentID ID) error {
	seen := map[ID]bool{nodeID: true}
	for depth := 0; parentID != ""; depth++ {
		if depth >= 5 || seen[parentID] {
			return ErrAddressTaxonomyCycle
		}
		seen[parentID] = true
		parent, err := s.GetGeoDictionary(ctx, tenantID, parentID)
		if err != nil {
			return err
		}
		if depth == 0 && !validGeoParentKind(parent.Kind, childKind) {
			return fmt.Errorf("%w: %s cannot be parent of %s", ErrAddressTaxonomyInvalid, parent.Kind, childKind)
		}
		parentID = parent.ParentID
	}
	return nil
}

func (s *MySQLStore) DeleteGeoDictionary(ctx context.Context, tenantID, id ID, expectedVersion uint64) error {
	return s.deleteAddressTaxonomyRow(ctx, "geo_dict", tenantID, id, expectedVersion, `
		SELECT EXISTS(
			SELECT 1 FROM geo_dict WHERE tenant_id = ? AND parent_id = ?
			UNION ALL SELECT 1 FROM address_prefixes WHERE tenant_id = ? AND geo_leaf_id = ?
			UNION ALL SELECT 1 FROM geo_lines WHERE tenant_id = ?
				AND JSON_CONTAINS(geo_selector, JSON_QUOTE(?), '$.geo_node_ids')
		)
	`, tenantID, id, tenantID, id, tenantID, string(id))
}

const ispOperatorColumns = `
	id, tenant_id, flow_isp_id, code, name, COALESCE(short_name, ''), category, asns,
	sort_order, enabled, row_version, created_at, updated_at`

func scanISPOperator(row rowScanner) (ISPOperator, error) {
	var item ISPOperator
	var asns []byte
	if err := row.Scan(&item.ID, &item.TenantID, &item.FlowISPID, &item.Code, &item.Name, &item.ShortName, &item.Category,
		&asns, &item.SortOrder, &item.Enabled, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return ISPOperator{}, err
	}
	if err := json.Unmarshal(asns, &item.ASNs); err != nil {
		return ISPOperator{}, err
	}
	return item, nil
}

func (s *MySQLStore) GetISPOperator(ctx context.Context, tenantID, id ID) (ISPOperator, error) {
	return scanISPOperator(s.db.QueryRowContext(ctx, `SELECT `+ispOperatorColumns+` FROM isp_operators WHERE tenant_id = ? AND id = ?`, tenantID, id))
}

func (s *MySQLStore) ListISPOperators(ctx context.Context, tenantID ID, filter AddressTaxonomyListFilter) ([]ISPOperator, string, int, error) {
	filter, err := normalizeAddressTaxonomyListFilter(filter)
	if err != nil {
		return nil, "", 0, err
	}
	if filter.Kind != "" || filter.ParentID != "" {
		return nil, "", 0, fmt.Errorf("%w: operator list does not accept kind or parent_id", ErrAddressTaxonomyInvalid)
	}
	if _, ok := ispOperatorSortColumns[filter.Sort]; !ok {
		return nil, "", 0, fmt.Errorf("%w: invalid operator sort", ErrAddressTaxonomyInvalid)
	}
	if filter.Cursor != "" {
		if _, err := decodeAddressTaxonomyCursor(filter.Cursor); err != nil {
			return nil, "", 0, err
		}
	}
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	where, args = appendAddressTaxonomyFilters(where, args, filter, false, "COALESCE(short_name, '')")
	countFilter := filter
	countFilter.Cursor = ""
	countWhere := ` WHERE tenant_id = ?`
	countArgs := []any{tenantID}
	countWhere, countArgs = appendAddressTaxonomyFilters(countWhere, countArgs, countFilter, false, "COALESCE(short_name, '')")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM isp_operators`+countWhere, countArgs...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + ispOperatorColumns + ` FROM isp_operators` + where
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, name %s, id %s LIMIT ? OFFSET ?", ispOperatorSortColumns[filter.Sort], direction, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY sort_order, name, id LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]ISPOperator, 0, filter.Limit)
	for rows.Next() {
		item, err := scanISPOperator(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	if filter.TableMode {
		return items, "", total, nil
	}
	if len(items) <= filter.Limit {
		return items, "", total, nil
	}
	items = items[:filter.Limit]
	last := items[len(items)-1]
	return items, encodeAddressTaxonomyCursor(addressTaxonomyCursor{SortOrder: last.SortOrder, Name: last.Name, ID: last.ID}), total, nil
}

func (s *MySQLStore) CreateISPOperator(ctx context.Context, operator ISPOperator) (ISPOperator, error) {
	var err error
	operator, err = normalizeISPOperator(operator)
	if err != nil {
		return ISPOperator{}, err
	}
	if operator.ID == "" {
		operator.ID, err = newManagementID()
		if err != nil {
			return ISPOperator{}, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ISPOperator{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO isp_operator_flow_id_sequences (tenant_id, next_flow_isp_id)
		VALUES (?, LAST_INSERT_ID(2))
		ON DUPLICATE KEY UPDATE next_flow_isp_id = CASE
			WHEN next_flow_isp_id < 65536 THEN LAST_INSERT_ID(next_flow_isp_id + 1)
			ELSE next_flow_isp_id + (0 * LAST_INSERT_ID(65537))
		END
	`, operator.TenantID); err != nil {
		return ISPOperator{}, err
	}
	var nextID uint32
	if err := tx.QueryRowContext(ctx, `SELECT LAST_INSERT_ID() - 1`).Scan(&nextID); err != nil {
		return ISPOperator{}, err
	}
	if nextID == 0 || nextID > 65_535 {
		return ISPOperator{}, fmt.Errorf("%w: Flow ISP id space is exhausted", ErrAddressTaxonomyInvalid)
	}
	operator.FlowISPID = uint16(nextID)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO isp_operator_flow_ids (tenant_id, flow_isp_id, operator_id)
		VALUES (?, ?, ?)
	`, operator.TenantID, operator.FlowISPID, operator.ID); err != nil {
		return ISPOperator{}, err
	}
	asns, _ := json.Marshal(operator.ASNs)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO isp_operators (id, tenant_id, flow_isp_id, code, name, short_name, category, asns, sort_order, enabled)
		VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?)
	`, operator.ID, operator.TenantID, operator.FlowISPID, operator.Code, operator.Name, operator.ShortName, operator.Category, asns, operator.SortOrder, operator.Enabled); err != nil {
		return ISPOperator{}, err
	}
	if err := tx.Commit(); err != nil {
		return ISPOperator{}, err
	}
	return s.GetISPOperator(ctx, operator.TenantID, operator.ID)
}

func (s *MySQLStore) UpdateISPOperator(ctx context.Context, operator ISPOperator, expectedVersion uint64) (ISPOperator, error) {
	var err error
	operator, err = normalizeISPOperator(operator)
	if err != nil {
		return ISPOperator{}, err
	}
	if expectedVersion == 0 {
		return ISPOperator{}, ErrAddressTaxonomyConflict
	}
	asns, _ := json.Marshal(operator.ASNs)
	result, err := s.db.ExecContext(ctx, `
		UPDATE isp_operators SET code = ?, name = ?, short_name = NULLIF(?, ''), category = ?,
			asns = ?, sort_order = ?, enabled = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, operator.Code, operator.Name, operator.ShortName, operator.Category, asns, operator.SortOrder, operator.Enabled, operator.TenantID, operator.ID, expectedVersion)
	if err != nil {
		return ISPOperator{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		if _, getErr := s.GetISPOperator(ctx, operator.TenantID, operator.ID); getErr != nil {
			return ISPOperator{}, getErr
		}
		return ISPOperator{}, ErrAddressTaxonomyConflict
	}
	return s.GetISPOperator(ctx, operator.TenantID, operator.ID)
}

func (s *MySQLStore) DeleteISPOperator(ctx context.Context, tenantID, id ID, expectedVersion uint64) error {
	return s.deleteAddressTaxonomyRow(ctx, "isp_operators", tenantID, id, expectedVersion, `
		SELECT EXISTS(
			SELECT 1 FROM geo_lines WHERE tenant_id = ? AND operator_id = ?
			UNION ALL SELECT 1 FROM address_prefixes WHERE tenant_id = ? AND operator_id = ?
			UNION ALL SELECT 1 FROM address_sets WHERE tenant_id = ?
				AND JSON_CONTAINS(selector, JSON_QUOTE(?), '$.operator_ids')
		)
	`, tenantID, id, tenantID, id, tenantID, string(id))
}

const geoLineColumns = `
	id, tenant_id, COALESCE(parent_id, ''), code, name, COALESCE(description, ''),
	geo_selector, COALESCE(operator_id, ''), COALESCE(address_set_id, ''), sort_order,
	enabled, row_version, created_at, updated_at`

func scanGeoLine(row rowScanner) (GeoLine, error) {
	var item GeoLine
	var selector []byte
	if err := row.Scan(&item.ID, &item.TenantID, &item.ParentID, &item.Code, &item.Name, &item.Description,
		&selector, &item.OperatorID, &item.AddressSetID, &item.SortOrder, &item.Enabled,
		&item.RowVersion, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return GeoLine{}, err
	}
	if err := json.Unmarshal(selector, &item.GeoSelector); err != nil {
		return GeoLine{}, err
	}
	return item, nil
}

func (s *MySQLStore) GetGeoLine(ctx context.Context, tenantID, id ID) (GeoLine, error) {
	return scanGeoLine(s.db.QueryRowContext(ctx, `SELECT `+geoLineColumns+` FROM geo_lines WHERE tenant_id = ? AND id = ?`, tenantID, id))
}

func (s *MySQLStore) ListGeoLines(ctx context.Context, tenantID ID, filter AddressTaxonomyListFilter) ([]GeoLine, string, int, error) {
	filter, err := normalizeAddressTaxonomyListFilter(filter)
	if err != nil {
		return nil, "", 0, err
	}
	if filter.Kind != "" {
		return nil, "", 0, fmt.Errorf("%w: line list does not accept kind", ErrAddressTaxonomyInvalid)
	}
	if _, ok := geoLineSortColumns[filter.Sort]; !ok {
		return nil, "", 0, fmt.Errorf("%w: invalid line sort", ErrAddressTaxonomyInvalid)
	}
	if filter.Cursor != "" {
		if _, err := decodeAddressTaxonomyCursor(filter.Cursor); err != nil {
			return nil, "", 0, err
		}
	}
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	where, args = appendAddressTaxonomyFilters(where, args, filter, false, "COALESCE(description, '')")
	countFilter := filter
	countFilter.Cursor = ""
	countWhere := ` WHERE tenant_id = ?`
	countArgs := []any{tenantID}
	countWhere, countArgs = appendAddressTaxonomyFilters(countWhere, countArgs, countFilter, false, "COALESCE(description, '')")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geo_lines`+countWhere, countArgs...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + geoLineColumns + ` FROM geo_lines` + where
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, name %s, id %s LIMIT ? OFFSET ?", geoLineSortColumns[filter.Sort], direction, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY sort_order, name, id LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]GeoLine, 0, filter.Limit)
	for rows.Next() {
		item, err := scanGeoLine(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	if filter.TableMode {
		return items, "", total, nil
	}
	if len(items) <= filter.Limit {
		return items, "", total, nil
	}
	items = items[:filter.Limit]
	last := items[len(items)-1]
	return items, encodeAddressTaxonomyCursor(addressTaxonomyCursor{SortOrder: last.SortOrder, Name: last.Name, ID: last.ID}), total, nil
}

func (s *MySQLStore) CreateGeoLine(ctx context.Context, line GeoLine) (GeoLine, error) {
	var err error
	line, err = normalizeGeoLine(line)
	if err != nil {
		return GeoLine{}, err
	}
	if line.ID == "" {
		line.ID, err = newManagementID()
		if err != nil {
			return GeoLine{}, err
		}
	}
	if err := s.validateGeoLineReferences(ctx, line); err != nil {
		return GeoLine{}, err
	}
	selector, _ := json.Marshal(line.GeoSelector)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO geo_lines (id, tenant_id, parent_id, code, name, description, geo_selector,
			operator_id, address_set_id, sort_order, enabled)
		VALUES (?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)
	`, line.ID, line.TenantID, line.ParentID, line.Code, line.Name, line.Description, selector,
		line.OperatorID, line.AddressSetID, line.SortOrder, line.Enabled)
	if err != nil {
		return GeoLine{}, err
	}
	return s.GetGeoLine(ctx, line.TenantID, line.ID)
}

func (s *MySQLStore) UpdateGeoLine(ctx context.Context, line GeoLine, expectedVersion uint64) (GeoLine, error) {
	var err error
	line, err = normalizeGeoLine(line)
	if err != nil {
		return GeoLine{}, err
	}
	if expectedVersion == 0 {
		return GeoLine{}, ErrAddressTaxonomyConflict
	}
	if _, err := s.GetGeoLine(ctx, line.TenantID, line.ID); err != nil {
		return GeoLine{}, err
	}
	if err := s.validateGeoLineReferences(ctx, line); err != nil {
		return GeoLine{}, err
	}
	selector, _ := json.Marshal(line.GeoSelector)
	result, err := s.db.ExecContext(ctx, `
		UPDATE geo_lines SET parent_id = NULLIF(?, ''), code = ?, name = ?, description = NULLIF(?, ''),
			geo_selector = ?, operator_id = NULLIF(?, ''), address_set_id = NULLIF(?, ''),
			sort_order = ?, enabled = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, line.ParentID, line.Code, line.Name, line.Description, selector, line.OperatorID, line.AddressSetID,
		line.SortOrder, line.Enabled, line.TenantID, line.ID, expectedVersion)
	if err != nil {
		return GeoLine{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return GeoLine{}, ErrAddressTaxonomyConflict
	}
	return s.GetGeoLine(ctx, line.TenantID, line.ID)
}

func (s *MySQLStore) validateGeoLineReferences(ctx context.Context, line GeoLine) error {
	seen := map[ID]bool{line.ID: true}
	for parentID, depth := line.ParentID, 0; parentID != ""; depth++ {
		if depth >= 32 || seen[parentID] {
			return ErrAddressTaxonomyCycle
		}
		seen[parentID] = true
		parent, err := s.GetGeoLine(ctx, line.TenantID, parentID)
		if err != nil {
			return err
		}
		parentID = parent.ParentID
	}
	for _, id := range line.GeoSelector.GeoNodeIDs {
		if _, err := s.GetGeoDictionary(ctx, line.TenantID, id); err != nil {
			return fmt.Errorf("geo node %s: %w", id, err)
		}
	}
	if line.OperatorID != "" {
		if _, err := s.GetISPOperator(ctx, line.TenantID, line.OperatorID); err != nil {
			return fmt.Errorf("operator %s: %w", line.OperatorID, err)
		}
	}
	if line.AddressSetID != "" {
		if _, err := s.GetAddressSet(ctx, line.TenantID, line.AddressSetID); err != nil {
			return fmt.Errorf("address set %s: %w", line.AddressSetID, err)
		}
	}
	return nil
}

func (s *MySQLStore) DeleteGeoLine(ctx context.Context, tenantID, id ID, expectedVersion uint64) error {
	return s.deleteAddressTaxonomyRow(ctx, "geo_lines", tenantID, id, expectedVersion, `
		SELECT EXISTS(SELECT 1 FROM geo_lines WHERE tenant_id = ? AND parent_id = ?)
	`, tenantID, id)
}

func (s *MySQLStore) deleteAddressTaxonomyRow(ctx context.Context, table string, tenantID, id ID, expectedVersion uint64, referenceQuery string, referenceArgs ...any) error {
	if expectedVersion == 0 {
		return ErrAddressTaxonomyConflict
	}
	if table != "geo_dict" && table != "isp_operators" && table != "geo_lines" {
		return ErrAddressTaxonomyInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT row_version FROM `+table+` WHERE tenant_id = ? AND id = ? FOR UPDATE`, tenantID, id).Scan(&currentVersion); err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return ErrAddressTaxonomyConflict
	}
	if referenceQuery != "" {
		var referenced bool
		if err := tx.QueryRowContext(ctx, referenceQuery, referenceArgs...).Scan(&referenced); err != nil {
			return err
		}
		if referenced {
			return ErrAddressTaxonomyInUse
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE tenant_id = ? AND id = ?`, tenantID, id); err != nil {
		return err
	}
	return tx.Commit()
}

var _ AddressTaxonomyRepository = (*MySQLStore)(nil)
