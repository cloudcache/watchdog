package address

import (
	"context"
	"fmt"
	"strings"
)

const maxLineExcludeDepth = 16

// ListAddressPrefixesByLine lists the base prefixes of importID that match a
// line/region-group's effective set — its include selector ∪ explicit members,
// minus the effective sets of its exclude lines (resolved recursively). family
// (4/6) and paging apply on top. Corrections are overlaid by the caller.
func (s *Store) ListAddressPrefixesByLine(ctx context.Context, importID, lineID ID, family uint8, limit, offset int) ([]AddressBasePrefix, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	if family != 0 && family != 4 && family != 6 {
		return nil, 0, fmt.Errorf("%w: family must be 4 or 6", ErrAddressImportInvalid)
	}
	line, err := s.GetGeoLine(ctx, lineID)
	if err != nil {
		return nil, 0, err
	}
	predicate, predArgs, err := s.resolveLinePredicate(ctx, line, map[ID]bool{line.ID: true}, 0)
	if err != nil {
		return nil, 0, err
	}
	where := ` WHERE import_id = ?`
	whereArgs := []any{importID}
	if family != 0 {
		where += ` AND family = ?`
		whereArgs = append(whereArgs, family)
	}
	where += ` AND (` + predicate + `)`
	whereArgs = append(whereArgs, predArgs...)

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_base_prefixes`+where, whereArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `SELECT ` + addressBasePrefixColumns + ` FROM address_base_prefixes` + where +
		` ORDER BY family ASC, ip_start ASC, prefix_length ASC, id ASC LIMIT ? OFFSET ?`
	queryArgs := append(append([]any(nil), whereArgs...), limit, offset)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]AddressBasePrefix, 0, limit)
	for rows.Next() {
		item, err := scanAddressBasePrefix(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// resolveLinePredicate builds the SQL boolean for a line's effective set:
//
//	include(line) AND NOT ( resolve(excludeChild) OR ... )
//
// with cycle protection (visited) and a depth cap so exclusion recursion cannot
// loop or blow the stack.
func (s *Store) resolveLinePredicate(ctx context.Context, line GeoLine, visited map[ID]bool, depth int) (string, []any, error) {
	inc, incArgs, err := s.includePredicate(ctx, line)
	if err != nil {
		return "", nil, err
	}
	if len(line.ExcludeLineIDs) == 0 || depth >= maxLineExcludeDepth {
		return inc, incArgs, nil
	}
	excludeParts := []string{}
	excludeArgs := []any{}
	for _, id := range line.ExcludeLineIDs {
		if visited[id] {
			continue
		}
		child, err := s.GetGeoLine(ctx, id)
		if err != nil {
			return "", nil, err
		}
		nextVisited := make(map[ID]bool, len(visited)+1)
		for k := range visited {
			nextVisited[k] = true
		}
		nextVisited[id] = true
		childPred, childArgs, err := s.resolveLinePredicate(ctx, child, nextVisited, depth+1)
		if err != nil {
			return "", nil, err
		}
		excludeParts = append(excludeParts, childPred)
		excludeArgs = append(excludeArgs, childArgs...)
	}
	if len(excludeParts) == 0 {
		return inc, incArgs, nil
	}
	return "(" + inc + ") AND NOT (" + strings.Join(excludeParts, " OR ") + ")", append(incArgs, excludeArgs...), nil
}

// includePredicate builds "(family AND geo AND operator AND asn) OR members" for
// one line — empty dimensions omitted, dimensions AND'd, members unioned. A line
// with no include dimensions and no members matches nothing (1=0).
func (s *Store) includePredicate(ctx context.Context, line GeoLine) (string, []any, error) {
	sel := line.GeoSelector
	hasDims := len(sel.GeoNodeIDs) > 0 || len(sel.OperatorIDs) > 0 || len(sel.ASNs) > 0
	var selectorSQL string
	var selectorArgs []any
	if hasDims {
		parts := []string{}
		if len(sel.Families) > 0 {
			ph := make([]string, len(sel.Families))
			for i, family := range sel.Families {
				ph[i] = "?"
				selectorArgs = append(selectorArgs, family)
			}
			parts = append(parts, "family IN ("+strings.Join(ph, ",")+")")
		}
		if len(sel.GeoNodeIDs) > 0 {
			geoSQL, geoArgs, err := s.geoNodesPredicate(ctx, sel.GeoNodeIDs)
			if err != nil {
				return "", nil, err
			}
			if geoSQL == "" {
				geoSQL = "1=0"
			}
			parts = append(parts, geoSQL)
			selectorArgs = append(selectorArgs, geoArgs...)
		}
		if len(sel.OperatorIDs) > 0 {
			opSQL, opArgs, err := s.operatorNamesPredicate(ctx, sel.OperatorIDs)
			if err != nil {
				return "", nil, err
			}
			if opSQL == "" {
				opSQL = "1=0"
			}
			parts = append(parts, opSQL)
			selectorArgs = append(selectorArgs, opArgs...)
		}
		if len(sel.ASNs) > 0 {
			ph := make([]string, len(sel.ASNs))
			for i, asn := range sel.ASNs {
				ph[i] = "?"
				selectorArgs = append(selectorArgs, asn)
			}
			parts = append(parts, "asn IN ("+strings.Join(ph, ",")+")")
		}
		selectorSQL = "(" + strings.Join(parts, " AND ") + ")"
	}
	var membersSQL string
	var membersArgs []any
	if len(line.Members) > 0 {
		ph := make([]string, len(line.Members))
		for i, member := range line.Members {
			ph[i] = "?"
			membersArgs = append(membersArgs, member)
		}
		membersSQL = "cidr IN (" + strings.Join(ph, ",") + ")"
	}
	switch {
	case selectorSQL != "" && membersSQL != "":
		return "(" + selectorSQL + " OR " + membersSQL + ")", append(selectorArgs, membersArgs...), nil
	case selectorSQL != "":
		return selectorSQL, selectorArgs, nil
	case membersSQL != "":
		return membersSQL, membersArgs, nil
	default:
		return "1=0", nil, nil
	}
}

// geoNodesPredicate resolves geo_dict node ids to a predicate over the base
// table's denormalized columns: continents expand to their country codes,
// countries match country_code, provinces/cities match by name.
func (s *Store) geoNodesPredicate(ctx context.Context, nodeIDs []ID) (string, []any, error) {
	ph := make([]string, len(nodeIDs))
	idArgs := make([]any, len(nodeIDs))
	for i, id := range nodeIDs {
		ph[i] = "?"
		idArgs[i] = string(id)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, kind, code, name FROM geo_dict WHERE id IN (`+strings.Join(ph, ",")+`)`, idArgs...)
	if err != nil {
		return "", nil, err
	}
	var countryCodes, subdivisions, cities []string
	var continentIDs []string
	for rows.Next() {
		var id, kind, code, name string
		if err := rows.Scan(&id, &kind, &code, &name); err != nil {
			rows.Close()
			return "", nil, err
		}
		switch kind {
		case GeoKindContinent:
			continentIDs = append(continentIDs, id)
		case GeoKindCountry:
			countryCodes = append(countryCodes, strings.ToUpper(code))
		case GeoKindProvince, GeoKindRegion:
			subdivisions = append(subdivisions, name)
		case GeoKindCity:
			cities = append(cities, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	for _, cid := range continentIDs {
		crows, err := s.db.QueryContext(ctx, `SELECT code FROM geo_dict WHERE parent_id = ? AND kind = ?`, cid, GeoKindCountry)
		if err != nil {
			return "", nil, err
		}
		for crows.Next() {
			var code string
			if err := crows.Scan(&code); err != nil {
				crows.Close()
				return "", nil, err
			}
			countryCodes = append(countryCodes, strings.ToUpper(code))
		}
		crows.Close()
		if err := crows.Err(); err != nil {
			return "", nil, err
		}
	}
	parts := []string{}
	args := []any{}
	appendIn := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, len(values))
		for i, value := range values {
			placeholders[i] = "?"
			args = append(args, value)
		}
		parts = append(parts, column+" IN ("+strings.Join(placeholders, ",")+")")
	}
	appendIn("country_code", countryCodes)
	appendIn("subdivision_name", subdivisions)
	appendIn("city_name", cities)
	if len(parts) == 0 {
		return "", nil, nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args, nil
}

// operatorNamesPredicate resolves operator ids to an operator_name IN (...)
// predicate (the base table denormalizes operator_name).
func (s *Store) operatorNamesPredicate(ctx context.Context, operatorIDs []ID) (string, []any, error) {
	names := make([]string, 0, len(operatorIDs))
	for _, id := range operatorIDs {
		op, err := s.GetISPOperator(ctx, id)
		if err != nil {
			return "", nil, err
		}
		names = append(names, op.Name)
	}
	if len(names) == 0 {
		return "", nil, nil
	}
	ph := make([]string, len(names))
	args := make([]any, len(names))
	for i, name := range names {
		ph[i] = "?"
		args[i] = name
	}
	return "operator_name IN (" + strings.Join(ph, ",") + ")", args, nil
}
