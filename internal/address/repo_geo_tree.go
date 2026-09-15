package address

import "context"

// GeoTreeNode is a geo_dict node plus its direct child count, for lazily
// drilling the virtual hierarchy tree (continent → country → province → city).
type GeoTreeNode struct {
	ID         ID     `json:"id"`
	Kind       string `json:"kind"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	ParentID   ID     `json:"parent_id,omitempty"`
	ChildCount int    `json:"child_count"`
}

// GeoTreeChildren returns the geo_dict children of parentID — or the root
// continents when parentID is empty — each carrying how many children it has so
// the UI can show an expand affordance without a second round trip. Pure
// dictionary traversal (geo_dict is ~600 rows); it never scans the address base.
func (s *Store) GeoTreeChildren(ctx context.Context, parentID ID) ([]GeoTreeNode, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.id, c.kind, c.code, c.name, COALESCE(c.parent_id, ''),
			(SELECT COUNT(*) FROM geo_dict g WHERE g.parent_id = c.id)
		FROM geo_dict c
		WHERE c.parent_id <=> NULLIF(?, '')
		ORDER BY c.sort_order, c.name, c.id`, string(parentID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []GeoTreeNode{}
	for rows.Next() {
		var node GeoTreeNode
		if err := rows.Scan(&node.ID, &node.Kind, &node.Code, &node.Name, &node.ParentID, &node.ChildCount); err != nil {
			return nil, err
		}
		out = append(out, node)
	}
	return out, rows.Err()
}
