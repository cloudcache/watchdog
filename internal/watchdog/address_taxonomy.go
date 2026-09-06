package watchdog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	GeoKindContinent = "continent"
	GeoKindRegion    = "region"
	GeoKindCountry   = "country"
	GeoKindProvince  = "province"
	GeoKindCity      = "city"
)

var (
	ErrAddressTaxonomyInvalid  = errors.New("address taxonomy is invalid")
	ErrAddressTaxonomyConflict = errors.New("address taxonomy version conflict")
	ErrAddressTaxonomyCycle    = errors.New("address taxonomy hierarchy contains a cycle")
	ErrAddressTaxonomyInUse    = errors.New("address taxonomy item is in use")
)

type GeoDictionaryNode struct {
	ID         ID        `json:"id"`
	TenantID   ID        `json:"tenant_id"`
	Kind       string    `json:"kind"`
	Code       string    `json:"code"`
	ParentID   ID        `json:"parent_id,omitempty"`
	Name       string    `json:"name"`
	ShortName  string    `json:"short_name,omitempty"`
	SortOrder  int       `json:"sort_order"`
	Enabled    bool      `json:"enabled"`
	RowVersion uint64    `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ISPOperator struct {
	ID         ID        `json:"id"`
	TenantID   ID        `json:"tenant_id"`
	FlowISPID  uint16    `json:"flow_isp_id"`
	Code       string    `json:"code"`
	Name       string    `json:"name"`
	ShortName  string    `json:"short_name,omitempty"`
	Category   string    `json:"category"`
	ASNs       []uint32  `json:"asns"`
	SortOrder  int       `json:"sort_order"`
	Enabled    bool      `json:"enabled"`
	RowVersion uint64    `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type GeoLineSelector struct {
	GeoNodeIDs []ID  `json:"geo_node_ids,omitempty"`
	Families   []int `json:"families,omitempty"`
}

type GeoLine struct {
	ID           ID              `json:"id"`
	TenantID     ID              `json:"tenant_id"`
	ParentID     ID              `json:"parent_id,omitempty"`
	Code         string          `json:"code"`
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	GeoSelector  GeoLineSelector `json:"geo_selector"`
	OperatorID   ID              `json:"operator_id,omitempty"`
	AddressSetID string          `json:"address_set_id,omitempty"`
	SortOrder    int             `json:"sort_order"`
	Enabled      bool            `json:"enabled"`
	RowVersion   uint64          `json:"row_version"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type AddressTaxonomyListFilter struct {
	Search   string
	Kind     string
	ParentID ID
	Enabled  *bool
	Limit    int
	Cursor   string
}

type AddressTaxonomyRepository interface {
	ListGeoDictionary(context.Context, ID, AddressTaxonomyListFilter) ([]GeoDictionaryNode, string, error)
	GetGeoDictionary(context.Context, ID, ID) (GeoDictionaryNode, error)
	CreateGeoDictionary(context.Context, GeoDictionaryNode) (GeoDictionaryNode, error)
	UpdateGeoDictionary(context.Context, GeoDictionaryNode, uint64) (GeoDictionaryNode, error)
	DeleteGeoDictionary(context.Context, ID, ID, uint64) error
	ListISPOperators(context.Context, ID, AddressTaxonomyListFilter) ([]ISPOperator, string, error)
	GetISPOperator(context.Context, ID, ID) (ISPOperator, error)
	CreateISPOperator(context.Context, ISPOperator) (ISPOperator, error)
	UpdateISPOperator(context.Context, ISPOperator, uint64) (ISPOperator, error)
	DeleteISPOperator(context.Context, ID, ID, uint64) error
	ListGeoLines(context.Context, ID, AddressTaxonomyListFilter) ([]GeoLine, string, error)
	GetGeoLine(context.Context, ID, ID) (GeoLine, error)
	CreateGeoLine(context.Context, GeoLine) (GeoLine, error)
	UpdateGeoLine(context.Context, GeoLine, uint64) (GeoLine, error)
	DeleteGeoLine(context.Context, ID, ID, uint64) error
}

func normalizeGeoDictionaryNode(node GeoDictionaryNode) (GeoDictionaryNode, error) {
	node.Kind = strings.ToLower(strings.TrimSpace(node.Kind))
	node.Code = strings.TrimSpace(node.Code)
	node.Name = strings.TrimSpace(node.Name)
	node.ShortName = strings.TrimSpace(node.ShortName)
	if !validGeoKind(node.Kind) || node.Code == "" || len(node.Code) > 64 || node.Name == "" || len(node.Name) > 190 || len(node.ShortName) > 190 {
		return GeoDictionaryNode{}, fmt.Errorf("%w: kind, code, or name", ErrAddressTaxonomyInvalid)
	}
	return node, nil
}

func validGeoKind(kind string) bool {
	switch kind {
	case GeoKindContinent, GeoKindRegion, GeoKindCountry, GeoKindProvince, GeoKindCity:
		return true
	default:
		return false
	}
}

func validGeoParentKind(parent, child string) bool {
	parentOrder := map[string]int{GeoKindContinent: 0, GeoKindRegion: 1, GeoKindCountry: 2, GeoKindProvince: 3, GeoKindCity: 4}
	p, parentOK := parentOrder[parent]
	c, childOK := parentOrder[child]
	return parentOK && childOK && p < c
}

func normalizeISPOperator(operator ISPOperator) (ISPOperator, error) {
	operator.Code = strings.TrimSpace(operator.Code)
	operator.Name = strings.TrimSpace(operator.Name)
	operator.ShortName = strings.TrimSpace(operator.ShortName)
	operator.Category = strings.ToLower(strings.TrimSpace(operator.Category))
	if operator.Category == "" {
		operator.Category = "other"
	}
	if operator.Code == "" || len(operator.Code) > 64 || operator.Name == "" || len(operator.Name) > 190 || len(operator.ShortName) > 190 || len(operator.Category) > 32 {
		return ISPOperator{}, fmt.Errorf("%w: operator code, name, or category", ErrAddressTaxonomyInvalid)
	}
	if len(operator.ASNs) > 10_000 {
		return ISPOperator{}, fmt.Errorf("%w: operator ASN list exceeds 10000", ErrAddressTaxonomyInvalid)
	}
	sort.Slice(operator.ASNs, func(i, j int) bool { return operator.ASNs[i] < operator.ASNs[j] })
	result := operator.ASNs[:0]
	for _, asn := range operator.ASNs {
		if asn == 0 {
			return ISPOperator{}, fmt.Errorf("%w: ASN zero is reserved for unknown", ErrAddressTaxonomyInvalid)
		}
		if len(result) == 0 || result[len(result)-1] != asn {
			result = append(result, asn)
		}
	}
	operator.ASNs = result
	return operator, nil
}

func normalizeGeoLine(line GeoLine) (GeoLine, error) {
	line.Code = strings.TrimSpace(line.Code)
	line.Name = strings.TrimSpace(line.Name)
	line.Description = strings.TrimSpace(line.Description)
	if line.Code == "" || len(line.Code) > 64 || line.Name == "" || len(line.Name) > 190 || len(line.Description) > 65_535 {
		return GeoLine{}, fmt.Errorf("%w: line code, name, or description", ErrAddressTaxonomyInvalid)
	}
	if len(line.GeoSelector.GeoNodeIDs) > 1_000 {
		return GeoLine{}, fmt.Errorf("%w: line geo selector exceeds 1000 nodes", ErrAddressTaxonomyInvalid)
	}
	ids := append([]ID(nil), line.GeoSelector.GeoNodeIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	line.GeoSelector.GeoNodeIDs = ids[:0]
	for _, id := range ids {
		if id == "" {
			return GeoLine{}, fmt.Errorf("%w: empty geo node id", ErrAddressTaxonomyInvalid)
		}
		if len(line.GeoSelector.GeoNodeIDs) == 0 || line.GeoSelector.GeoNodeIDs[len(line.GeoSelector.GeoNodeIDs)-1] != id {
			line.GeoSelector.GeoNodeIDs = append(line.GeoSelector.GeoNodeIDs, id)
		}
	}
	families := append([]int(nil), line.GeoSelector.Families...)
	sort.Ints(families)
	line.GeoSelector.Families = families[:0]
	for _, family := range families {
		if family != 4 && family != 6 {
			return GeoLine{}, fmt.Errorf("%w: line family must be 4 or 6", ErrAddressTaxonomyInvalid)
		}
		if len(line.GeoSelector.Families) == 0 || line.GeoSelector.Families[len(line.GeoSelector.Families)-1] != family {
			line.GeoSelector.Families = append(line.GeoSelector.Families, family)
		}
	}
	return line, nil
}
