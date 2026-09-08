package address

import (
	"sort"
	"strconv"
	"strings"
)

// AddressPrefixMergeGroup is one attribution group of a merge preview: the
// prefixes that share identical geography/operator/ASN/labels/source, their
// input CIDRs, and the coalesced result CIDRs for that same attribution.
type AddressPrefixMergeGroup struct {
	GeoLeafID   ID                `json:"geo_leaf_id,omitempty"`
	OperatorID  ID                `json:"operator_id,omitempty"`
	ASN         *uint32           `json:"asn,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Source      string            `json:"source"`
	InputCIDRs  []string          `json:"input_cidrs"`
	ResultCIDRs []string          `json:"result_cidrs"`
}

// AddressPrefixMergePreview is the plan for an attribution-preserving merge.
type AddressPrefixMergePreview struct {
	InputPrefixes  int                       `json:"input_prefixes"`
	ResultPrefixes int                       `json:"result_prefixes"`
	Groups         []AddressPrefixMergeGroup `json:"groups"`
}

// PreviewAddressPrefixMerge coalesces prefixes that share identical attribution
// — geography, operator, ASN, labels, and source — merging only the
// adjacent/overlapping CIDRs within each attribution group. Coverage and
// attribution are both preserved: prefixes with any differing attribute never
// merge together, so applying the result is lossless. This is the difference
// from a plain CIDR "cover"/"normalize", which would blend or drop attribution.
func PreviewAddressPrefixMerge(prefixes []AddressPrefix) (AddressPrefixMergePreview, error) {
	type group struct {
		meta  AddressPrefixMergeGroup
		cidrs []string
	}
	order := make([]string, 0)
	groups := make(map[string]*group)
	for _, prefix := range prefixes {
		key := addressPrefixAttributionKey(prefix)
		existing, ok := groups[key]
		if !ok {
			existing = &group{meta: AddressPrefixMergeGroup{
				GeoLeafID: prefix.GeoLeafID, OperatorID: prefix.OperatorID, ASN: prefix.ASN,
				Labels: prefix.Labels, Source: prefix.Source,
			}}
			groups[key] = existing
			order = append(order, key)
		}
		existing.cidrs = append(existing.cidrs, prefix.CIDR)
	}
	preview := AddressPrefixMergePreview{InputPrefixes: len(prefixes)}
	for _, key := range order {
		current := groups[key]
		normalized, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: current.cidrs})
		if err != nil {
			return AddressPrefixMergePreview{}, err
		}
		current.meta.InputCIDRs = current.cidrs
		current.meta.ResultCIDRs = normalized.Result
		preview.Groups = append(preview.Groups, current.meta)
		preview.ResultPrefixes += len(normalized.Result)
	}
	return preview, nil
}

// addressPrefixAttributionKey is a canonical key over every attribute that must
// match for two prefixes to be mergeable. Labels are order-independent.
func addressPrefixAttributionKey(prefix AddressPrefix) string {
	var builder strings.Builder
	builder.WriteString(string(prefix.GeoLeafID))
	builder.WriteByte(0)
	builder.WriteString(string(prefix.OperatorID))
	builder.WriteByte(0)
	if prefix.ASN != nil {
		builder.WriteString(strconv.FormatUint(uint64(*prefix.ASN), 10))
	}
	builder.WriteByte(0)
	builder.WriteString(prefix.Source)
	builder.WriteByte(0)
	labelKeys := make([]string, 0, len(prefix.Labels))
	for key := range prefix.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)
	for _, key := range labelKeys {
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(prefix.Labels[key])
		builder.WriteByte(';')
	}
	return builder.String()
}
