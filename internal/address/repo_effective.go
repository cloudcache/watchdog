package address

import (
	"context"
	"strings"
)

// AddressPrefixCorrection is an editable manual override (address_prefixes row)
// overlaid on the immutable base library, with operator/geo resolved to names.
type AddressPrefixCorrection struct {
	CIDR         string
	OperatorName string
	GeoName      string
	ASN          *uint32
	Source       string
}

// LookupAddressPrefixCorrections returns the correction overlay for the given
// CIDRs (keyed by CIDR); only CIDRs that have an editable row are present. Used
// by the effective view to overlay corrections onto a base page.
func (s *Store) LookupAddressPrefixCorrections(ctx context.Context, cidrs []string) (map[string]AddressPrefixCorrection, error) {
	out := make(map[string]AddressPrefixCorrection, len(cidrs))
	if len(cidrs) == 0 {
		return out, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(cidrs)), ",")
	args := make([]any, len(cidrs))
	for i, cidr := range cidrs {
		args[i] = cidr
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.cidr, COALESCE(op.name, ''), COALESCE(gd.name, ''), c.asn, c.source
		FROM address_prefixes c
		LEFT JOIN isp_operators op ON op.id = c.operator_id
		LEFT JOIN geo_dict gd ON gd.id = c.geo_leaf_id
		WHERE c.cidr IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var correction AddressPrefixCorrection
		if err := rows.Scan(&correction.CIDR, &correction.OperatorName, &correction.GeoName, &correction.ASN, &correction.Source); err != nil {
			return nil, err
		}
		out[correction.CIDR] = correction
	}
	return out, rows.Err()
}

// BulkReassignAddressPrefixes upserts a manual correction (source="correction")
// for each CIDR, overriding operator/geo/asn. An existing correction for the
// CIDR is updated in place. Returns the number applied.
func (s *Store) BulkReassignAddressPrefixes(ctx context.Context, cidrs []string, operatorID, geoLeafID ID, asn *uint32) (int, error) {
	if len(cidrs) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	applied := 0
	for _, cidr := range cidrs {
		norm, start, end, err := normalizeAddressPrefix(AddressPrefix{
			CIDR: strings.TrimSpace(cidr), Labels: map[string]string{}, Source: "correction",
			OperatorID: operatorID, GeoLeafID: geoLeafID, ASN: asn,
		})
		if err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO address_prefixes (id, cidr, family, prefix_length, ip_start, ip_end, labels, geo_leaf_id, operator_id, asn, source)
			VALUES (?, ?, ?, ?, ?, ?, '{}', NULLIF(?, ''), NULLIF(?, ''), ?, 'correction')
			ON DUPLICATE KEY UPDATE geo_leaf_id = VALUES(geo_leaf_id), operator_id = VALUES(operator_id),
				asn = VALUES(asn), source = 'correction', row_version = row_version + 1
		`, NewSetID(), norm.CIDR, norm.Family, norm.PrefixLength, start[:], end[:],
			norm.GeoLeafID, norm.OperatorID, norm.ASN); err != nil {
			return 0, err
		}
		applied++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return applied, nil
}
