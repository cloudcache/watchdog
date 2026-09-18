// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import "net/netip"

func isIPDimension(dimension Dimension) bool {
	return dimension == DimensionSourceIP || dimension == DimensionDestinationIP
}

// resultDimensionValue keeps ClickHouse's IPv6 storage representation out of
// the API contract. IPv4 is stored in IPv6 columns as ::ffff:a.b.c.d, while
// callers must see and submit the canonical a.b.c.d representation.
func resultDimensionValue(dimension Dimension, value string) (string, error) {
	if !isIPDimension(dimension) || value == "_other" || value == "_unassigned" {
		return value, nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil || address.Zone() != "" {
		return "", requestError("dimension_value", ErrorInvalid, "IP dimension result is not an IPv4 or IPv6 address")
	}
	return address.Unmap().String(), nil
}

// storageDimensionValues performs the inverse conversion for exact rollup
// filters. This makes an API value returned as a.b.c.d match the persisted
// ::ffff:a.b.c.d dimension without changing the ClickHouse storage schema.
func storageDimensionValues(dimension Dimension, values []string) ([]string, error) {
	if !isIPDimension(dimension) || len(values) == 0 {
		return values, nil
	}
	result := make([]string, len(values))
	for index, value := range values {
		if value == "_other" || value == "_unassigned" {
			result[index] = value
			continue
		}
		address, err := netip.ParseAddr(value)
		if err != nil || address.Zone() != "" {
			return nil, requestError("filters.dimension_values", ErrorInvalid, "IP dimension filters must contain IPv4 or IPv6 addresses")
		}
		address = address.Unmap()
		if address.Is4() {
			address = netip.AddrFrom16(address.As16())
		}
		result[index] = address.String()
	}
	return result, nil
}
