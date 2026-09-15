package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/gin-gonic/gin"
)

// listEffectiveAddressPrefixes returns the active base library (a source-import
// slot) overlaid with editable corrections — the "effective view" workbench:
// each row is the base value unless an address_prefixes correction overrides
// its operator/geo/asn, marked with source=base|correction.
func (s *Server) listEffectiveAddressPrefixes(c *gin.Context) {
	ctx := c.Request.Context()
	slot := strings.TrimSpace(c.DefaultQuery("slot", "combined"))
	active, err := s.addressStore.GetAddressImportSlot(ctx, slot)
	if err != nil || active.ImportID == "" {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0, "limit": 0, "offset": 0})
		return
	}
	limit, offset := pageParams(c)
	filter := address.AddressBasePrefixFilter{
		CountryCode: strings.TrimSpace(c.Query("country_code")), Operator: strings.TrimSpace(c.Query("operator")),
		Search: strings.TrimSpace(c.Query("q")), Limit: limit, Offset: offset, TableMode: true,
		Sort: c.Query("sort"), Desc: sortDirection(c) == "DESC",
	}
	if raw := strings.TrimSpace(c.Query("family")); raw != "" {
		family, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || (family != 4 && family != 6) {
			fail(c, http.StatusBadRequest, "invalid_request", "family must be 4 or 6")
			return
		}
		filter.Family = uint8(family)
	}
	if raw := strings.TrimSpace(c.Query("asn")); raw != "" {
		asn, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "asn must be an unsigned 32-bit integer")
			return
		}
		value := uint32(asn)
		filter.ASN = &value
	}
	base, _, total, err := s.addressStore.ListAddressBasePrefixes(ctx, active.ImportID, filter)
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	cidrs := make([]string, len(base))
	for i := range base {
		cidrs[i] = base[i].CIDR
	}
	corrections, err := s.addressStore.LookupAddressPrefixCorrections(ctx, cidrs)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	items := make([]gin.H, len(base))
	for i := range base {
		row := base[i]
		item := gin.H{
			"cidr": row.CIDR, "family": row.Family, "prefix_length": row.PrefixLength,
			"country_code": row.CountryCode, "country_name": row.CountryName,
			"subdivision_name": row.SubdivisionName, "city_name": row.CityName,
			"asn": row.ASN, "operator_name": row.OperatorName, "source": "base",
		}
		if correction, ok := corrections[row.CIDR]; ok {
			item["source"] = "correction"
			if correction.OperatorName != "" {
				item["operator_name"] = correction.OperatorName
			}
			if correction.GeoName != "" {
				item["subdivision_name"] = correction.GeoName
			}
			if correction.ASN != nil {
				item["asn"] = correction.ASN
			}
		}
		items[i] = item
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

// bulkReassignAddressPrefixes upserts corrections over the base for the selected
// CIDRs, overriding operator/geo/asn (an existing correction is updated).
func (s *Server) bulkReassignAddressPrefixes(c *gin.Context) {
	var req struct {
		CIDRs      []string `json:"cidrs"`
		OperatorID string   `json:"operator_id"`
		GeoLeafID  string   `json:"geo_leaf_id"`
		ASN        *uint32  `json:"asn"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if len(req.CIDRs) == 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "cidrs is required")
		return
	}
	if len(req.CIDRs) > 5000 {
		fail(c, http.StatusBadRequest, "invalid_request", "at most 5000 prefixes per bulk reassign")
		return
	}
	applied, err := s.addressStore.BulkReassignAddressPrefixes(c.Request.Context(), req.CIDRs, req.OperatorID, req.GeoLeafID, req.ASN)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "prefix.bulk_reassign", "prefix", strconv.Itoa(applied))
	c.JSON(http.StatusOK, gin.H{"applied": applied})
}
