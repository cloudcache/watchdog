package flowdimension

import (
	"net/netip"
	"strings"
	"time"
)

type BusinessDirection string

const (
	DirectionIn        BusinessDirection = "in"
	DirectionOut       BusinessDirection = "out"
	DirectionInternal  BusinessDirection = "internal"
	DirectionTransit   BusinessDirection = "transit"
	DirectionAmbiguous BusinessDirection = "ambiguous"
	DirectionBoth      BusinessDirection = "both"
)

type EndpointSide string

const (
	EndpointNone EndpointSide = "none"
	EndpointSrc  EndpointSide = "src"
	EndpointDst  EndpointSide = "dst"
)

type EndpointDimension struct {
	IP          netip.Addr
	Side        EndpointSide
	PrefixID    string
	PrefixCIDR  string
	AddressSets AddressSetMembership
}

// AddressSetMembership exposes an immutable, compile-time membership list.
// Count/At are allocation-free on the per-record classification path. IDs is
// reserved for persistence boundaries that need an owned slice.
type AddressSetMembership struct {
	ids []string
}

func (m AddressSetMembership) Count() int {
	return len(m.ids)
}

func (m AddressSetMembership) At(index int) (string, bool) {
	if index < 0 || index >= len(m.ids) {
		return "", false
	}
	return m.ids[index], true
}

func (m AddressSetMembership) IDs() []string {
	return append([]string(nil), m.ids...)
}

// AppendTo copies the immutable membership IDs into destination. Persistence
// boundaries can reuse destination capacity instead of allocating one slice
// per flow record.
func (m AddressSetMembership) AppendTo(destination []string) []string {
	return append(destination, m.ids...)
}

type ClassifiedEndpoints struct {
	SnapshotID string
	Version    uint64
	Direction  BusinessDirection
	Business   string
	Local      EndpointDimension
	Remote     EndpointDimension
}

func (s *CompiledSnapshot) ClassifyEndpoints(source, destination netip.Addr) ClassifiedEndpoints {
	result := ClassifiedEndpoints{Direction: DirectionAmbiguous, Business: UnassignedDimensionID}
	if s == nil {
		return result
	}
	result.SnapshotID = s.metadata.SnapshotID
	result.Version = s.metadata.Version
	if !source.IsValid() || !destination.IsValid() {
		return result
	}
	source = source.Unmap()
	destination = destination.Unmap()
	sourcePrefix, sourceMatched := s.lookup(source)
	destinationPrefix, destinationMatched := s.lookup(destination)
	sourceLocal := sourceMatched && sourcePrefix.labels["flow"] == "local"
	destinationLocal := destinationMatched && destinationPrefix.labels["flow"] == "local"

	switch {
	case sourceLocal && !destinationLocal:
		result.Direction = DirectionOut
		result.Local = s.endpoint(source, EndpointSrc, sourcePrefix, sourceMatched, result.Direction)
		result.Remote = s.endpoint(destination, EndpointDst, destinationPrefix, destinationMatched, result.Direction)
		result.Business = businessLabel(sourcePrefix)
	case !sourceLocal && destinationLocal:
		result.Direction = DirectionIn
		result.Local = s.endpoint(destination, EndpointDst, destinationPrefix, destinationMatched, result.Direction)
		result.Remote = s.endpoint(source, EndpointSrc, sourcePrefix, sourceMatched, result.Direction)
		result.Business = businessLabel(destinationPrefix)
	case sourceLocal && destinationLocal:
		result.Direction = DirectionInternal
		result.Local = s.endpoint(source, EndpointSrc, sourcePrefix, sourceMatched, result.Direction)
		result.Remote = s.endpoint(destination, EndpointDst, destinationPrefix, destinationMatched, result.Direction)
		result.Business = businessLabel(sourcePrefix)
	default:
		result.Direction = DirectionTransit
	}
	return result
}

func (s *CompiledSnapshot) endpoint(ip netip.Addr, side EndpointSide, prefix compiledPrefix, matched bool, direction BusinessDirection) EndpointDimension {
	endpoint := EndpointDimension{IP: ip, Side: side, PrefixID: UnassignedDimensionID}
	if matched {
		endpoint.PrefixID = prefix.id
		endpoint.PrefixCIDR = prefix.cidr
	}
	if s.addressSets != nil {
		if membership, membershipMatched := s.addressSets.Lookup(ip); membershipMatched {
			switch direction {
			case DirectionIn:
				endpoint.AddressSets.ids = membership.in
			case DirectionOut:
				endpoint.AddressSets.ids = membership.out
			}
		}
	}
	return endpoint
}

func businessLabel(prefix compiledPrefix) string {
	if business := strings.TrimSpace(prefix.labels["business"]); business != "" {
		return business
	}
	return UnassignedDimensionID
}

func (c *SnapshotCatalog) ClassifyAt(tenantID string, eventTime time.Time, source, destination netip.Addr) (ClassifiedEndpoints, error) {
	snapshot, err := c.Select(tenantID, eventTime)
	if err != nil {
		return ClassifiedEndpoints{}, err
	}
	return snapshot.ClassifyEndpoints(source, destination), nil
}

type Category string

const (
	CategoryOnNetLocalCity      Category = "on_net_local_city"
	CategoryOnNetCrossCity      Category = "on_net_cross_city"
	CategoryOnNetCrossProvince  Category = "on_net_cross_province"
	CategoryOffNetInProvince    Category = "off_net_in_province"
	CategoryOffNetCrossProvince Category = "off_net_cross_province"
	CategoryOverseas            Category = "overseas"
	CategoryUnknown             Category = "unknown"
	CategoryInternal            Category = "internal"
	CategoryTransit             Category = "transit"
	CategoryAmbiguous           Category = "ambiguous"
)

type HomeProfile struct {
	Province            string
	City                string
	ISPIDs              map[uint16]struct{}
	ASNs                map[uint32]struct{}
	OverseasIncludesHMT bool
	Version             uint32
}

type GeoInfo struct {
	Country     string
	AdminCode   string
	Subdivision string
	City        string
	ContinentID string
	RegionID    string
	CountryID   string
	ProvinceID  string
	CityID      string
	ISPID       uint16
	ASN         uint32
	Version     string
	Source      string
}

func ClassifyCategory(direction BusinessDirection, remote GeoInfo, home HomeProfile) Category {
	switch direction {
	case DirectionInternal:
		return CategoryInternal
	case DirectionTransit:
		return CategoryTransit
	case DirectionAmbiguous:
		return CategoryAmbiguous
	case DirectionIn, DirectionOut:
	default:
		return CategoryUnknown
	}
	country, adminCode, isHMT := normalizeHMT(remote.Country, remote.AdminCode)
	if !validCountryCode(country) {
		return CategoryUnknown
	}
	if country != "CN" || (isHMT && home.OverseasIncludesHMT) {
		return CategoryOverseas
	}
	province, ok := provincePart(adminCode)
	homeProvince, homeOK := provincePart(home.Province)
	if !ok || !homeOK {
		return CategoryUnknown
	}
	identityAvailable := false
	onNet := false
	if remote.ISPID != 0 && len(home.ISPIDs) > 0 {
		identityAvailable = true
		_, onNet = home.ISPIDs[remote.ISPID]
	}
	if remote.ASN != 0 && len(home.ASNs) > 0 {
		identityAvailable = true
		if _, matched := home.ASNs[remote.ASN]; matched {
			onNet = true
		}
	}
	if !identityAvailable {
		return CategoryUnknown
	}
	if !onNet {
		if province == homeProvince {
			return CategoryOffNetInProvince
		}
		return CategoryOffNetCrossProvince
	}
	if province != homeProvince {
		return CategoryOnNetCrossProvince
	}
	remoteCity, remoteCityOK := cityPart(adminCode)
	homeCity, homeCityOK := cityPart(home.City)
	if !remoteCityOK || !homeCityOK {
		return CategoryUnknown
	}
	if remoteCity == homeCity {
		return CategoryOnNetLocalCity
	}
	return CategoryOnNetCrossCity
}

func normalizeHMT(country, adminCode string) (string, string, bool) {
	country = strings.ToUpper(strings.TrimSpace(country))
	adminCode = strings.TrimSpace(adminCode)
	prefix := ""
	switch country {
	case "HK":
		prefix = "81"
	case "MO":
		prefix = "82"
	case "TW":
		prefix = "71"
	}
	if prefix != "" {
		country = "CN"
		if len(adminCode) != 6 || !allDigits(adminCode) || !strings.HasPrefix(adminCode, prefix) {
			adminCode = prefix + "0000"
		}
	}
	isHMT := len(adminCode) == 6 && (strings.HasPrefix(adminCode, "71") || strings.HasPrefix(adminCode, "81") || strings.HasPrefix(adminCode, "82"))
	return country, adminCode, isHMT
}

func provincePart(value string) (string, bool) {
	if len(value) != 6 || !allDigits(value) || value[:2] == "00" {
		return "", false
	}
	return value[:2], true
}

func cityPart(value string) (string, bool) {
	if len(value) != 6 || !allDigits(value) || value[2:4] == "00" {
		return "", false
	}
	return value[:4], true
}

func allDigits(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func validCountryCode(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'Z' && value[1] >= 'A' && value[1] <= 'Z'
}
