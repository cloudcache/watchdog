package watchdog

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	ipdb "github.com/ipipdotnet/ipdb-go"
	maxminddb "github.com/oschwald/maxminddb-golang/v2"
)

type AddressImportRecord struct {
	Prefix          string   `json:"prefix"`
	ContinentCode   string   `json:"continent_code,omitempty"`
	CountryCode     string   `json:"country_code,omitempty"`
	CountryName     string   `json:"country_name,omitempty"`
	SubdivisionCode string   `json:"subdivision_code,omitempty"`
	SubdivisionName string   `json:"subdivision_name,omitempty"`
	CityCode        string   `json:"city_code,omitempty"`
	CityName        string   `json:"city_name,omitempty"`
	ASN             uint32   `json:"asn,omitempty"`
	Operator        string   `json:"operator,omitempty"`
	Latitude        *float64 `json:"latitude,omitempty"`
	Longitude       *float64 `json:"longitude,omitempty"`
	Source          string   `json:"source"`
}

type AddressImportMetadata struct {
	Format       string    `json:"format"`
	DatabaseType string    `json:"database_type,omitempty"`
	BuildTime    time.Time `json:"build_time"`
	IPVersion    uint      `json:"ip_version"`
	Languages    []string  `json:"languages,omitempty"`
	Fields       []string  `json:"fields,omitempty"`
}

type mmdbImportWire struct {
	Continent struct {
		Code string `maxminddb:"code"`
	} `maxminddb:"continent"`
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	Subdivisions []struct {
		ISOCode   string            `maxminddb:"iso_code"`
		GeoNameID uint32            `maxminddb:"geoname_id"`
		Names     map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	City struct {
		GeoNameID uint32            `maxminddb:"geoname_id"`
		Names     map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Location struct {
		Latitude  *float64 `maxminddb:"latitude"`
		Longitude *float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
	AutonomousSystemNumber       uint32 `maxminddb:"autonomous_system_number"`
	AutonomousSystemOrganization string `maxminddb:"autonomous_system_organization"`
	ISP                          string `maxminddb:"isp"`
	Organization                 string `maxminddb:"organization"`
}

func StreamMMDB(path string, visit func(AddressImportRecord) error) (AddressImportMetadata, error) {
	if visit == nil {
		return AddressImportMetadata{}, errors.New("MMDB visitor is required")
	}
	database, err := maxminddb.Open(path)
	if err != nil {
		return AddressImportMetadata{}, fmt.Errorf("open MMDB: %w", err)
	}
	defer database.Close()
	metadata := AddressImportMetadata{
		Format: "mmdb", DatabaseType: database.Metadata.DatabaseType,
		BuildTime: database.Metadata.BuildTime().UTC(), IPVersion: database.Metadata.IPVersion,
		Languages: append([]string(nil), database.Metadata.Languages...),
	}
	for result := range database.Networks(maxminddb.SkipEmptyValues()) {
		if err := result.Err(); err != nil {
			return metadata, fmt.Errorf("iterate MMDB: %w", err)
		}
		var wire mmdbImportWire
		if err := result.Decode(&wire); err != nil {
			return metadata, fmt.Errorf("decode MMDB network %s: %w", result.Prefix(), err)
		}
		record := AddressImportRecord{
			Prefix: result.Prefix().Masked().String(), ContinentCode: strings.ToUpper(strings.TrimSpace(wire.Continent.Code)),
			CountryCode: strings.ToUpper(strings.TrimSpace(wire.Country.ISOCode)), CountryName: preferredGeoName(wire.Country.Names),
			ASN: wire.AutonomousSystemNumber, Operator: firstNonEmpty(wire.ISP, wire.Organization, wire.AutonomousSystemOrganization),
			Latitude: wire.Location.Latitude, Longitude: wire.Location.Longitude, Source: "mmdb",
		}
		if len(wire.Subdivisions) != 0 {
			record.SubdivisionCode = strings.TrimSpace(wire.Subdivisions[0].ISOCode)
			record.SubdivisionName = preferredGeoName(wire.Subdivisions[0].Names)
		}
		if wire.City.GeoNameID != 0 || len(wire.City.Names) != 0 {
			record.CityCode = strconv.FormatUint(uint64(wire.City.GeoNameID), 10)
			record.CityName = preferredGeoName(wire.City.Names)
		}
		if err := visit(record); err != nil {
			return metadata, err
		}
	}
	return metadata, nil
}

func preferredGeoName(names map[string]string) string {
	for _, language := range []string{"zh-CN", "en"} {
		if value := strings.TrimSpace(names[language]); value != "" {
			return value
		}
	}
	keys := make([]string, 0, len(names))
	for key := range names {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if value := strings.TrimSpace(names[key]); value != "" {
			return value
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

type ipdbMetadata struct {
	Build     int64          `json:"build"`
	IPVersion uint16         `json:"ip_version"`
	Languages map[string]int `json:"languages"`
	NodeCount int            `json:"node_count"`
	TotalSize int            `json:"total_size"`
	Fields    []string       `json:"fields"`
}

type ipdbNetworkReader struct {
	metadata ipdbMetadata
	data     []byte
	language string
	offset   int
	v4Root   uint32
}

// StreamIPDB validates the file with the official ipipdotnet reader and then
// enumerates its public trie format. The upstream API only exposes point
// lookups; enumerating trie leaves preserves source boundaries and avoids a
// 2^32 address scan.
func StreamIPDB(path, language string, visit func(AddressImportRecord) error) (AddressImportMetadata, error) {
	if visit == nil {
		return AddressImportMetadata{}, errors.New("IPDB visitor is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AddressImportMetadata{}, fmt.Errorf("read IPDB: %w", err)
	}
	official, err := ipdb.NewCityFromBytes(data)
	if err != nil {
		return AddressImportMetadata{}, fmt.Errorf("validate IPDB: %w", err)
	}
	reader, err := newIPDBNetworkReader(data, language)
	if err != nil {
		return AddressImportMetadata{}, err
	}
	languages := official.Languages()
	sort.Strings(languages)
	metadata := AddressImportMetadata{
		Format: "ipdb", DatabaseType: "ipip-city", BuildTime: official.BuildTime().UTC(),
		Languages: languages, Fields: official.Fields(),
	}
	if official.IsIPv6() {
		metadata.IPVersion = 6
	} else if official.IsIPv4() {
		metadata.IPVersion = 4
	}
	if official.IsIPv4() {
		if err := reader.walk(4, reader.v4Root, visit); err != nil {
			return metadata, err
		}
	}
	if official.IsIPv6() {
		if err := reader.walk(6, 0, visit); err != nil {
			return metadata, err
		}
	}
	return metadata, nil
}

func newIPDBNetworkReader(file []byte, language string) (*ipdbNetworkReader, error) {
	if len(file) < 4 {
		return nil, errors.New("IPDB file is shorter than its metadata header")
	}
	metadataLength := int(binary.BigEndian.Uint32(file[:4]))
	if metadataLength <= 0 || metadataLength > len(file)-4 {
		return nil, errors.New("IPDB metadata length is invalid")
	}
	var metadata ipdbMetadata
	if err := json.Unmarshal(file[4:4+metadataLength], &metadata); err != nil {
		return nil, fmt.Errorf("decode IPDB metadata: %w", err)
	}
	data := file[4+metadataLength:]
	if metadata.NodeCount <= 0 || metadata.NodeCount > len(data)/8 {
		return nil, errors.New("IPDB node_count is outside the file")
	}
	if metadata.TotalSize != len(data) || len(metadata.Fields) == 0 || len(metadata.Languages) == 0 {
		return nil, errors.New("IPDB metadata does not match the data section")
	}
	if language == "" {
		for _, candidate := range []string{"CN", "EN"} {
			if _, exists := metadata.Languages[candidate]; exists {
				language = candidate
				break
			}
		}
		if language == "" {
			languages := make([]string, 0, len(metadata.Languages))
			for candidate := range metadata.Languages {
				languages = append(languages, candidate)
			}
			sort.Strings(languages)
			language = languages[0]
		}
	}
	offset, exists := metadata.Languages[language]
	if !exists || offset < 0 {
		return nil, fmt.Errorf("IPDB language %q is not available", language)
	}
	reader := &ipdbNetworkReader{metadata: metadata, data: data, language: language, offset: offset}
	reader.v4Root = 0
	for depth := 0; depth < 96 && int(reader.v4Root) < metadata.NodeCount; depth++ {
		bit := uint8(0)
		if depth >= 80 {
			bit = 1
		}
		next, err := reader.child(reader.v4Root, bit)
		if err != nil {
			return nil, err
		}
		reader.v4Root = next
	}
	return reader, nil
}

type ipdbWalkItem struct {
	node  uint32
	depth int
	base  addressNumber
}

func (reader *ipdbNetworkReader) walk(family uint8, root uint32, visit func(AddressImportRecord) error) error {
	width := 128
	if family == 4 {
		width = 32
	}
	stack := []ipdbWalkItem{{node: root}}
	mapped := addressInterval{family: 6, lo: addressNumber{lo: 0xffff00000000}, hi: addressNumber{lo: 0xffffffffffff}}
	for len(stack) != 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if int(item.node) == reader.metadata.NodeCount {
			continue
		}
		if int(item.node) > reader.metadata.NodeCount {
			values, err := reader.resolve(item.node)
			if err != nil {
				return err
			}
			interval := addressInterval{family: family, lo: item.base, hi: addressBlockEnd(item.base, width-item.depth)}
			intervals := []addressInterval{interval}
			if family == 6 && reader.metadata.IPVersion&1 != 0 {
				intervals = subtractAddressIntervals(intervals, []addressInterval{mapped})
			}
			for _, output := range intervals {
				prefixes, err := addressIntervalsToCIDRs([]addressInterval{output}, MaxAddressOperationResults)
				if err != nil {
					return fmt.Errorf("expand IPDB leaf: %w", err)
				}
				for _, prefix := range prefixes {
					record, err := ipdbValuesToRecord(prefix, reader.metadata.Fields, values)
					if err != nil {
						return fmt.Errorf("decode IPDB network %s: %w", prefix, err)
					}
					if err := visit(record); err != nil {
						return err
					}
				}
			}
			continue
		}
		if item.depth >= width {
			return fmt.Errorf("IPDB trie still references an internal node at IPv%d depth %d", family, item.depth)
		}
		left, err := reader.child(item.node, 0)
		if err != nil {
			return err
		}
		right, err := reader.child(item.node, 1)
		if err != nil {
			return err
		}
		rightBase := item.base
		bitPosition := width - item.depth - 1
		if bitPosition < 64 {
			rightBase.lo |= uint64(1) << bitPosition
		} else {
			rightBase.hi |= uint64(1) << (bitPosition - 64)
		}
		stack = append(stack,
			ipdbWalkItem{node: right, depth: item.depth + 1, base: rightBase},
			ipdbWalkItem{node: left, depth: item.depth + 1, base: item.base},
		)
	}
	return nil
}

func (reader *ipdbNetworkReader) child(node uint32, bit uint8) (uint32, error) {
	if int(node) >= reader.metadata.NodeCount || bit > 1 {
		return 0, errors.New("IPDB trie node is invalid")
	}
	offset := int(node)*8 + int(bit)*4
	if offset < 0 || offset+4 > len(reader.data) {
		return 0, errors.New("IPDB trie node points outside the file")
	}
	return binary.BigEndian.Uint32(reader.data[offset : offset+4]), nil
}

func (reader *ipdbNetworkReader) resolve(node uint32) ([]string, error) {
	offset := int(node) - reader.metadata.NodeCount + reader.metadata.NodeCount*8
	if offset < 0 || offset+2 > len(reader.data) {
		return nil, errors.New("IPDB data pointer is outside the file")
	}
	size := int(binary.BigEndian.Uint16(reader.data[offset : offset+2]))
	if size < 0 || offset+2+size > len(reader.data) {
		return nil, errors.New("IPDB data record is truncated")
	}
	allValues := strings.Split(string(reader.data[offset+2:offset+2+size]), "\t")
	end := reader.offset + len(reader.metadata.Fields)
	if reader.offset > len(allValues) || end > len(allValues) {
		return nil, errors.New("IPDB data record does not contain the selected language fields")
	}
	return allValues[reader.offset:end], nil
}

func ipdbValuesToRecord(prefix string, fields, values []string) (AddressImportRecord, error) {
	data := make(map[string]string, len(fields))
	for index, field := range fields {
		if index < len(values) {
			data[field] = strings.TrimSpace(values[index])
		}
	}
	latitude, err := parseOptionalFloat(data["latitude"])
	if err != nil {
		return AddressImportRecord{}, fmt.Errorf("latitude: %w", err)
	}
	longitude, err := parseOptionalFloat(data["longitude"])
	if err != nil {
		return AddressImportRecord{}, fmt.Errorf("longitude: %w", err)
	}
	asn, err := parseIPDBASN(data["asn"])
	if err != nil {
		return AddressImportRecord{}, fmt.Errorf("asn: %w", err)
	}
	record := AddressImportRecord{
		Prefix: prefix, Source: "ipdb", ContinentCode: strings.ToUpper(data["continent_code"]),
		CountryCode: strings.ToUpper(data["country_code"]), CountryName: data["country_name"],
		SubdivisionCode: firstNonEmpty(data["china_region_code"], provinceFromChinaAdminCode(data["china_admin_code"])),
		SubdivisionName: data["region_name"], CityCode: firstNonEmpty(data["china_city_code"], cityFromChinaAdminCode(data["china_admin_code"])),
		CityName: data["city_name"], Operator: firstNonEmpty(data["isp_domain"], data["owner_domain"]),
		Latitude: latitude, Longitude: longitude, ASN: asn,
	}
	return record, nil
}

func provinceFromChinaAdminCode(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return ""
	}
	return value[:2] + "0000"
}

func cityFromChinaAdminCode(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 4 {
		return ""
	}
	return value[:4] + "00"
}

func parseOptionalFloat(value string) (*float64, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseIPDBASN(value string) (uint32, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	value = strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(value), "AS"))
	if separator := strings.IndexAny(value, ",; /|"); separator >= 0 {
		value = value[:separator]
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

func addressRecordPrefix(record AddressImportRecord) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(record.Prefix)
	if err != nil || prefix != prefix.Masked() {
		return netip.Prefix{}, fmt.Errorf("import record has non-canonical prefix %q", record.Prefix)
	}
	return prefix, nil
}
