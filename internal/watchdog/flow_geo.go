package watchdog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// flow-geo-v1 bundle loader (flow-module-design.md §3.3). The bundle is an
// offline export of the EdgeManager merged+corrected interval library;
// watchdog only reads files — no database access, no writeback.

type FlowGeoManifest struct {
	Schema          string                        `json:"schema"`
	Version         string                        `json:"version"`
	GeneratedAt     string                        `json:"generated_at"`
	AdminCodeSystem string                        `json:"admin_code_system"`
	UnknownCountry  string                        `json:"unknown_country"`
	Files           map[string]FlowGeoManifestRef `json:"files"`
}

type FlowGeoManifestRef struct {
	SHA256 string `json:"sha256"`
	Rows   int    `json:"rows"`
}

type FlowGeoOperator struct {
	ID        uint16 `json:"id"`
	Name      string `json:"name"`
	ShortName string `json:"short_name"`
	Category  string `json:"category"`
	Enabled   bool   `json:"enabled"`
}

type FlowGeoDictEntry struct {
	Kind       string `json:"kind"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	ParentCode string `json:"parent_code"`
	Enabled    bool   `json:"enabled"`
}

type GeoInfo struct {
	Country     string
	AdminCode   string
	Subdivision string
	City        string
	ISPID       uint16
	ISPName     string
	ASN         uint32
	Version     string
	Source      string
}

type flowGeoAttrs struct {
	country     string
	adminCode   string
	subdivision string
	city        string
	ispID       uint16
	asn         uint32
}

type flowGeoRangeV4 struct {
	start, end uint32
	attr       int32
}

type flowGeoRangeV6 struct {
	start, end [16]byte
	attr       int32
}

type FlowGeoIndex struct {
	Version     string
	GeneratedAt string
	LoadedAt    time.Time
	RowsV4      int
	RowsV6      int
	v4          []flowGeoRangeV4
	v6          []flowGeoRangeV6
	attrs       []flowGeoAttrs
	operators   map[uint16]FlowGeoOperator
	provinces   map[string]string
}

// LoadFlowGeoBundle reads and fully validates one versioned bundle directory
// (not the `current` symlink target semantics — pass the resolved directory or
// the symlink itself, both work through os file APIs).
func LoadFlowGeoBundle(dir string) (*FlowGeoIndex, error) {
	manifestRaw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var manifest FlowGeoManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if manifest.Schema != "flow-geo-v1" {
		return nil, fmt.Errorf("unsupported schema %q", manifest.Schema)
	}
	if manifest.Version == "" {
		return nil, errors.New("manifest version is required")
	}

	index := &FlowGeoIndex{
		Version:     manifest.Version,
		GeneratedAt: manifest.GeneratedAt,
		LoadedAt:    time.Now().UTC(),
		operators:   map[uint16]FlowGeoOperator{},
		provinces:   map[string]string{},
	}

	operatorsRaw, err := readVerifiedFile(dir, "operators.json", manifest)
	if err != nil {
		return nil, err
	}
	var operators []FlowGeoOperator
	if err := json.Unmarshal(operatorsRaw, &operators); err != nil {
		return nil, fmt.Errorf("parse operators.json: %w", err)
	}
	for _, operator := range operators {
		index.operators[operator.ID] = operator
	}

	dictRaw, err := readVerifiedFile(dir, "geo_dict.json", manifest)
	if err != nil {
		return nil, err
	}
	var dictionary []FlowGeoDictEntry
	if err := json.Unmarshal(dictRaw, &dictionary); err != nil {
		return nil, fmt.Errorf("parse geo_dict.json: %w", err)
	}
	for _, entry := range dictionary {
		if entry.Kind == "province" {
			index.provinces[entry.Code] = entry.Name
		}
	}

	attrCache := map[flowGeoAttrs]int32{}
	intern := func(attr flowGeoAttrs) int32 {
		if id, ok := attrCache[attr]; ok {
			return id
		}
		id := int32(len(index.attrs))
		index.attrs = append(index.attrs, attr)
		attrCache[attr] = id
		return id
	}

	if err := streamGeoCSV(dir, "ipv4.csv.zst", manifest, func(startIP, endIP netip.Addr, attr flowGeoAttrs) error {
		if !startIP.Is4() || !endIP.Is4() {
			return fmt.Errorf("ipv4.csv.zst contains non-IPv4 row %s", startIP)
		}
		start := be32(startIP.As4())
		end := be32(endIP.As4())
		if start > end {
			return fmt.Errorf("inverted interval %s-%s", startIP, endIP)
		}
		if n := len(index.v4); n > 0 && start <= index.v4[n-1].end {
			return fmt.Errorf("unsorted or overlapping interval at %s", startIP)
		}
		index.v4 = append(index.v4, flowGeoRangeV4{start: start, end: end, attr: intern(attr)})
		return nil
	}); err != nil {
		return nil, err
	}
	index.RowsV4 = len(index.v4)

	if err := streamGeoCSV(dir, "ipv6.csv.zst", manifest, func(startIP, endIP netip.Addr, attr flowGeoAttrs) error {
		start := startIP.As16()
		end := endIP.As16()
		if compare16(start, end) > 0 {
			return fmt.Errorf("inverted interval %s-%s", startIP, endIP)
		}
		if n := len(index.v6); n > 0 && compare16(start, index.v6[n-1].end) <= 0 {
			return fmt.Errorf("unsorted or overlapping interval at %s", startIP)
		}
		index.v6 = append(index.v6, flowGeoRangeV6{start: start, end: end, attr: intern(attr)})
		return nil
	}); err != nil {
		return nil, err
	}
	index.RowsV6 = len(index.v6)

	if expected := manifest.Files["ipv4.csv.zst"].Rows; expected != index.RowsV4 {
		return nil, fmt.Errorf("ipv4 row count %d does not match manifest %d", index.RowsV4, expected)
	}
	if expected := manifest.Files["ipv6.csv.zst"].Rows; expected != index.RowsV6 {
		return nil, fmt.Errorf("ipv6 row count %d does not match manifest %d", index.RowsV6, expected)
	}
	if index.RowsV4 == 0 {
		return nil, errors.New("bundle contains no IPv4 intervals")
	}
	return index, nil
}

func readVerifiedFile(dir, name string, manifest FlowGeoManifest) ([]byte, error) {
	ref, ok := manifest.Files[name]
	if !ok {
		return nil, fmt.Errorf("manifest is missing %s", name)
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != strings.ToLower(ref.SHA256) {
		return nil, fmt.Errorf("%s checksum mismatch", name)
	}
	return data, nil
}

func streamGeoCSV(dir, name string, manifest FlowGeoManifest, emit func(start, end netip.Addr, attr flowGeoAttrs) error) error {
	raw, err := readVerifiedFile(dir, name, manifest)
	if err != nil {
		return err
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(raw, nil)
	if err != nil {
		return fmt.Errorf("decompress %s: %w", name, err)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(decoded)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	first := true
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimRight(scanner.Text(), "\r")
		if text == "" {
			continue
		}
		if first {
			first = false
			if !strings.HasPrefix(text, "ip_start,") {
				return fmt.Errorf("%s: unexpected header %q", name, text)
			}
			continue
		}
		fields := strings.Split(text, ",")
		if len(fields) != 8 {
			return fmt.Errorf("%s line %d: expected 8 fields, got %d", name, line, len(fields))
		}
		start, err := netip.ParseAddr(fields[0])
		if err != nil {
			return fmt.Errorf("%s line %d: %w", name, line, err)
		}
		end, err := netip.ParseAddr(fields[1])
		if err != nil {
			return fmt.Errorf("%s line %d: %w", name, line, err)
		}
		ispID, err := strconv.ParseUint(fields[6], 10, 16)
		if err != nil {
			return fmt.Errorf("%s line %d: isp_id: %w", name, line, err)
		}
		asn, err := strconv.ParseUint(fields[7], 10, 32)
		if err != nil {
			return fmt.Errorf("%s line %d: asn: %w", name, line, err)
		}
		attr := flowGeoAttrs{
			country:     fields[2],
			adminCode:   fields[3],
			subdivision: fields[4],
			city:        fields[5],
			ispID:       uint16(ispID),
			asn:         uint32(asn),
		}
		if err := emit(start, end, attr); err != nil {
			return fmt.Errorf("%s line %d: %w", name, line, err)
		}
	}
	return scanner.Err()
}

func be32(b [4]byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func compare16(a, b [16]byte) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Lookup resolves one address against the loaded intervals. IPv4-mapped IPv6
// addresses resolve through the IPv4 table.
func (index *FlowGeoIndex) Lookup(addr netip.Addr) (GeoInfo, bool) {
	if index == nil {
		return GeoInfo{}, false
	}
	addr = addr.Unmap()
	var attrID int32 = -1
	if addr.Is4() {
		value := be32(addr.As4())
		low, high := 0, len(index.v4)-1
		for low <= high {
			mid := (low + high) / 2
			entry := index.v4[mid]
			switch {
			case value < entry.start:
				high = mid - 1
			case value > entry.end:
				low = mid + 1
			default:
				attrID = entry.attr
				low = high + 1
			}
		}
	} else {
		value := addr.As16()
		low, high := 0, len(index.v6)-1
		for low <= high {
			mid := (low + high) / 2
			entry := index.v6[mid]
			switch {
			case compare16(value, entry.start) < 0:
				high = mid - 1
			case compare16(value, entry.end) > 0:
				low = mid + 1
			default:
				attrID = entry.attr
				low = high + 1
			}
		}
	}
	if attrID < 0 {
		return GeoInfo{}, false
	}
	attr := index.attrs[attrID]
	info := GeoInfo{
		Country:     attr.country,
		AdminCode:   attr.adminCode,
		Subdivision: attr.subdivision,
		City:        attr.city,
		ISPID:       attr.ispID,
		ASN:         attr.asn,
		Version:     index.Version,
		Source:      "bundle",
	}
	if operator, ok := index.operators[attr.ispID]; ok {
		info.ISPName = operator.Name
	}
	if info.Subdivision == "" && len(info.AdminCode) == 6 {
		if name, ok := index.provinces[info.AdminCode[:2]+"0000"]; ok {
			info.Subdivision = name
		}
	}
	return info, true
}

// FlowGeoService holds the active index behind an atomic pointer so reloads
// never block lookups and a failed reload keeps the previous version serving.
type FlowGeoService struct {
	Path    string
	current atomic.Pointer[FlowGeoIndex]
	lastErr atomic.Pointer[string]
}

func NewFlowGeoService(path string) *FlowGeoService {
	return &FlowGeoService{Path: path}
}

func (s *FlowGeoService) Reload() error {
	index, err := LoadFlowGeoBundle(s.Path)
	if err != nil {
		message := err.Error()
		s.lastErr.Store(&message)
		return err
	}
	s.current.Store(index)
	s.lastErr.Store(nil)
	return nil
}

func (s *FlowGeoService) Index() *FlowGeoIndex {
	if s == nil {
		return nil
	}
	return s.current.Load()
}

type FlowGeoStatus struct {
	Path        string
	Loaded      bool
	Version     string
	GeneratedAt string
	LoadedAt    time.Time
	RowsV4      int
	RowsV6      int
	Operators   int
	LastError   string
}

func (s *FlowGeoService) Status() FlowGeoStatus {
	status := FlowGeoStatus{Path: s.Path}
	if message := s.lastErr.Load(); message != nil {
		status.LastError = *message
	}
	index := s.Index()
	if index == nil {
		return status
	}
	status.Loaded = true
	status.Version = index.Version
	status.GeneratedAt = index.GeneratedAt
	status.LoadedAt = index.LoadedAt
	status.RowsV4 = index.RowsV4
	status.RowsV6 = index.RowsV6
	status.Operators = len(index.operators)
	return status
}
