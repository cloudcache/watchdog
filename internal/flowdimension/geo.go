package flowdimension

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	GeoSchema                 = "flow-geo-v1"
	GeoAdminCodeSystem        = "GB/T2260-6"
	GeoUnknownCountry         = "ZZ"
	defaultMaxManifestBytes   = 256 << 10
	defaultMaxDictionaryBytes = 64 << 20
	defaultMaxCompressedBytes = 1 << 30
	defaultMaxUncompressed    = 2 << 30
	defaultMaxGeoRows         = 20_000_000
	defaultMaxCSVRowBytes     = 64 << 10
	defaultMaxZstdWindow      = 128 << 20
	defaultInitialGeoCapacity = 64 << 10
)

var geoRequiredFiles = [...]string{"ipv4.csv.zst", "ipv6.csv.zst", "operators.json", "geo_dict.json"}

var ErrNoGeoIndex = errors.New("no geo index for event time")

type GeoLoadLimits struct {
	MaxManifestBytes     int64
	MaxDictionaryBytes   int64
	MaxCompressedBytes   int64
	MaxUncompressedBytes int64
	MaxIPv4Rows          uint64
	MaxIPv6Rows          uint64
	MaxCSVRowBytes       int
	MaxZstdWindowBytes   uint64
}

type GeoManifest struct {
	Schema          string                 `json:"schema"`
	Version         string                 `json:"version"`
	GeneratedAt     time.Time              `json:"generated_at"`
	EffectiveFrom   time.Time              `json:"effective_from"`
	AdminCodeSystem string                 `json:"admin_code_system"`
	UnknownCountry  string                 `json:"unknown_country"`
	Files           map[string]GeoFileSpec `json:"files"`
}

type GeoFileSpec struct {
	SHA256 string `json:"sha256"`
	Rows   uint64 `json:"rows"`
}

type GeoOperator struct {
	ID        uint16 `json:"id"`
	Name      string `json:"name"`
	ShortName string `json:"short_name"`
	Category  string `json:"category"`
	Enabled   bool   `json:"enabled"`
}

type GeoDictionaryEntry struct {
	Kind       string `json:"kind"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	ParentCode string `json:"parent_code"`
	Enabled    bool   `json:"enabled"`
}

type GeoIndexMetadata struct {
	Version          string
	GeneratedAt      time.Time
	EffectiveFrom    time.Time
	ManifestChecksum string
	IPv4Rows         uint64
	IPv6Rows         uint64
	DistinctInfoRows uint64
	OperatorRows     uint64
	DictionaryRows   uint64
}

type geoRange4 struct {
	start uint32
	end   uint32
	info  uint32
}

type geoRange6 struct {
	start [2]uint64
	end   [2]uint64
	info  uint32
}

type geoDictionaryKey struct {
	kind string
	code string
}

// GeoIndex is immutable after construction. Lookup performs one binary search
// and does not retain mutable data supplied by callers.
type GeoIndex struct {
	metadata   GeoIndexMetadata
	ipv4       []geoRange4
	ipv6       []geoRange6
	infos      []GeoInfo
	operators  map[uint16]GeoOperator
	dictionary map[geoDictionaryKey]GeoDictionaryEntry
}

func (i *GeoIndex) Metadata() GeoIndexMetadata {
	if i == nil {
		return GeoIndexMetadata{}
	}
	return i.metadata
}

func (i *GeoIndex) Lookup(address netip.Addr) (GeoInfo, bool) {
	if i == nil || !address.IsValid() {
		return GeoInfo{}, false
	}
	address = address.Unmap()
	if address.Is4() {
		value := address.As4()
		key := uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
		position := sort.Search(len(i.ipv4), func(position int) bool { return i.ipv4[position].start > key })
		if position == 0 || key > i.ipv4[position-1].end {
			return GeoInfo{}, false
		}
		return i.infos[i.ipv4[position-1].info], true
	}
	value := address.As16()
	key := [2]uint64{bytesToUint64(value[:8]), bytesToUint64(value[8:])}
	position := sort.Search(len(i.ipv6), func(position int) bool { return compareUint128(i.ipv6[position].start, key) > 0 })
	if position == 0 || compareUint128(key, i.ipv6[position-1].end) > 0 {
		return GeoInfo{}, false
	}
	return i.infos[i.ipv6[position-1].info], true
}

func (i *GeoIndex) Operator(id uint16) (GeoOperator, bool) {
	if i == nil {
		return GeoOperator{}, false
	}
	operator, ok := i.operators[id]
	return operator, ok
}

func (i *GeoIndex) Dictionary(kind, code string) (GeoDictionaryEntry, bool) {
	if i == nil {
		return GeoDictionaryEntry{}, false
	}
	entry, ok := i.dictionary[geoDictionaryKey{kind: kind, code: code}]
	return entry, ok
}

type GeoCatalog struct {
	mu    sync.Mutex
	state atomic.Pointer[geoCatalogState]
}

type geoCatalogState struct {
	active      *GeoIndex
	byVersion   map[string]*GeoIndex
	byEffective []*GeoIndex
}

func NewGeoCatalog() *GeoCatalog {
	catalog := &GeoCatalog{}
	catalog.state.Store(&geoCatalogState{byVersion: map[string]*GeoIndex{}})
	return catalog
}

func (c *GeoCatalog) Active() (*GeoIndex, bool) {
	if c == nil {
		return nil, false
	}
	state := c.state.Load()
	if state == nil || state.active == nil {
		return nil, false
	}
	return state.active, true
}

func (c *GeoCatalog) Get(version string) (*GeoIndex, bool) {
	if c == nil {
		return nil, false
	}
	state := c.state.Load()
	if state == nil {
		return nil, false
	}
	index, ok := state.byVersion[version]
	return index, ok
}

func (c *GeoCatalog) Select(eventTime time.Time) (*GeoIndex, error) {
	if c == nil || eventTime.IsZero() {
		return nil, ErrNoGeoIndex
	}
	state := c.state.Load()
	if state == nil {
		return nil, ErrNoGeoIndex
	}
	position := sort.Search(len(state.byEffective), func(position int) bool {
		return state.byEffective[position].metadata.EffectiveFrom.After(eventTime)
	})
	if position == 0 {
		return nil, ErrNoGeoIndex
	}
	return state.byEffective[position-1], nil
}

// Reload resolves a current symlink once, builds the complete index away from
// the hot path, and publishes it only after every file and lookup check passes.
// A failed reload therefore leaves both the active and historical indexes intact.
func (c *GeoCatalog) Reload(path string, limits GeoLoadLimits) (bool, error) {
	return c.load(path, limits, true)
}

// LoadHistorical validates and installs a version for event-time replay
// without changing the version currently published by the current symlink.
func (c *GeoCatalog) LoadHistorical(path string, limits GeoLoadLimits) (bool, error) {
	return c.load(path, limits, false)
}

func (c *GeoCatalog) load(path string, limits GeoLoadLimits, activate bool) (bool, error) {
	if c == nil {
		return false, errors.New("geo catalog is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	limits = normalizeGeoLoadLimits(limits)
	directory, manifest, manifestChecksum, err := readGeoManifest(path, limits)
	if err != nil {
		return false, err
	}
	current := c.state.Load()
	if current == nil {
		current = &geoCatalogState{byVersion: map[string]*GeoIndex{}}
	}
	if !activate && current.active == nil {
		return false, errors.New("load the active geo version before historical versions")
	}
	if existing, ok := current.byVersion[manifest.Version]; ok {
		if existing.metadata.ManifestChecksum != manifestChecksum {
			return false, errors.New("geo version is immutable but manifest checksum changed")
		}
		if !activate || current.active == existing {
			return false, nil
		}
		if current.active != nil && !existing.metadata.EffectiveFrom.After(current.active.metadata.EffectiveFrom) {
			return false, errors.New("geo active effective_from must increase; rollback requires a new version")
		}
		next := cloneGeoCatalogState(current)
		next.active = existing
		c.state.Store(next)
		return true, nil
	}
	for _, existing := range current.byEffective {
		if existing.metadata.EffectiveFrom.Equal(manifest.EffectiveFrom) {
			return false, errors.New("geo effective_from already exists")
		}
	}
	if !activate && !manifest.EffectiveFrom.Before(current.active.metadata.EffectiveFrom) {
		return false, errors.New("historical geo effective_from must precede the active version")
	}
	if activate && current.active != nil && !manifest.EffectiveFrom.After(current.active.metadata.EffectiveFrom) {
		return false, errors.New("geo active effective_from must increase; rollback requires a new version")
	}
	index, err := loadResolvedGeoIndex(directory, manifest, manifestChecksum, limits)
	if err != nil {
		return false, err
	}
	next := cloneGeoCatalogState(current)
	next.byVersion[index.metadata.Version] = index
	next.byEffective = append(next.byEffective, index)
	sort.Slice(next.byEffective, func(left, right int) bool {
		return next.byEffective[left].metadata.EffectiveFrom.Before(next.byEffective[right].metadata.EffectiveFrom)
	})
	if activate {
		next.active = index
	}
	c.state.Store(next)
	return true, nil
}

// RetainVersions removes only an oldest, contiguous prefix of historical
// indexes. The active version and every version at or after the oldest requested
// version are retained so event-time selection can never fall through a gap to
// the wrong older index. An unknown requested version conservatively skips GC.
func (c *GeoCatalog) RetainVersions(versions []string) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.state.Load()
	if current == nil || current.active == nil || len(current.byVersion) == 0 {
		return 0
	}
	cutoff := current.active.metadata.EffectiveFrom
	for _, version := range versions {
		index, exists := current.byVersion[version]
		if !exists {
			return 0
		}
		if index.metadata.EffectiveFrom.Before(cutoff) {
			cutoff = index.metadata.EffectiveFrom
		}
	}
	next := &geoCatalogState{active: current.active, byVersion: make(map[string]*GeoIndex, len(current.byVersion))}
	for _, index := range current.byEffective {
		if !index.metadata.EffectiveFrom.Before(cutoff) {
			next.byVersion[index.metadata.Version] = index
			next.byEffective = append(next.byEffective, index)
		}
	}
	removed := len(current.byVersion) - len(next.byVersion)
	if removed == 0 {
		return 0
	}
	c.state.Store(next)
	return removed
}

func LoadGeoIndex(path string, limits GeoLoadLimits) (*GeoIndex, error) {
	limits = normalizeGeoLoadLimits(limits)
	directory, manifest, manifestChecksum, err := readGeoManifest(path, limits)
	if err != nil {
		return nil, err
	}
	return loadResolvedGeoIndex(directory, manifest, manifestChecksum, limits)
}

func cloneGeoCatalogState(current *geoCatalogState) *geoCatalogState {
	next := &geoCatalogState{
		active: current.active, byVersion: make(map[string]*GeoIndex, len(current.byVersion)+1),
		byEffective: append([]*GeoIndex(nil), current.byEffective...),
	}
	for version, index := range current.byVersion {
		next.byVersion[version] = index
	}
	return next
}

func loadResolvedGeoIndex(directory string, manifest GeoManifest, manifestChecksum string, limits GeoLoadLimits) (*GeoIndex, error) {
	operators, err := loadGeoOperators(filepath.Join(directory, "operators.json"), manifest.Files["operators.json"], limits)
	if err != nil {
		return nil, err
	}
	dictionary, dictionaryCodes, err := loadGeoDictionary(filepath.Join(directory, "geo_dict.json"), manifest.Files["geo_dict.json"], limits)
	if err != nil {
		return nil, err
	}
	infoTable := newGeoInfoTable()
	ipv4, _, err := loadGeoRanges(filepath.Join(directory, "ipv4.csv.zst"), 4, manifest, operators, dictionaryCodes, infoTable, limits)
	if err != nil {
		return nil, err
	}
	_, ipv6, err := loadGeoRanges(filepath.Join(directory, "ipv6.csv.zst"), 6, manifest, operators, dictionaryCodes, infoTable, limits)
	if err != nil {
		return nil, err
	}
	index := &GeoIndex{
		metadata: GeoIndexMetadata{
			Version: manifest.Version, GeneratedAt: manifest.GeneratedAt.UTC(), EffectiveFrom: manifest.EffectiveFrom.UTC(), ManifestChecksum: manifestChecksum,
			IPv4Rows: uint64(len(ipv4)), IPv6Rows: uint64(len(ipv6)), DistinctInfoRows: uint64(len(infoTable.values)),
			OperatorRows: manifest.Files["operators.json"].Rows, DictionaryRows: manifest.Files["geo_dict.json"].Rows,
		},
		ipv4: ipv4, ipv6: ipv6, infos: infoTable.values, operators: operators, dictionary: dictionary,
	}
	if err := validateGeoLookupSamples(index); err != nil {
		return nil, err
	}
	return index, nil
}

func readGeoManifest(path string, limits GeoLoadLimits) (string, GeoManifest, string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", GeoManifest{}, "", fmt.Errorf("resolve geo directory: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", GeoManifest{}, "", errors.New("geo path must resolve to a directory")
	}
	data, err := readBoundedFile(filepath.Join(resolved, "manifest.json"), limits.MaxManifestBytes)
	if err != nil {
		return "", GeoManifest{}, "", fmt.Errorf("read geo manifest: %w", err)
	}
	var manifest GeoManifest
	if err := decodeStrictJSON(data, &manifest); err != nil {
		return "", GeoManifest{}, "", fmt.Errorf("decode geo manifest: %w", err)
	}
	if err := validateGeoManifest(manifest, limits); err != nil {
		return "", GeoManifest{}, "", err
	}
	digest := sha256.Sum256(data)
	return resolved, manifest, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func validateGeoManifest(manifest GeoManifest, limits GeoLoadLimits) error {
	if manifest.Schema != GeoSchema {
		return fmt.Errorf("unsupported geo schema %q", manifest.Schema)
	}
	if !validIdentifier(manifest.Version, 128) {
		return errors.New("geo version is required and must be bounded printable UTF-8")
	}
	_, offset := manifest.GeneratedAt.Zone()
	if manifest.GeneratedAt.IsZero() || offset != 0 {
		return errors.New("geo generated_at must be a UTC timestamp")
	}
	effectiveFrom := manifest.EffectiveFrom.UTC()
	_, effectiveOffset := manifest.EffectiveFrom.Zone()
	if effectiveFrom.IsZero() || effectiveOffset != 0 || effectiveFrom.Second() != 0 || effectiveFrom.Nanosecond() != 0 {
		return errors.New("geo effective_from must be a UTC minute boundary")
	}
	if manifest.AdminCodeSystem != GeoAdminCodeSystem || manifest.UnknownCountry != GeoUnknownCountry {
		return errors.New("geo admin_code_system or unknown_country is unsupported")
	}
	if len(manifest.Files) != len(geoRequiredFiles) {
		return errors.New("geo manifest must contain exactly the four required files")
	}
	for _, name := range geoRequiredFiles {
		spec, ok := manifest.Files[name]
		if !ok {
			return fmt.Errorf("geo manifest is missing %s", name)
		}
		if _, err := parseRawSHA256(spec.SHA256); err != nil {
			return fmt.Errorf("geo manifest %s: %w", name, err)
		}
	}
	if manifest.Files["ipv4.csv.zst"].Rows > limits.MaxIPv4Rows || manifest.Files["ipv6.csv.zst"].Rows > limits.MaxIPv6Rows {
		return errors.New("geo manifest range row count exceeds configured limit")
	}
	maxInfoRows := uint64(^uint32(0))
	if manifest.Files["ipv4.csv.zst"].Rows > maxInfoRows || manifest.Files["ipv6.csv.zst"].Rows > maxInfoRows ||
		manifest.Files["ipv4.csv.zst"].Rows+manifest.Files["ipv6.csv.zst"].Rows > maxInfoRows {
		return errors.New("geo manifest total range rows exceed info index capacity")
	}
	return nil
}

func loadGeoOperators(path string, spec GeoFileSpec, limits GeoLoadLimits) (map[uint16]GeoOperator, error) {
	data, err := readAndVerifyGeoFile(path, spec.SHA256, limits.MaxDictionaryBytes)
	if err != nil {
		return nil, fmt.Errorf("load operators.json: %w", err)
	}
	var entries []GeoOperator
	if err := decodeStrictJSONArray(data, &entries); err != nil {
		return nil, fmt.Errorf("decode operators.json: %w", err)
	}
	if uint64(len(entries)) != spec.Rows {
		return nil, fmt.Errorf("operators.json row count is %d, manifest declares %d", len(entries), spec.Rows)
	}
	operators := make(map[uint16]GeoOperator, len(entries))
	for position, entry := range entries {
		if !validText(entry.Name, 256) || !validText(entry.ShortName, 128) || !validText(entry.Category, 128) {
			return nil, fmt.Errorf("operators.json[%d] has invalid text fields", position)
		}
		if _, exists := operators[entry.ID]; exists {
			return nil, fmt.Errorf("operators.json[%d] duplicates id %d", position, entry.ID)
		}
		operators[entry.ID] = entry
	}
	return operators, nil
}

func loadGeoDictionary(path string, spec GeoFileSpec, limits GeoLoadLimits) (map[geoDictionaryKey]GeoDictionaryEntry, map[string]struct{}, error) {
	data, err := readAndVerifyGeoFile(path, spec.SHA256, limits.MaxDictionaryBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("load geo_dict.json: %w", err)
	}
	var entries []GeoDictionaryEntry
	if err := decodeStrictJSONArray(data, &entries); err != nil {
		return nil, nil, fmt.Errorf("decode geo_dict.json: %w", err)
	}
	if uint64(len(entries)) != spec.Rows {
		return nil, nil, fmt.Errorf("geo_dict.json row count is %d, manifest declares %d", len(entries), spec.Rows)
	}
	dictionary := make(map[geoDictionaryKey]GeoDictionaryEntry, len(entries))
	codes := make(map[string]struct{}, len(entries))
	parents := make(map[string]string, len(entries))
	for position, entry := range entries {
		if !validText(entry.Kind, 64) || !validText(entry.Code, 128) || !validText(entry.Name, 256) ||
			(entry.ParentCode != "" && !validText(entry.ParentCode, 128)) {
			return nil, nil, fmt.Errorf("geo_dict.json[%d] has invalid text fields", position)
		}
		key := geoDictionaryKey{kind: entry.Kind, code: entry.Code}
		if _, exists := dictionary[key]; exists {
			return nil, nil, fmt.Errorf("geo_dict.json[%d] duplicates kind/code", position)
		}
		if _, exists := codes[entry.Code]; exists {
			return nil, nil, fmt.Errorf("geo_dict.json[%d] duplicates globally unique code %q", position, entry.Code)
		}
		dictionary[key] = entry
		codes[entry.Code] = struct{}{}
		parents[entry.Code] = entry.ParentCode
	}
	for position, entry := range entries {
		if entry.ParentCode != "" {
			if _, exists := codes[entry.ParentCode]; !exists {
				return nil, nil, fmt.Errorf("geo_dict.json[%d] references missing parent_code %q", position, entry.ParentCode)
			}
		}
	}
	if err := validateGeoDictionaryHierarchy(parents); err != nil {
		return nil, nil, err
	}
	return dictionary, codes, nil
}

func validateGeoDictionaryHierarchy(parents map[string]string) error {
	states := make(map[string]uint8, len(parents))
	var visit func(string) error
	visit = func(code string) error {
		switch states[code] {
		case 1:
			return fmt.Errorf("geo_dict.json contains a parent cycle at code %q", code)
		case 2:
			return nil
		}
		states[code] = 1
		if parent := parents[code]; parent != "" {
			if err := visit(parent); err != nil {
				return err
			}
		}
		states[code] = 2
		return nil
	}
	for code := range parents {
		if err := visit(code); err != nil {
			return err
		}
	}
	return nil
}

func loadGeoRanges(path string, family int, manifest GeoManifest, operators map[uint16]GeoOperator, dictionaryCodes map[string]struct{}, infoTable *geoInfoTable, limits GeoLoadLimits) ([]geoRange4, []geoRange6, error) {
	name := filepath.Base(path)
	spec := manifest.Files[name]
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat %s: %w", name, err)
	}
	if info.Size() < 1 || info.Size() > limits.MaxCompressedBytes {
		return nil, nil, fmt.Errorf("%s compressed size must be 1..%d bytes", name, limits.MaxCompressedBytes)
	}
	hasher := sha256.New()
	compressed := io.TeeReader(file, hasher)
	decoder, err := zstd.NewReader(compressed,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(uint64(limits.MaxUncompressedBytes)),
		zstd.WithDecoderMaxWindow(limits.MaxZstdWindowBytes),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s zstd stream: %w", name, err)
	}
	defer decoder.Close()
	decoded := &boundedReader{reader: decoder, maximum: limits.MaxUncompressedBytes}
	scanner := bufio.NewScanner(decoded)
	initialBuffer := 64 << 10
	if limits.MaxCSVRowBytes < initialBuffer {
		initialBuffer = limits.MaxCSVRowBytes
	}
	scanner.Buffer(make([]byte, initialBuffer), limits.MaxCSVRowBytes)
	var ipv4 []geoRange4
	var ipv6 []geoRange6
	initialCapacity := defaultInitialGeoCapacity
	if spec.Rows < uint64(initialCapacity) {
		initialCapacity = int(spec.Rows)
	}
	if family == 4 {
		ipv4 = make([]geoRange4, 0, initialCapacity)
	} else {
		ipv6 = make([]geoRange6, 0, initialCapacity)
	}
	row := uint64(0)
	for scanner.Scan() {
		if row == 0 {
			if string(scanner.Bytes()) != "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn" {
				return nil, nil, fmt.Errorf("%s has an invalid CSV header", name)
			}
			row++
			continue
		}
		if row-1 >= spec.Rows {
			return nil, nil, fmt.Errorf("%s has more rows than the manifest declares", name)
		}
		fields, err := parseGeoCSVLine(scanner.Bytes())
		if err != nil {
			return nil, nil, fmt.Errorf("%s row %d: %w", name, row+1, err)
		}
		start, end, info, err := parseGeoRange(fields, family, manifest.Version, operators, dictionaryCodes)
		if err != nil {
			return nil, nil, fmt.Errorf("%s row %d: %w", name, row+1, err)
		}
		if family == 4 {
			infoID := infoTable.intern(info)
			startValue, endValue := addressToUint32(start), addressToUint32(end)
			if len(ipv4) > 0 && (startValue <= ipv4[len(ipv4)-1].start || startValue <= ipv4[len(ipv4)-1].end) {
				return nil, nil, fmt.Errorf("%s row %d is unsorted or overlaps the previous range", name, row+1)
			}
			ipv4 = append(ipv4, geoRange4{start: startValue, end: endValue, info: infoID})
		} else {
			infoID := infoTable.intern(info)
			startValue, endValue := addressToUint128(start), addressToUint128(end)
			if len(ipv6) > 0 && (compareUint128(startValue, ipv6[len(ipv6)-1].start) <= 0 || compareUint128(startValue, ipv6[len(ipv6)-1].end) <= 0) {
				return nil, nil, fmt.Errorf("%s row %d is unsorted or overlaps the previous range", name, row+1)
			}
			ipv6 = append(ipv6, geoRange6{start: startValue, end: endValue, info: infoID})
		}
		row++
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", name, err)
	}
	if decoded.exceeded {
		return nil, nil, fmt.Errorf("%s exceeds uncompressed size limit %d", name, limits.MaxUncompressedBytes)
	}
	if row == 0 {
		return nil, nil, fmt.Errorf("%s is missing its CSV header", name)
	}
	actualRows := row - 1
	if actualRows != spec.Rows {
		return nil, nil, fmt.Errorf("%s row count is %d, manifest declares %d", name, actualRows, spec.Rows)
	}
	if _, err := io.Copy(io.Discard, compressed); err != nil {
		return nil, nil, fmt.Errorf("finish hashing %s: %w", name, err)
	}
	if err := verifyRawSHA256(spec.SHA256, hasher); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	return ipv4, ipv6, nil
}

func parseGeoCSVLine(line []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewReader(line))
	reader.FieldsPerRecord = 8
	record, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		return nil, errors.New("CSV row must contain exactly one record")
	}
	return record, nil
}

func parseGeoRange(fields []string, family int, version string, operators map[uint16]GeoOperator, dictionaryCodes map[string]struct{}) (netip.Addr, netip.Addr, GeoInfo, error) {
	start, err := netip.ParseAddr(fields[0])
	if err != nil || start.Is4() != (family == 4) || start.Unmap() != start {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("ip_start has the wrong address family")
	}
	end, err := netip.ParseAddr(fields[1])
	if err != nil || end.Is4() != (family == 4) || end.Unmap() != end || start.Compare(end) > 0 {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("ip_end has the wrong address family or precedes ip_start")
	}
	country := fields[2]
	adminCode := fields[3]
	if !validCountryCode(country) || country != strings.ToUpper(country) {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("country must be an uppercase ISO alpha-2 code")
	}
	if country == "HK" || country == "MO" || country == "TW" {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("HK/MO/TW must be normalized to country=CN and admin_code=71/81/82xxxx")
	}
	if country == "CN" {
		if adminCode != "" && (len(adminCode) != 6 || !allDigits(adminCode)) {
			return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("CN admin_code must be empty or six digits")
		}
	} else if adminCode != "" {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("non-CN admin_code must be empty")
	}
	if adminCode != "" {
		if _, exists := dictionaryCodes[adminCode]; !exists {
			return netip.Addr{}, netip.Addr{}, GeoInfo{}, fmt.Errorf("admin_code %q is missing from geo_dict.json", adminCode)
		}
	}
	if (fields[4] != "" && !validText(fields[4], 256)) || (fields[5] != "" && !validText(fields[5], 256)) {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("subdivision and city must be bounded printable UTF-8")
	}
	isp, err := strconv.ParseUint(fields[6], 10, 16)
	if err != nil {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("isp_id must be UInt16")
	}
	if isp != 0 {
		if _, exists := operators[uint16(isp)]; !exists {
			return netip.Addr{}, netip.Addr{}, GeoInfo{}, fmt.Errorf("isp_id %d is missing from operators.json", isp)
		}
	}
	asn, err := strconv.ParseUint(fields[7], 10, 32)
	if err != nil {
		return netip.Addr{}, netip.Addr{}, GeoInfo{}, errors.New("asn must be UInt32")
	}
	return start, end, GeoInfo{
		Country: country, AdminCode: adminCode, Subdivision: fields[4], City: fields[5],
		ISPID: uint16(isp), ASN: uint32(asn), Version: version, Source: GeoSchema,
	}, nil
}

func validateGeoLookupSamples(index *GeoIndex) error {
	for _, position := range samplePositions(len(index.ipv4)) {
		rangeValue := index.ipv4[position]
		address := netip.AddrFrom4([4]byte{byte(rangeValue.start >> 24), byte(rangeValue.start >> 16), byte(rangeValue.start >> 8), byte(rangeValue.start)})
		actual, ok := index.Lookup(address)
		if !ok || actual != index.infos[rangeValue.info] {
			return errors.New("IPv4 geo index sample validation failed")
		}
	}
	for _, position := range samplePositions(len(index.ipv6)) {
		rangeValue := index.ipv6[position]
		address := uint128ToAddress(rangeValue.start)
		actual, ok := index.Lookup(address)
		if !ok || actual != index.infos[rangeValue.info] {
			return errors.New("IPv6 geo index sample validation failed")
		}
	}
	return nil
}

type geoInfoTable struct {
	values []GeoInfo
	ids    map[GeoInfo]uint32
}

func newGeoInfoTable() *geoInfoTable {
	return &geoInfoTable{ids: make(map[GeoInfo]uint32)}
}

func (t *geoInfoTable) intern(info GeoInfo) uint32 {
	if id, exists := t.ids[info]; exists {
		return id
	}
	id := uint32(len(t.values))
	t.values = append(t.values, info)
	t.ids[info] = id
	return id
}

func samplePositions(length int) []int {
	if length == 0 {
		return nil
	}
	positions := []int{0, length / 2, length - 1}
	result := positions[:0]
	for _, position := range positions {
		if len(result) == 0 || result[len(result)-1] != position {
			result = append(result, position)
		}
	}
	return result
}

type boundedReader struct {
	reader   io.Reader
	maximum  int64
	read     int64
	exceeded bool
}

func (r *boundedReader) Read(buffer []byte) (int, error) {
	if r.read > r.maximum {
		r.exceeded = true
		return 0, io.EOF
	}
	remaining := r.maximum - r.read + 1
	if int64(len(buffer)) > remaining {
		buffer = buffer[:remaining]
	}
	count, err := r.reader.Read(buffer)
	r.read += int64(count)
	if r.read > r.maximum {
		r.exceeded = true
	}
	return count, err
}

func normalizeGeoLoadLimits(limits GeoLoadLimits) GeoLoadLimits {
	if limits.MaxManifestBytes <= 0 {
		limits.MaxManifestBytes = defaultMaxManifestBytes
	}
	if limits.MaxDictionaryBytes <= 0 {
		limits.MaxDictionaryBytes = defaultMaxDictionaryBytes
	}
	if limits.MaxCompressedBytes <= 0 {
		limits.MaxCompressedBytes = defaultMaxCompressedBytes
	}
	if limits.MaxUncompressedBytes <= 0 {
		limits.MaxUncompressedBytes = defaultMaxUncompressed
	}
	if limits.MaxIPv4Rows == 0 {
		limits.MaxIPv4Rows = defaultMaxGeoRows
	}
	if limits.MaxIPv6Rows == 0 {
		limits.MaxIPv6Rows = defaultMaxGeoRows
	}
	if limits.MaxCSVRowBytes <= 0 {
		limits.MaxCSVRowBytes = defaultMaxCSVRowBytes
	}
	if limits.MaxZstdWindowBytes == 0 {
		limits.MaxZstdWindowBytes = defaultMaxZstdWindow
	}
	return limits
}

func readAndVerifyGeoFile(path, checksum string, maximum int64) ([]byte, error) {
	data, err := readBoundedFile(path, maximum)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	want, err := parseRawSHA256(checksum)
	if err != nil {
		return nil, err
	}
	if digest != want {
		return nil, errors.New("SHA-256 checksum mismatch")
	}
	return data, nil
}

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 1 || info.Size() > maximum {
		return nil, fmt.Errorf("file size must be 1..%d bytes", maximum)
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, fmt.Errorf("file exceeds size limit %d", maximum)
	}
	return data, nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func decodeStrictJSONArray(data []byte, target any) error {
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '[' {
		return errors.New("file must contain a JSON array")
	}
	return decodeStrictJSON(data, target)
}

func parseRawSHA256(value string) ([sha256.Size]byte, error) {
	var checksum [sha256.Size]byte
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return checksum, errors.New("sha256 must be canonical lower-case hex")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return checksum, errors.New("sha256 must be canonical lower-case hex")
	}
	copy(checksum[:], decoded)
	return checksum, nil
}

func verifyRawSHA256(want string, actual hash.Hash) error {
	expected, err := parseRawSHA256(want)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected[:], actual.Sum(nil)) {
		return errors.New("SHA-256 checksum mismatch")
	}
	return nil
}

func addressToUint32(address netip.Addr) uint32 {
	value := address.As4()
	return uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
}

func addressToUint128(address netip.Addr) [2]uint64 {
	value := address.As16()
	return [2]uint64{bytesToUint64(value[:8]), bytesToUint64(value[8:])}
}

func bytesToUint64(value []byte) uint64 {
	return uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 |
		uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7])
}

func compareUint128(left, right [2]uint64) int {
	if left[0] < right[0] {
		return -1
	}
	if left[0] > right[0] {
		return 1
	}
	if left[1] < right[1] {
		return -1
	}
	if left[1] > right[1] {
		return 1
	}
	return 0
}

func uint128ToAddress(value [2]uint64) netip.Addr {
	var bytes [16]byte
	for index := range 8 {
		shift := uint(56 - index*8)
		bytes[index] = byte(value[0] >> shift)
		bytes[index+8] = byte(value[1] >> shift)
	}
	return netip.AddrFrom16(bytes)
}
