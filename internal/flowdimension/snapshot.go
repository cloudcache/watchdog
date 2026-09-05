package flowdimension

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gaissmai/bart"
)

const (
	BundleSchemaVersion       = 1
	UnassignedDimensionID     = "_unassigned"
	defaultMaxBundleBytes     = 64 << 20
	defaultMaxPrefixes        = 1_000_000
	defaultMaxAddressSets     = 10_000
	defaultMaxSetsPerRecord   = 32
	defaultMaxSetCandidates   = 50_000_000
	maxLabelsPerPrefix        = 64
	maxSelectorValuesPerLabel = 256
)

var ErrNoDimensionSnapshot = errors.New("no dimension snapshot for event time")

type CompileLimits struct {
	MaxBundleBytes          int
	MaxPrefixes             int
	MaxAddressSets          int
	MaxAddressSetsPerRecord int
	MaxSelectorCandidates   int64
}

type SnapshotBundle struct {
	SchemaVersion uint32                 `json:"schema_version"`
	SnapshotID    string                 `json:"snapshot_id"`
	TenantID      string                 `json:"tenant_id"`
	Version       uint64                 `json:"version"`
	EffectiveFrom time.Time              `json:"effective_from"`
	Prefixes      []PrefixDefinition     `json:"prefixes"`
	AddressSets   []AddressSetDefinition `json:"address_sets"`
}

type PrefixDefinition struct {
	ID     string            `json:"id"`
	CIDR   string            `json:"cidr"`
	Labels map[string]string `json:"labels"`
}

type AddressSetDefinition struct {
	ID             string        `json:"id"`
	Selector       LabelSelector `json:"selector"`
	MatchDirection string        `json:"match_direction"`
	Enabled        bool          `json:"enabled"`
}

// LabelSelector accepts the existing management shape where each label value
// is either one string or an array of strings. Compilation canonicalizes both
// forms to sorted, unique arrays.
type LabelSelector struct {
	Labels map[string][]string `json:"labels"`
}

func (s *LabelSelector) UnmarshalJSON(data []byte) error {
	var wire struct {
		Labels map[string]json.RawMessage `json:"labels"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	s.Labels = make(map[string][]string, len(wire.Labels))
	for key, raw := range wire.Labels {
		var single string
		if err := json.Unmarshal(raw, &single); err == nil {
			s.Labels[key] = []string{single}
			continue
		}
		var many []string
		if err := json.Unmarshal(raw, &many); err != nil {
			return fmt.Errorf("selector label %q must be a string or string array", key)
		}
		s.Labels[key] = many
	}
	return nil
}

func (s LabelSelector) MarshalJSON() ([]byte, error) {
	type selectorWire struct {
		Labels map[string][]string `json:"labels"`
	}
	return json.Marshal(selectorWire{Labels: s.Labels})
}

type SnapshotMetadata struct {
	SnapshotID              string
	TenantID                string
	Version                 uint64
	EffectiveFrom           time.Time
	Checksum                string
	PrefixCount             int
	EnabledAddressSetCount  int
	MaxAddressSetsPerRecord int
}

type CompiledSnapshot struct {
	metadata     SnapshotMetadata
	prefixes     *bart.Table[compiledPrefix]
	geoOverrides *bart.Table[compiledGeoOverride]
}

type compiledPrefix struct {
	id             string
	cidr           string
	labels         map[string]string
	inAddressSets  []string
	outAddressSets []string
}

type GeoOverrideFields uint8

const (
	GeoOverrideCountry GeoOverrideFields = 1 << iota
	GeoOverrideAdminCode
	GeoOverrideSubdivision
	GeoOverrideCity
	GeoOverrideISPID
	GeoOverrideASN
)

type compiledGeoOverride struct {
	info   GeoInfo
	fields GeoOverrideFields
}

type compiledAddressSet struct {
	id        string
	direction BusinessDirection
	selector  map[string][]string
}

func DecodeAndCompileBundle(data []byte, expectedChecksum string, limits CompileLimits) (*CompiledSnapshot, error) {
	limits = normalizeCompileLimits(limits)
	if len(data) == 0 || len(data) > limits.MaxBundleBytes {
		return nil, fmt.Errorf("dimension bundle size must be 1..%d bytes", limits.MaxBundleBytes)
	}
	want, err := parseSHA256Checksum(expectedChecksum)
	if err != nil {
		return nil, err
	}
	got := sha256.Sum256(data)
	if got != want {
		return nil, errors.New("dimension bundle checksum mismatch")
	}
	var bundle SnapshotBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode dimension bundle: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode dimension bundle: %w", err)
	}
	return compileBundle(bundle, "sha256:"+hex.EncodeToString(got[:]), limits)
}

func CompileBundle(bundle SnapshotBundle, limits CompileLimits) (*CompiledSnapshot, error) {
	return compileBundle(bundle, "", normalizeCompileLimits(limits))
}

func compileBundle(bundle SnapshotBundle, checksum string, limits CompileLimits) (*CompiledSnapshot, error) {
	if bundle.SchemaVersion != BundleSchemaVersion {
		return nil, fmt.Errorf("unsupported dimension bundle schema_version %d", bundle.SchemaVersion)
	}
	if !validIdentifier(bundle.SnapshotID, 64) || !validIdentifier(bundle.TenantID, 64) || bundle.Version == 0 {
		return nil, errors.New("dimension bundle identity and version are required")
	}
	effectiveFrom := bundle.EffectiveFrom.UTC()
	_, effectiveOffset := bundle.EffectiveFrom.Zone()
	if effectiveFrom.IsZero() || effectiveOffset != 0 || effectiveFrom.Second() != 0 || effectiveFrom.Nanosecond() != 0 {
		return nil, errors.New("dimension bundle effective_from must be a UTC minute boundary")
	}
	if len(bundle.Prefixes) > limits.MaxPrefixes {
		return nil, fmt.Errorf("dimension bundle has %d prefixes, limit is %d", len(bundle.Prefixes), limits.MaxPrefixes)
	}
	if len(bundle.AddressSets) > limits.MaxAddressSets {
		return nil, fmt.Errorf("dimension bundle has %d address sets, limit is %d", len(bundle.AddressSets), limits.MaxAddressSets)
	}

	prefixIDs := make(map[string]struct{}, len(bundle.Prefixes))
	prefixCIDRs := make(map[string]struct{}, len(bundle.Prefixes))
	parsedPrefixes := make([]netip.Prefix, 0, len(bundle.Prefixes))
	compiledPrefixes := make([]compiledPrefix, 0, len(bundle.Prefixes))
	geoOverrides := &bart.Table[compiledGeoOverride]{}
	for index, definition := range bundle.Prefixes {
		if !validIdentifier(definition.ID, 128) || len(definition.Labels) > maxLabelsPerPrefix {
			return nil, fmt.Errorf("prefixes[%d] has invalid id or too many labels", index)
		}
		if _, exists := prefixIDs[definition.ID]; exists {
			return nil, fmt.Errorf("prefixes[%d] duplicates id %q", index, definition.ID)
		}
		prefix, err := netip.ParsePrefix(definition.CIDR)
		if err != nil || definition.CIDR != prefix.Masked().String() {
			return nil, fmt.Errorf("prefixes[%d].cidr must be canonical IPv4 or IPv6 CIDR", index)
		}
		if _, exists := prefixCIDRs[definition.CIDR]; exists {
			return nil, fmt.Errorf("prefixes[%d] duplicates cidr %q", index, definition.CIDR)
		}
		labels, err := cloneAndValidateLabels(definition.Labels)
		if err != nil {
			return nil, fmt.Errorf("prefixes[%d].labels: %w", index, err)
		}
		compiled := compiledPrefix{id: definition.ID, cidr: definition.CIDR, labels: labels}
		override, hasOverride, err := compileGeoOverride(labels)
		if err != nil {
			return nil, fmt.Errorf("prefixes[%d].labels: %w", index, err)
		}
		if hasOverride {
			geoOverrides.Insert(prefix, override)
		}
		parsedPrefixes = append(parsedPrefixes, prefix)
		compiledPrefixes = append(compiledPrefixes, compiled)
		prefixIDs[definition.ID] = struct{}{}
		prefixCIDRs[definition.CIDR] = struct{}{}
	}

	sets := make([]compiledAddressSet, 0, len(bundle.AddressSets))
	setIDs := make(map[string]struct{}, len(bundle.AddressSets))
	for index, definition := range bundle.AddressSets {
		if !validIdentifier(definition.ID, 128) {
			return nil, fmt.Errorf("address_sets[%d].id is invalid", index)
		}
		if _, exists := setIDs[definition.ID]; exists {
			return nil, fmt.Errorf("address_sets[%d] duplicates id %q", index, definition.ID)
		}
		setIDs[definition.ID] = struct{}{}
		if !definition.Enabled {
			continue
		}
		direction := BusinessDirection(strings.ToLower(strings.TrimSpace(definition.MatchDirection)))
		if direction == "" {
			direction = DirectionBoth
		}
		if direction != DirectionIn && direction != DirectionOut && direction != DirectionBoth {
			return nil, fmt.Errorf("address_sets[%d].match_direction must be in, out, or both", index)
		}
		selector, err := cloneAndValidateSelector(definition.Selector.Labels)
		if err != nil {
			return nil, fmt.Errorf("address_sets[%d].selector: %w", index, err)
		}
		sets = append(sets, compiledAddressSet{id: definition.ID, direction: direction, selector: selector})
	}
	sort.Slice(sets, func(i, j int) bool { return sets[i].id < sets[j].id })

	if err := precomputePrefixMemberships(compiledPrefixes, sets, limits.MaxAddressSetsPerRecord, limits.MaxSelectorCandidates); err != nil {
		return nil, err
	}
	maxExpansion := estimateMaxSetExpansion(compiledPrefixes)
	if maxExpansion > limits.MaxAddressSetsPerRecord {
		return nil, fmt.Errorf("dimension bundle address-set expansion %d exceeds per-record limit %d", maxExpansion, limits.MaxAddressSetsPerRecord)
	}
	tree := &bart.Table[compiledPrefix]{}
	for index := range compiledPrefixes {
		tree.Insert(parsedPrefixes[index], compiledPrefixes[index])
	}
	return &CompiledSnapshot{
		metadata: SnapshotMetadata{
			SnapshotID: bundle.SnapshotID, TenantID: bundle.TenantID, Version: bundle.Version,
			EffectiveFrom: effectiveFrom, Checksum: checksum, PrefixCount: len(compiledPrefixes),
			EnabledAddressSetCount: len(sets), MaxAddressSetsPerRecord: maxExpansion,
		},
		prefixes: tree, geoOverrides: geoOverrides,
	}, nil
}

// ApplyGeoOverride overlays the most-specific tenant correction without
// consulting mutable management state. The selected Geo version remains on
// the result so event-time provenance is not lost.
func (s *CompiledSnapshot) ApplyGeoOverride(address netip.Addr, base GeoInfo) (GeoInfo, GeoOverrideFields, bool) {
	if s == nil || s.geoOverrides == nil || !address.IsValid() {
		return base, 0, false
	}
	override, matched := s.geoOverrides.Lookup(address.Unmap())
	if !matched {
		return base, 0, false
	}
	if override.fields&GeoOverrideCountry != 0 {
		base.Country = override.info.Country
	}
	if override.fields&GeoOverrideAdminCode != 0 {
		base.AdminCode = override.info.AdminCode
	}
	if override.fields&GeoOverrideSubdivision != 0 {
		base.Subdivision = override.info.Subdivision
	}
	if override.fields&GeoOverrideCity != 0 {
		base.City = override.info.City
	}
	if override.fields&GeoOverrideISPID != 0 {
		base.ISPID = override.info.ISPID
	}
	if override.fields&GeoOverrideASN != 0 {
		base.ASN = override.info.ASN
	}
	base.Source = "flow_geo_override"
	return base, override.fields, true
}

func (s *CompiledSnapshot) Metadata() SnapshotMetadata {
	if s == nil {
		return SnapshotMetadata{}
	}
	return s.metadata
}

func (s *CompiledSnapshot) lookup(addr netip.Addr) (compiledPrefix, bool) {
	if s == nil || !addr.IsValid() {
		return compiledPrefix{}, false
	}
	return s.prefixes.Lookup(addr.Unmap())
}

type SnapshotCatalog struct {
	state atomic.Pointer[snapshotCatalogState]
}

type snapshotCatalogState struct {
	byTenant map[string][]*CompiledSnapshot
	byID     map[string]*CompiledSnapshot
}

func NewSnapshotCatalog(snapshots ...*CompiledSnapshot) (*SnapshotCatalog, error) {
	catalog := &SnapshotCatalog{}
	catalog.state.Store(&snapshotCatalogState{byTenant: map[string][]*CompiledSnapshot{}, byID: map[string]*CompiledSnapshot{}})
	for _, snapshot := range snapshots {
		if err := catalog.Install(snapshot); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *SnapshotCatalog) Install(snapshot *CompiledSnapshot) error {
	if c == nil || snapshot == nil || snapshot.prefixes == nil {
		return errors.New("compiled dimension snapshot is required")
	}
	for {
		current := c.state.Load()
		if current == nil {
			current = &snapshotCatalogState{byTenant: map[string][]*CompiledSnapshot{}, byID: map[string]*CompiledSnapshot{}}
		}
		if existing, exists := current.byID[snapshot.metadata.SnapshotID]; exists {
			if sameSnapshot(existing, snapshot) {
				return nil
			}
			return errors.New("dimension snapshot id is immutable")
		}
		next := &snapshotCatalogState{
			byTenant: make(map[string][]*CompiledSnapshot, len(current.byTenant)+1),
			byID:     make(map[string]*CompiledSnapshot, len(current.byID)+1),
		}
		for tenantID, existing := range current.byTenant {
			next.byTenant[tenantID] = append([]*CompiledSnapshot(nil), existing...)
		}
		for snapshotID, existing := range current.byID {
			next.byID[snapshotID] = existing
		}
		items := next.byTenant[snapshot.metadata.TenantID]
		for _, existing := range items {
			if existing.metadata.Version == snapshot.metadata.Version {
				return errors.New("dimension snapshot version already exists")
			}
			if existing.metadata.EffectiveFrom.Equal(snapshot.metadata.EffectiveFrom) {
				return errors.New("dimension snapshot effective_from already exists")
			}
			if (existing.metadata.EffectiveFrom.Before(snapshot.metadata.EffectiveFrom) && existing.metadata.Version > snapshot.metadata.Version) ||
				(existing.metadata.EffectiveFrom.After(snapshot.metadata.EffectiveFrom) && existing.metadata.Version < snapshot.metadata.Version) {
				return errors.New("dimension snapshot version and effective_from are not monotonic")
			}
		}
		items = append(items, snapshot)
		sort.Slice(items, func(i, j int) bool {
			return items[i].metadata.EffectiveFrom.Before(items[j].metadata.EffectiveFrom)
		})
		next.byTenant[snapshot.metadata.TenantID] = items
		next.byID[snapshot.metadata.SnapshotID] = snapshot
		if c.state.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (c *SnapshotCatalog) Select(tenantID string, eventTime time.Time) (*CompiledSnapshot, error) {
	if c == nil || !validIdentifier(tenantID, 64) || eventTime.IsZero() {
		return nil, ErrNoDimensionSnapshot
	}
	state := c.state.Load()
	if state == nil {
		return nil, ErrNoDimensionSnapshot
	}
	items := state.byTenant[tenantID]
	index := sort.Search(len(items), func(index int) bool {
		return items[index].metadata.EffectiveFrom.After(eventTime)
	})
	if index == 0 {
		return nil, ErrNoDimensionSnapshot
	}
	return items[index-1], nil
}

func sameSnapshot(left, right *CompiledSnapshot) bool {
	if left == right {
		return true
	}
	return left.metadata.SnapshotID == right.metadata.SnapshotID &&
		left.metadata.TenantID == right.metadata.TenantID &&
		left.metadata.Version == right.metadata.Version &&
		left.metadata.EffectiveFrom.Equal(right.metadata.EffectiveFrom) &&
		left.metadata.Checksum != "" && left.metadata.Checksum == right.metadata.Checksum
}

func normalizeCompileLimits(limits CompileLimits) CompileLimits {
	if limits.MaxBundleBytes <= 0 {
		limits.MaxBundleBytes = defaultMaxBundleBytes
	}
	if limits.MaxPrefixes <= 0 {
		limits.MaxPrefixes = defaultMaxPrefixes
	}
	if limits.MaxAddressSets <= 0 {
		limits.MaxAddressSets = defaultMaxAddressSets
	}
	if limits.MaxAddressSetsPerRecord <= 0 {
		limits.MaxAddressSetsPerRecord = defaultMaxSetsPerRecord
	}
	if limits.MaxSelectorCandidates <= 0 {
		limits.MaxSelectorCandidates = defaultMaxSetCandidates
	}
	return limits
}

func cloneAndValidateLabels(labels map[string]string) (map[string]string, error) {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		if !validText(key, 64) || !validText(value, 256) {
			return nil, fmt.Errorf("label keys and values must be bounded printable UTF-8")
		}
		cloned[key] = value
	}
	return cloned, nil
}

func compileGeoOverride(labels map[string]string) (compiledGeoOverride, bool, error) {
	var override compiledGeoOverride
	for key, value := range labels {
		switch key {
		case "flow.geo.country":
			if !validCountryCode(value) || value == "HK" || value == "MO" || value == "TW" {
				return override, false, errors.New("flow.geo.country must be canonical ISO alpha-2 with HMT represented as CN")
			}
			override.info.Country = value
			override.fields |= GeoOverrideCountry
		case "flow.geo.admin_code":
			if len(value) != 6 || !allDigits(value) || value[:2] == "00" {
				return override, false, errors.New("flow.geo.admin_code must be a six-digit GB/T 2260 code")
			}
			override.info.AdminCode = value
			override.fields |= GeoOverrideAdminCode
		case "flow.geo.subdivision":
			override.info.Subdivision = value
			override.fields |= GeoOverrideSubdivision
		case "flow.geo.city":
			override.info.City = value
			override.fields |= GeoOverrideCity
		case "flow.geo.isp_id":
			parsed, err := strconv.ParseUint(value, 10, 16)
			if err != nil {
				return override, false, errors.New("flow.geo.isp_id must be an unsigned 16-bit decimal")
			}
			override.info.ISPID = uint16(parsed)
			override.fields |= GeoOverrideISPID
		case "flow.geo.asn":
			parsed, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return override, false, errors.New("flow.geo.asn must be an unsigned 32-bit decimal")
			}
			override.info.ASN = uint32(parsed)
			override.fields |= GeoOverrideASN
		case "flow.geo.reason":
		default:
			if strings.HasPrefix(key, "flow.geo.") {
				return override, false, fmt.Errorf("unknown Geo override label %q", key)
			}
		}
	}
	if override.fields == 0 {
		if _, hasReason := labels["flow.geo.reason"]; hasReason {
			return override, false, errors.New("flow.geo.reason requires at least one Geo override field")
		}
		return override, false, nil
	}
	if override.fields&GeoOverrideAdminCode != 0 {
		if override.fields&GeoOverrideCountry == 0 || override.info.Country != "CN" {
			return override, false, errors.New("flow.geo.admin_code requires flow.geo.country=CN")
		}
	}
	return override, true, nil
}

func cloneAndValidateSelector(selector map[string][]string) (map[string][]string, error) {
	if len(selector) == 0 || len(selector) > maxLabelsPerPrefix {
		return nil, errors.New("labels must contain 1..64 entries")
	}
	cloned := make(map[string][]string, len(selector))
	for key, values := range selector {
		if !validText(key, 64) || len(values) == 0 || len(values) > maxSelectorValuesPerLabel {
			return nil, errors.New("selector keys and value counts are invalid")
		}
		unique := make(map[string]struct{}, len(values))
		for _, value := range values {
			if !validText(value, 256) {
				return nil, errors.New("selector values must be bounded printable UTF-8")
			}
			unique[value] = struct{}{}
		}
		canonical := make([]string, 0, len(unique))
		for value := range unique {
			canonical = append(canonical, value)
		}
		sort.Strings(canonical)
		cloned[key] = canonical
	}
	return cloned, nil
}

func precomputePrefixMemberships(prefixes []compiledPrefix, sets []compiledAddressSet, endpointLimit int, candidateLimit int64) error {
	type anchorBuckets map[string][]int
	index := make(map[string]anchorBuckets)
	for setIndex, set := range sets {
		anchorKey := ""
		var anchorValues []string
		for key, values := range set.selector {
			if anchorKey == "" || len(values) < len(anchorValues) || (len(values) == len(anchorValues) && key < anchorKey) {
				anchorKey, anchorValues = key, values
			}
		}
		if index[anchorKey] == nil {
			index[anchorKey] = make(anchorBuckets)
		}
		for _, value := range anchorValues {
			index[anchorKey][value] = append(index[anchorKey][value], setIndex)
		}
	}
	var candidates int64
	for prefixIndex := range prefixes {
		prefix := &prefixes[prefixIndex]
		for key, value := range prefix.labels {
			bucket := index[key][value]
			if int64(len(bucket)) > candidateLimit-candidates {
				return fmt.Errorf("dimension bundle selector candidates exceed compilation limit %d", candidateLimit)
			}
			candidates += int64(len(bucket))
			for _, setIndex := range bucket {
				set := sets[setIndex]
				if !selectorMatches(set.selector, prefix.labels) {
					continue
				}
				if set.direction == DirectionIn || set.direction == DirectionBoth {
					prefix.inAddressSets = append(prefix.inAddressSets, set.id)
				}
				if set.direction == DirectionOut || set.direction == DirectionBoth {
					prefix.outAddressSets = append(prefix.outAddressSets, set.id)
				}
				if len(prefix.inAddressSets) > endpointLimit || len(prefix.outAddressSets) > endpointLimit {
					return fmt.Errorf("dimension bundle address-set endpoint expansion exceeds per-record limit %d", endpointLimit)
				}
			}
		}
		sort.Strings(prefix.inAddressSets)
		sort.Strings(prefix.outAddressSets)
	}
	return nil
}

func estimateMaxSetExpansion(prefixes []compiledPrefix) int {
	if len(prefixes) == 0 {
		return 0
	}
	maximum := 0
	for _, direction := range []BusinessDirection{DirectionIn, DirectionOut} {
		localMaximum, remoteMaximum, hasLocal := 0, 0, false
		for _, prefix := range prefixes {
			count := len(prefix.inAddressSets)
			if direction == DirectionOut {
				count = len(prefix.outAddressSets)
			}
			if prefix.labels["flow"] == "local" {
				hasLocal = true
				if count > localMaximum {
					localMaximum = count
				}
			} else if count > remoteMaximum {
				remoteMaximum = count
			}
		}
		if hasLocal && localMaximum+remoteMaximum > maximum {
			maximum = localMaximum + remoteMaximum
		}
	}
	return maximum
}

func selectorMatches(selector map[string][]string, labels map[string]string) bool {
	for key, allowed := range selector {
		actual, ok := labels[key]
		if !ok {
			return false
		}
		index := sort.SearchStrings(allowed, actual)
		if index == len(allowed) || allowed[index] != actual {
			return false
		}
	}
	return true
}

func parseSHA256Checksum(value string) ([32]byte, error) {
	var checksum [32]byte
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return checksum, errors.New("dimension bundle checksum must be canonical sha256:<lowerhex>")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != len(checksum) {
		return checksum, errors.New("dimension bundle checksum must be canonical sha256:<lowerhex>")
	}
	copy(checksum[:], decoded)
	return checksum, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("request must contain one JSON object")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func validIdentifier(value string, maximum int) bool {
	return validText(value, maximum) && !strings.ContainsAny(value, " /\\")
}

func validText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}
