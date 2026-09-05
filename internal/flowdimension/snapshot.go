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
	defaultMaxSetEvaluations  = 50_000_000
	maxLabelsPerPrefix        = 64
	maxSelectorValuesPerLabel = 256
)

var ErrNoDimensionSnapshot = errors.New("no dimension snapshot for event time")

type CompileLimits struct {
	MaxBundleBytes           int
	MaxPrefixes              int
	MaxAddressSets           int
	MaxAddressSetsPerRecord  int
	MaxAddressSetEvaluations int64
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
	Members        []string      `json:"members,omitempty"`
	ExcludeMembers []string      `json:"exclude_members,omitempty"`
	IncludeSetIDs  []string      `json:"include_set_ids,omitempty"`
	ExcludeSetIDs  []string      `json:"exclude_set_ids,omitempty"`
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
	addressSets  *bart.Table[compiledAddressSetMembership]
	geoOverrides *bart.Table[compiledGeoOverride]
}

type compiledPrefix struct {
	id     string
	cidr   string
	labels map[string]string
}

type compiledAddressSetMembership struct {
	in  []string
	out []string
}

type GeoOverrideFields uint8

const (
	// These values are persisted in flow_records.customer_geo_override_fields.
	// Never renumber them; new fields must use an unused bit.
	GeoOverrideCountry     GeoOverrideFields = 1 << 0
	GeoOverrideAdminCode   GeoOverrideFields = 1 << 1
	GeoOverrideSubdivision GeoOverrideFields = 1 << 2
	GeoOverrideCity        GeoOverrideFields = 1 << 3
	GeoOverrideISPID       GeoOverrideFields = 1 << 4
	GeoOverrideASN         GeoOverrideFields = 1 << 5
	GeoOverrideKnownFields                   = GeoOverrideCountry | GeoOverrideAdminCode | GeoOverrideSubdivision | GeoOverrideCity | GeoOverrideISPID | GeoOverrideASN
)

func (f GeoOverrideFields) Valid() bool {
	return f&^GeoOverrideKnownFields == 0
}

type compiledGeoOverride struct {
	info   GeoInfo
	fields GeoOverrideFields
}

type compiledAddressSet struct {
	id             string
	direction      BusinessDirection
	selector       map[string][]string
	members        []netip.Prefix
	excludeMembers []netip.Prefix
	include        []int
	exclude        []int
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
		parsedPrefixes = append(parsedPrefixes, prefix)
		compiledPrefixes = append(compiledPrefixes, compiled)
		prefixIDs[definition.ID] = struct{}{}
		prefixCIDRs[definition.CIDR] = struct{}{}
	}

	sets, membershipPrefixes, err := compileAddressSetDefinitions(bundle.AddressSets, limits.MaxPrefixes)
	if err != nil {
		return nil, err
	}

	if err := inheritPrefixLabels(parsedPrefixes, compiledPrefixes); err != nil {
		return nil, err
	}
	geoOverrides := &bart.Table[compiledGeoOverride]{}
	for index := range compiledPrefixes {
		override, hasOverride, err := compileGeoOverride(compiledPrefixes[index].labels)
		if err != nil {
			return nil, fmt.Errorf("prefixes[%d].effective labels: %w", index, err)
		}
		if hasOverride {
			geoOverrides.Insert(parsedPrefixes[index], override)
		}
	}
	tree := &bart.Table[compiledPrefix]{}
	for index := range compiledPrefixes {
		tree.Insert(parsedPrefixes[index], compiledPrefixes[index])
	}
	addressSets, maxExpansion, err := compileAddressSetMemberships(
		tree, parsedPrefixes, membershipPrefixes, sets,
		limits.MaxAddressSetsPerRecord, limits.MaxAddressSetEvaluations,
	)
	if err != nil {
		return nil, err
	}
	return &CompiledSnapshot{
		metadata: SnapshotMetadata{
			SnapshotID: bundle.SnapshotID, TenantID: bundle.TenantID, Version: bundle.Version,
			EffectiveFrom: effectiveFrom, Checksum: checksum, PrefixCount: len(compiledPrefixes),
			EnabledAddressSetCount: len(sets), MaxAddressSetsPerRecord: maxExpansion,
		},
		prefixes: tree, addressSets: addressSets, geoOverrides: geoOverrides,
	}, nil
}

// inheritPrefixLabels compiles nested CIDRs into one effective label set per
// LPM result. A more-specific prefix overrides the same key from a covering
// prefix, while labels at different hierarchy levels are retained. Runtime
// lookup therefore remains one allocation-free LPM and address sets can match
// continent/region/country/site labels without ASN being present.
func inheritPrefixLabels(prefixes []netip.Prefix, values []compiledPrefix) error {
	if len(prefixes) != len(values) {
		return errors.New("dimension prefix compilation is inconsistent")
	}
	tree := &bart.Table[compiledPrefix]{}
	for index := range prefixes {
		tree.Insert(prefixes[index], values[index])
	}
	for index, prefix := range prefixes {
		chain := make([]compiledPrefix, 0, 8)
		for _, value := range tree.Supernets(prefix) {
			chain = append(chain, value)
		}
		effective := make(map[string]string)
		for position := len(chain) - 1; position >= 0; position-- {
			for key, value := range chain[position].labels {
				effective[key] = value
			}
		}
		if len(effective) > maxLabelsPerPrefix {
			return fmt.Errorf("prefixes[%d] effective labels exceed limit %d", index, maxLabelsPerPrefix)
		}
		values[index].labels = effective
	}
	return nil
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
		base.ContinentID = ""
		base.RegionID = ""
		base.CountryID = override.info.Country
		base.ProvinceID = ""
		base.CityID = ""
	}
	if override.fields&GeoOverrideAdminCode != 0 {
		base.AdminCode = override.info.AdminCode
		base.ProvinceID = ""
		base.CityID = ""
		if base.Country == "CN" && len(base.AdminCode) == 6 {
			base.ProvinceID = base.AdminCode[:2] + "0000"
			if !strings.HasSuffix(base.AdminCode, "0000") {
				base.CityID = base.AdminCode
			}
		}
	}
	if override.fields&GeoOverrideSubdivision != 0 {
		base.Subdivision = override.info.Subdivision
	}
	if override.fields&GeoOverrideCity != 0 {
		base.City = override.info.City
		if override.fields&GeoOverrideAdminCode == 0 {
			base.CityID = ""
		}
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
	if limits.MaxAddressSetEvaluations <= 0 {
		limits.MaxAddressSetEvaluations = defaultMaxSetEvaluations
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
		return checksum, errors.New("bundle checksum must be canonical sha256:<lowerhex>")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != len(checksum) {
		return checksum, errors.New("bundle checksum must be canonical sha256:<lowerhex>")
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
