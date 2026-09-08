package address

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/opjob"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const defaultAddressSnapshotBuildPageSize = 5_000

// BuildAddressSnapshotPublication compiles the preview-pinned definition and
// import generations outside a long transaction, then a short final transaction
// revalidates the digest and version under the installation lock before making
// the verified WADS object visible as an active, pending-approval snapshot.
// De-tenanted port; the WADS builder, object writer, and commit logic are
// unchanged. The snapshot id is the build job id (idempotent build).
func (p *Publisher) BuildAddressSnapshotPublication(ctx context.Context, actorID, buildJobID ID, request AddressDimensionPublishRequest) (AddressDimensionSnapshot, error) {
	if p == nil || p.store == nil || actorID == "" || buildJobID == "" || !isUTCMinute(request.EffectiveFrom) || !validSHA256Digest(request.PreviewDigest) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	if existing, err := p.getAddressSnapshotByBuildJob(ctx, buildJobID); err == nil {
		if existing.DraftDigest != request.PreviewDigest || !existing.EffectiveFrom.Equal(request.EffectiveFrom) {
			return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AddressDimensionSnapshot{}, err
	}

	draft, digest, version, err := p.loadAddressSnapshotBuildDefinition(ctx)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if digest != request.PreviewDigest {
		return AddressDimensionSnapshot{}, ErrAddressDimensionDraftChanged
	}
	_, definition, _, err := encodeAddressDimensionBundle(draft, string(buildJobID), version, request.EffectiveFrom)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	sources, supplierGeo, supplierOperators, err := p.loadAddressSnapshotBuildSources(ctx, draft.Sources)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	build, err := flowdimension.BuildAddressSnapshotContext(ctx, flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: AddressSnapshotBuilderVersion,
		Sources: sources, SupplierGeoNodes: supplierGeo, SupplierOperators: supplierOperators,
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		return AddressDimensionSnapshot{}, fmt.Errorf("%w: %v", ErrAddressDimensionInvalid, err)
	}
	object, err := p.objects.SaveDimensionObject(ctx, buildJobID, build.Data)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if object.Checksum != build.ChecksumSHA256 {
		return AddressDimensionSnapshot{}, errors.New("address snapshot checksum changed while saving")
	}
	snapshot, err := p.commitAddressSnapshotPublication(ctx, actorID, buildJobID, request, draft, version, object, build)
	if err != nil {
		// Keep the object on commit failure: the path is a content-fenced function
		// of the build job id; the publication orphan sweeper owns eventual cleanup.
		return AddressDimensionSnapshot{}, err
	}
	return snapshot, nil
}

func (p *Publisher) getAddressSnapshotByBuildJob(ctx context.Context, buildJobID ID) (AddressDimensionSnapshot, error) {
	return scanAddressDimensionSnapshot(p.store.db.QueryRowContext(ctx, `SELECT `+addressDimensionSnapshotColumns+`
		FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ? AND build_job_id = ?`,
		p.scope.ModuleKey, p.scope.DimensionKey, buildJobID))
}

func (p *Publisher) loadAddressSnapshotBuildDefinition(ctx context.Context) (AddressDimensionDraft, string, uint64, error) {
	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return AddressDimensionDraft{}, "", 0, err
	}
	defer tx.Rollback()
	draft, digest, err := loadAddressDimensionDraft(ctx, tx, false)
	if err != nil {
		return AddressDimensionDraft{}, "", 0, err
	}
	var version uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ?`, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&version); err != nil {
		return AddressDimensionDraft{}, "", 0, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionDraft{}, "", 0, err
	}
	return draft, digest, version, nil
}

func (p *Publisher) commitAddressSnapshotPublication(ctx context.Context, actorID, buildJobID ID, request AddressDimensionPublishRequest, builtDraft AddressDimensionDraft, version uint64, object DimensionObject, build flowdimension.AddressSnapshotBuildResult) (AddressDimensionSnapshot, error) {
	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if existing, err := getAddressSnapshotByBuildJobTx(ctx, tx, p.scope, buildJobID); err == nil {
		if existing.DraftDigest != request.PreviewDigest || !existing.EffectiveFrom.Equal(request.EffectiveFrom) || existing.Checksum != object.Checksum {
			return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
		}
		if err := tx.Commit(); err != nil {
			return AddressDimensionSnapshot{}, err
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AddressDimensionSnapshot{}, err
	}
	currentDraft, digest, err := loadAddressDimensionDraft(ctx, tx, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if digest != request.PreviewDigest || digest != addressDimensionDraftDigest(builtDraft) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionDraftChanged
	}
	var nextVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ?`, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&nextVersion); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if nextVersion != version {
		return AddressDimensionSnapshot{}, ErrAddressSnapshotBuildRace
	}
	sourceManifest, err := json.Marshal(currentDraft.Sources)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	sourcePrefixCount, err := countAddressDimensionSourcePrefixes(currentDraft.Sources)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	artifact := build.Artifact
	entryCount := len(artifact.IPv4Ranges) + len(artifact.IPv6Ranges) + len(artifact.GeoNodes) + len(artifact.Operators) + len(artifact.AddressSets)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest, source_prefix_count,
			bundle_schema_version, entry_count, prefix_count, address_set_count,
			max_address_sets_per_record, status, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', NULLIF(?, ''))
	`, buildJobID, p.scope.ModuleKey, p.scope.DimensionKey, version, request.EffectiveFrom.UTC(),
		object.Ref, AddressSnapshotObjectFormat, flowdimension.AddressSnapshotFormatVersion, AddressSnapshotBuilderVersion, buildJobID,
		object.Checksum, digest, AddressDimensionSourceManifestV1, sourceManifest, sourcePrefixCount,
		flowdimension.BundleSchemaVersion, entryCount, len(currentDraft.Prefixes), len(currentDraft.AddressSets),
		definitionMaxAddressSets(build), actorID)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return AddressDimensionSnapshot{}, ErrAddressSnapshotBuildRace
		}
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, buildJobID, "dimension.snapshot.built", map[string]any{
		"version": version, "checksum": object.Checksum, "object_format": AddressSnapshotObjectFormat,
		"builder_version": AddressSnapshotBuilderVersion, "source_prefix_count": sourcePrefixCount,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetAddressDimensionSnapshot(ctx, buildJobID)
}

func addressDimensionDraftDigest(draft AddressDimensionDraft) string {
	data, err := json.Marshal(draft)
	if err != nil {
		return ""
	}
	return sha256Checksum(data)
}

func sha256Checksum(data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest)
}

func definitionMaxAddressSets(build flowdimension.AddressSnapshotBuildResult) uint32 {
	maximum := 0
	for _, value := range build.Artifact.Values {
		if count := max(len(value.InAddressSetIDs), len(value.OutAddressSetIDs)); count > maximum {
			maximum = count
		}
	}
	return uint32(maximum)
}

func getAddressSnapshotByBuildJobTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, buildJobID ID) (AddressDimensionSnapshot, error) {
	return scanAddressDimensionSnapshot(tx.QueryRowContext(ctx, `SELECT `+addressDimensionSnapshotColumns+`
		FROM dimension_snapshots WHERE module_key = ? AND dimension_key = ? AND build_job_id = ? FOR UPDATE`,
		scope.ModuleKey, scope.DimensionKey, buildJobID))
}

type addressSnapshotImportPrefix struct {
	id           uint64
	bits         uint8
	start        netip.Addr
	end          netip.Addr
	geo          *flowdimension.AddressSnapshotBuildGeo
	supplierKey  string
	supplierName string
	ispID        uint16
	asn          uint32
}

func (p addressSnapshotImportPrefix) buildRange() flowdimension.AddressSnapshotBuildRange {
	value := flowdimension.AddressSnapshotBuildRange{Start: p.start, End: p.end, ISPID: p.ispID, ASN: p.asn}
	if p.geo != nil {
		value.Geo = *p.geo
	}
	return value
}

type addressSnapshotLoadedSource struct {
	manifest   AddressDimensionSource
	prefixes   []addressSnapshotImportPrefix
	rowCountV4 uint64
	rowCountV6 uint64
}

type addressSnapshotSupplierEvidence struct {
	name string
	asns map[uint32]struct{}
}

func (p *Publisher) loadAddressSnapshotBuildSources(ctx context.Context, manifest []AddressDimensionSource) ([]flowdimension.AddressSnapshotBuildSource, []flowdimension.AddressSnapshotBuildGeoNode, []flowdimension.AddressSnapshotBuildOperator, error) {
	loaded := make([]addressSnapshotLoadedSource, 0, len(manifest))
	geoNodes := make(map[string]flowdimension.AddressSnapshotBuildGeoNode)
	supplierEvidence := make(map[string]*addressSnapshotSupplierEvidence)
	progress := uint64(0)
	for _, source := range manifest {
		prefixes, count4, count6, err := p.loadAddressSnapshotImportPrefixes(ctx, source, geoNodes, &progress)
		if err != nil {
			return nil, nil, nil, err
		}
		if count4 != source.RowCountV4 || count6 != source.RowCountV6 {
			return nil, nil, nil, fmt.Errorf("%w: source %s row count changed", ErrAddressDimensionInvalid, source.ImportID)
		}
		if source.Slot != AddressImportSlotGeo {
			for _, item := range prefixes {
				if item.supplierKey == "" {
					continue
				}
				evidence := supplierEvidence[item.supplierKey]
				if evidence == nil {
					evidence = &addressSnapshotSupplierEvidence{name: item.supplierName, asns: map[uint32]struct{}{}}
					supplierEvidence[item.supplierKey] = evidence
				}
				if item.supplierName < evidence.name {
					evidence.name = item.supplierName
				}
				if item.asn != 0 {
					evidence.asns[item.asn] = struct{}{}
				}
			}
		}
		loaded = append(loaded, addressSnapshotLoadedSource{manifest: source, prefixes: prefixes, rowCountV4: count4, rowCountV6: count6})
	}
	supplierIDs, supplierOperators, err := p.ensureAddressSnapshotSupplierOperators(ctx, supplierEvidence)
	if err != nil {
		return nil, nil, nil, err
	}
	result := make([]flowdimension.AddressSnapshotBuildSource, 0, len(loaded))
	for _, source := range loaded {
		for index := range source.prefixes {
			if source.manifest.Slot != AddressImportSlotGeo && source.prefixes[index].supplierKey != "" {
				source.prefixes[index].ispID = supplierIDs[source.prefixes[index].supplierKey]
			}
		}
		ranges, err := normalizeAddressSnapshotImportPrefixes(source.prefixes)
		if err != nil {
			return nil, nil, nil, err
		}
		result = append(result, flowdimension.AddressSnapshotBuildSource{
			Slot: source.manifest.Slot, ImportID: string(source.manifest.ImportID), ChecksumSHA256: "sha256:" + source.manifest.ChecksumSHA256,
			SlotRowVersion: source.manifest.SlotRowVersion, RowCountV4: source.rowCountV4, RowCountV6: source.rowCountV6, Ranges: ranges,
		})
	}
	nodes := make([]flowdimension.AddressSnapshotBuildGeoNode, 0, len(geoNodes))
	for _, node := range geoNodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return result, nodes, supplierOperators, nil
}

func canonicalAddressSnapshotSupplierKey(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(name), " "))
}

// ensureAddressSnapshotSupplierOperators allocates IDs only for exact supplier
// names present in the pinned source. De-tenanted: the sequence is a singleton row.
func (p *Publisher) ensureAddressSnapshotSupplierOperators(ctx context.Context, evidence map[string]*addressSnapshotSupplierEvidence) (map[string]uint16, []flowdimension.AddressSnapshotBuildOperator, error) {
	ids := make(map[string]uint16, len(evidence))
	if len(evidence) == 0 {
		return ids, nil, nil
	}
	if len(evidence) > 65_535 {
		return nil, nil, fmt.Errorf("%w: supplier operator identity limit exceeded", ErrAddressDimensionInvalid)
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return nil, nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT supplier_key, flow_isp_id FROM address_supplier_operators`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var key string
		var id uint16
		if err := rows.Scan(&key, &id); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if _, used := evidence[key]; used {
			ids[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	missing := make([]string, 0, len(evidence)-len(ids))
	for key := range evidence {
		if _, exists := ids[key]; !exists {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO address_supplier_operator_sequences (id, next_flow_isp_id) VALUES (1, 1)`); err != nil {
			return nil, nil, err
		}
		var next uint32
		if err := tx.QueryRowContext(ctx, `SELECT next_flow_isp_id FROM address_supplier_operator_sequences WHERE id = 1 FOR UPDATE`).Scan(&next); err != nil {
			return nil, nil, err
		}
		if next == 0 || uint64(next)+uint64(len(missing))-1 > 65_535 {
			return nil, nil, fmt.Errorf("%w: supplier operator ID space exhausted", ErrAddressDimensionInvalid)
		}
		for _, key := range missing {
			id := uint16(next)
			if _, err := tx.ExecContext(ctx, `INSERT INTO address_supplier_operators (supplier_key, flow_isp_id, name) VALUES (?, ?, ?)`, key, id, evidence[key].name); err != nil {
				return nil, nil, err
			}
			ids[key] = id
			next++
		}
		if _, err := tx.ExecContext(ctx, `UPDATE address_supplier_operator_sequences SET next_flow_isp_id = ? WHERE id = 1`, next); err != nil {
			return nil, nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	operators := make([]flowdimension.AddressSnapshotBuildOperator, 0, len(evidence))
	for key, item := range evidence {
		asns := make([]uint32, 0, len(item.asns))
		for asn := range item.asns {
			asns = append(asns, asn)
		}
		sort.Slice(asns, func(i, j int) bool { return asns[i] < asns[j] })
		operators = append(operators, flowdimension.AddressSnapshotBuildOperator{
			ID: ids[key], StableID: "supplier/operator/" + url.PathEscape(key), Code: key,
			Name: item.name, Category: "supplier", ASNs: asns, Enabled: true,
		})
	}
	sort.Slice(operators, func(i, j int) bool { return operators[i].ID < operators[j].ID })
	return ids, operators, nil
}

func (p *Publisher) loadAddressSnapshotImportPrefixes(ctx context.Context, source AddressDimensionSource, geoNodes map[string]flowdimension.AddressSnapshotBuildGeoNode, progress *uint64) ([]addressSnapshotImportPrefix, uint64, uint64, error) {
	const columns = `id, family, prefix_length, cidr, ip_start, ip_end,
		COALESCE(continent_code, ''), COALESCE(country_code, ''), COALESCE(country_name, ''),
		COALESCE(subdivision_code, ''), COALESCE(subdivision_name, ''),
		COALESCE(city_code, ''), COALESCE(city_name, ''), COALESCE(asn, 0), COALESCE(operator_name, '')`
	var all []addressSnapshotImportPrefix
	var cursorFamily uint8
	var cursorStart []byte
	var cursorBits uint8
	var cursorID uint64
	var count4, count6 uint64
	geoValues := make(map[flowdimension.AddressSnapshotBuildGeo]*flowdimension.AddressSnapshotBuildGeo)
	for {
		query := `SELECT ` + columns + ` FROM address_base_prefixes WHERE import_id = ?`
		args := []any{source.ImportID}
		if cursorStart != nil {
			query += ` AND (family > ? OR (family = ? AND (ip_start > ? OR (ip_start = ? AND (prefix_length > ? OR (prefix_length = ? AND id > ?))))))`
			args = append(args, cursorFamily, cursorFamily, cursorStart, cursorStart, cursorBits, cursorBits, cursorID)
		}
		query += ` ORDER BY family, ip_start, prefix_length, id LIMIT ?`
		args = append(args, defaultAddressSnapshotBuildPageSize)
		rows, err := p.store.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, 0, 0, err
		}
		page := 0
		for rows.Next() {
			var id uint64
			var family, bits uint8
			var cidr, continent, country, countryName, subdivision, subdivisionName, city, cityName string
			var startBytes, endBytes []byte
			var asn uint32
			var operatorName string
			if err := rows.Scan(&id, &family, &bits, &cidr, &startBytes, &endBytes, &continent, &country, &countryName, &subdivision, &subdivisionName, &city, &cityName, &asn, &operatorName); err != nil {
				rows.Close()
				return nil, 0, 0, err
			}
			prefix, err := validateAddressSnapshotStoredPrefix(family, bits, cidr, startBytes, endBytes)
			if err != nil {
				rows.Close()
				return nil, 0, 0, err
			}
			geo := registerAddressSnapshotSupplierGeo(geoNodes, continent, country, countryName, subdivision, subdivisionName, city, cityName)
			var geoValue *flowdimension.AddressSnapshotBuildGeo
			if geo != (flowdimension.AddressSnapshotBuildGeo{}) {
				geoValue = geoValues[geo]
				if geoValue == nil {
					copy := geo
					geoValue = &copy
					geoValues[geo] = geoValue
				}
			}
			start, end := addressSnapshotPrefixRange(prefix)
			operatorName = strings.TrimSpace(operatorName)
			all = append(all, addressSnapshotImportPrefix{
				id: id, bits: bits, start: start, end: end, geo: geoValue,
				supplierKey: canonicalAddressSnapshotSupplierKey(operatorName), supplierName: operatorName, asn: asn,
			})
			if family == 4 {
				count4++
			} else {
				count6++
			}
			cursorFamily, cursorStart, cursorBits, cursorID = family, append(cursorStart[:0], startBytes...), bits, id
			page++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, 0, 0, err
		}
		if err := rows.Close(); err != nil {
			return nil, 0, 0, err
		}
		*progress += uint64(page)
		if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
			if err := reporter.Report(ctx, *progress, nil); err != nil {
				return nil, 0, 0, err
			}
		}
		if page < defaultAddressSnapshotBuildPageSize {
			break
		}
	}
	return all, count4, count6, nil
}

func validateAddressSnapshotStoredPrefix(family, bits uint8, cidr string, startBytes, endBytes []byte) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || prefix != prefix.Masked() || prefix.Bits() != int(bits) || (family == 4) != prefix.Addr().Is4() || len(startBytes) != 16 || len(endBytes) != 16 {
		return netip.Prefix{}, fmt.Errorf("%w: imported prefix %q is not canonical", ErrAddressDimensionInvalid, cidr)
	}
	wantStart, wantEnd := addressPrefixBounds(prefix)
	if string(startBytes) != string(wantStart[:]) || string(endBytes) != string(wantEnd[:]) {
		return netip.Prefix{}, fmt.Errorf("%w: imported prefix %q has inconsistent bounds", ErrAddressDimensionInvalid, cidr)
	}
	return prefix, nil
}

func addressSnapshotPrefixRange(prefix netip.Prefix) (netip.Addr, netip.Addr) {
	startBytes, endBytes := addressPrefixBounds(prefix)
	if prefix.Addr().Is4() {
		return netip.AddrFrom4([4]byte(startBytes[12:])), netip.AddrFrom4([4]byte(endBytes[12:]))
	}
	return netip.AddrFrom16(startBytes), netip.AddrFrom16(endBytes)
}

func registerAddressSnapshotSupplierGeo(nodes map[string]flowdimension.AddressSnapshotBuildGeoNode, continent, country, countryName, subdivision, subdivisionName, city, cityName string) flowdimension.AddressSnapshotBuildGeo {
	component := func(value string) string { return url.PathEscape(strings.TrimSpace(value)) }
	put := func(id, kind, code, name, parent string) string {
		if strings.TrimSpace(code) == "" {
			return ""
		}
		candidate := flowdimension.AddressSnapshotBuildGeoNode{ID: id, Kind: kind, Code: strings.TrimSpace(code), Name: firstNonEmpty(name, code), ParentID: parent, Enabled: true}
		if current, exists := nodes[id]; !exists || candidate.Name < current.Name {
			nodes[id] = candidate
		}
		return id
	}
	continentID := ""
	if continent != "" {
		continentID = put("supplier/continent/"+component(continent), GeoKindContinent, continent, continent, "")
	}
	countryID := ""
	if country != "" {
		countryID = put("supplier/country/"+component(country), GeoKindCountry, country, countryName, continentID)
	}
	provinceID := ""
	if subdivision != "" {
		provinceID = put("supplier/province/"+component(country)+"/"+component(subdivision), GeoKindProvince, subdivision, subdivisionName, countryID)
	}
	cityID := ""
	if city != "" || cityName != "" {
		cityCode := firstNonEmpty(city, cityName)
		cityID = put("supplier/city/"+component(country)+"/"+component(subdivision)+"/"+component(cityCode), GeoKindCity, cityCode, cityName, firstNonEmpty(provinceID, countryID))
	}
	admin := firstNonEmpty(city, subdivision)
	return flowdimension.AddressSnapshotBuildGeo{
		CountryCode: strings.TrimSpace(country), AdminCode: admin, Subdivision: strings.TrimSpace(subdivision), City: strings.TrimSpace(city),
		ContinentID: continentID, CountryID: countryID, ProvinceID: provinceID, CityID: cityID,
	}
}

type addressSnapshotPrefixEvent struct {
	address netip.Addr
	add     bool
	index   int
}

type addressSnapshotPrefixHeap struct {
	items    []int
	prefixes []addressSnapshotImportPrefix
}

func (h addressSnapshotPrefixHeap) Len() int { return len(h.items) }
func (h addressSnapshotPrefixHeap) Less(i, j int) bool {
	left, right := h.prefixes[h.items[i]], h.prefixes[h.items[j]]
	if left.bits != right.bits {
		return left.bits > right.bits
	}
	return left.id > right.id
}
func (h addressSnapshotPrefixHeap) Swap(i, j int)   { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *addressSnapshotPrefixHeap) Push(value any) { h.items = append(h.items, value.(int)) }
func (h *addressSnapshotPrefixHeap) Pop() any {
	old := h.items
	value := old[len(old)-1]
	h.items = old[:len(old)-1]
	return value
}

func normalizeAddressSnapshotImportPrefixes(prefixes []addressSnapshotImportPrefix) ([]flowdimension.AddressSnapshotBuildRange, error) {
	if ranges, canonical := normalizeDisjointAddressSnapshotImportPrefixes(prefixes); canonical {
		return ranges, nil
	}
	var result []flowdimension.AddressSnapshotBuildRange
	for _, family4 := range []bool{true, false} {
		var family []addressSnapshotImportPrefix
		for _, item := range prefixes {
			if item.start.Is4() == family4 {
				family = append(family, item)
			}
		}
		if len(family) == 0 {
			continue
		}
		events := make([]addressSnapshotPrefixEvent, 0, len(family)*2)
		for index, item := range family {
			events = append(events, addressSnapshotPrefixEvent{address: item.start, add: true, index: index})
			if next := item.end.Next(); next.IsValid() {
				events = append(events, addressSnapshotPrefixEvent{address: next, index: index})
			}
		}
		sort.Slice(events, func(i, j int) bool {
			if comparison := events[i].address.Compare(events[j].address); comparison != 0 {
				return comparison < 0
			}
			return !events[i].add && events[j].add
		})
		active := make([]bool, len(family))
		priority := &addressSnapshotPrefixHeap{prefixes: family}
		heap.Init(priority)
		var previous netip.Addr
		previousTop := -1
		for offset := 0; offset < len(events); {
			boundary := events[offset].address
			if previous.IsValid() && previousTop >= 0 {
				appendAddressSnapshotNormalizedRange(&result, previous, boundary.Prev(), family[previousTop].buildRange())
			}
			for offset < len(events) && events[offset].address == boundary && !events[offset].add {
				active[events[offset].index] = false
				offset++
			}
			for offset < len(events) && events[offset].address == boundary {
				active[events[offset].index] = true
				heap.Push(priority, events[offset].index)
				offset++
			}
			for priority.Len() != 0 && !active[priority.items[0]] {
				heap.Pop(priority)
			}
			previous, previousTop = boundary, -1
			if priority.Len() != 0 {
				previousTop = priority.items[0]
			}
		}
		if previous.IsValid() && previousTop >= 0 {
			appendAddressSnapshotNormalizedRange(&result, previous, family[previousTop].end, family[previousTop].buildRange())
		}
	}
	return result, nil
}

func normalizeDisjointAddressSnapshotImportPrefixes(prefixes []addressSnapshotImportPrefix) ([]flowdimension.AddressSnapshotBuildRange, bool) {
	mergedCount := 0
	lastFamily := 0
	var lastEnd netip.Addr
	var lastValue flowdimension.AddressSnapshotBuildRange
	for _, item := range prefixes {
		family := item.start.BitLen()
		if family != 32 && family != 128 {
			return nil, false
		}
		if family < lastFamily || (family == lastFamily && lastEnd.IsValid() && lastEnd.Compare(item.start) >= 0) {
			return nil, false
		}
		if family != lastFamily {
			lastFamily = family
			lastEnd = netip.Addr{}
		}
		value := item.buildRange()
		if !lastEnd.IsValid() || lastEnd.Next() != item.start || !sameAddressSnapshotBuildRangeValue(lastValue, value) {
			mergedCount++
		}
		lastValue, lastEnd = value, item.end
	}
	result := make([]flowdimension.AddressSnapshotBuildRange, 0, mergedCount)
	for _, item := range prefixes {
		appendAddressSnapshotNormalizedRange(&result, item.start, item.end, item.buildRange())
	}
	return result, true
}

func sameAddressSnapshotBuildRangeValue(left, right flowdimension.AddressSnapshotBuildRange) bool {
	left.Start, left.End = netip.Addr{}, netip.Addr{}
	right.Start, right.End = netip.Addr{}, netip.Addr{}
	return left == right
}

func appendAddressSnapshotNormalizedRange(result *[]flowdimension.AddressSnapshotBuildRange, start, end netip.Addr, source flowdimension.AddressSnapshotBuildRange) {
	if !start.IsValid() || !end.IsValid() || start.Compare(end) > 0 {
		return
	}
	value := source
	value.Start, value.End = start, end
	if len(*result) != 0 {
		last := &(*result)[len(*result)-1]
		if sameAddressSnapshotBuildRangeValue(*last, value) && last.End.Next().IsValid() && last.End.Next() == start {
			last.End = end
			return
		}
	}
	*result = append(*result, value)
}
