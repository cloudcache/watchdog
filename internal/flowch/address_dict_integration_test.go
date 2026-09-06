// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// TestRealClickHouseAddressDictionaryResolvesGeoAndGroups validates step 1 of
// docs/flow-address-query-plan.md: publish the address library as a versioned
// IP_TRIE dictionary and resolve an IP's geo + admin-group membership with
// dictGet, instead of baking classification into flow_records at ingest. It
// pins the two mechanics the rest of the plan depends on:
//
//   - an IPv4-mapped-IPv6 lookup matches an IPv4 prefix — flow_records stores
//     src_ip/dst_ip as IPv6, so every query-time lookup arrives as ::ffff:v4;
//   - bumping dict_version re-resolves classification with no change to any
//     stored fact — reclassification becomes "publish a new version", not a
//     rewrite (which is what makes rollup-reclassify = re-run rollup in step 2).
func TestRealClickHouseAddressDictionaryResolvesGeoAndGroups(t *testing.T) {
	const db = "watchdog_flow_it_address_dict"
	ctx, native := openDataIntegrationClickHouse(t, db)
	createAddressDictionary(t, ctx, native, db)

	insertAddressDictSource(t, ctx, native, 1, []addressDictRow{
		{prefix: "203.0.113.0/24", countryID: "CN", provinceID: "330000", groupIDs: []string{"east-china"}},
		{prefix: "198.51.100.0/24", countryID: "US", provinceID: "US-CA", groupIDs: []string{"overseas-a"}},
	})
	reloadAddressDictionary(t, ctx, native, db)

	got := lookupAddressDict(t, ctx, native, db, "203.0.113.5")
	if got.countryID != "CN" || got.provinceID != "330000" || !addressDictContains(got.groupIDs, "east-china") {
		t.Fatalf("v1 lookup of 203.0.113.5 = %+v, want CN/330000/[east-china]", got)
	}
	other := lookupAddressDict(t, ctx, native, db, "198.51.100.7")
	if other.countryID != "US" || !addressDictContains(other.groupIDs, "overseas-a") {
		t.Fatalf("v1 lookup of 198.51.100.7 = %+v, want US/[overseas-a]", other)
	}
	miss := lookupAddressDict(t, ctx, native, db, "10.0.0.1")
	if miss.countryID != "" || len(miss.groupIDs) != 0 {
		t.Fatalf("uncovered lookup of 10.0.0.1 = %+v, want defaults", miss)
	}

	// A new version re-classifies the same prefix. The dictionary source loads
	// the latest version only, so dictGet reflects the new definition after a
	// reload — with no change to any stored fact.
	insertAddressDictSource(t, ctx, native, 2, []addressDictRow{
		{prefix: "203.0.113.0/24", countryID: "US", provinceID: "US-WA", groupIDs: []string{"overseas-a", "reclassified"}},
	})
	reloadAddressDictionary(t, ctx, native, db)
	after := lookupAddressDict(t, ctx, native, db, "203.0.113.5")
	if after.countryID != "US" || after.provinceID != "US-WA" || !addressDictContains(after.groupIDs, "reclassified") {
		t.Fatalf("v2 lookup of 203.0.113.5 = %+v, want US/US-WA/[...reclassified]", after)
	}
}

type addressDictRow struct {
	prefix     string
	countryID  string
	provinceID string
	groupIDs   []string
}

type addressDictResult struct {
	countryID  string
	provinceID string
	groupIDs   []string
}

// createAddressDictionary defines the runtime IP_TRIE dictionary over the
// migrated source table. The dictionary is created here rather than in the
// migration because its ClickHouse source needs injected credentials; the
// source query loads the latest dict_version so a reload picks up new versions.
func createAddressDictionary(t *testing.T, ctx context.Context, native *NativeInserter, db string) {
	t.Helper()
	cfg := realMigrationConfig(t, "watchdog-flow-address-dict-source", 30*time.Second)
	host, port, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		t.Fatalf("split clickhouse address %q: %v", cfg.Address, err)
	}
	escape := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	sourceQuery := fmt.Sprintf(
		"SELECT prefix, country_id, province_id, group_ids FROM %s.flow_address_dict_source "+
			"WHERE dict_version = (SELECT max(dict_version) FROM %s.flow_address_dict_source)", db, db)
	ddl := fmt.Sprintf(`CREATE DICTIONARY %s.flow_address (
  prefix String,
  country_id String DEFAULT '',
  province_id String DEFAULT '',
  group_ids Array(String)
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(HOST '%s' PORT %s USER '%s' PASSWORD '%s' DB '%s' QUERY '%s'))
LAYOUT(IP_TRIE())
LIFETIME(0)`, db, escape(host), port, escape(cfg.User), escape(cfg.Password), db, escape(sourceQuery))
	if err := native.executor.Do(ctx, ch.Query{Body: ddl}); err != nil {
		t.Fatalf("create address dictionary: %v", err)
	}
}

func insertAddressDictSource(t *testing.T, ctx context.Context, native *NativeInserter, version uint64, rows []addressDictRow) {
	t.Helper()
	var (
		dictVersion proto.ColUInt64
		prefix      proto.ColStr
		countryID   = new(proto.ColStr).LowCardinality()
		provinceID  = new(proto.ColStr).LowCardinality()
		groupIDs    = new(proto.ColStr).Array()
	)
	for _, row := range rows {
		dictVersion.Append(version)
		prefix.Append(row.prefix)
		countryID.Append(row.countryID)
		provinceID.Append(row.provinceID)
		groupIDs.Append(row.groupIDs)
	}
	input := proto.Input{
		{Name: "dict_version", Data: dictVersion},
		{Name: "prefix", Data: prefix},
		{Name: "country_id", Data: countryID},
		{Name: "province_id", Data: provinceID},
		{Name: "group_ids", Data: groupIDs},
	}
	query := ch.Query{Body: input.Into("flow_address_dict_source"), Input: input}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("insert address dict source version %d: %v", version, err)
	}
}

func reloadAddressDictionary(t *testing.T, ctx context.Context, native *NativeInserter, db string) {
	t.Helper()
	if err := native.executor.Do(ctx, ch.Query{Body: "SYSTEM RELOAD DICTIONARY " + db + ".flow_address"}); err != nil {
		t.Fatalf("reload address dictionary: %v", err)
	}
}

// lookupAddressDict resolves an IP through the dictionary the way the query and
// rollup paths will: as an IPv4-mapped IPv6 key, matching how flow_records
// stores addresses.
func lookupAddressDict(t *testing.T, ctx context.Context, native *NativeInserter, db, ip string) addressDictResult {
	t.Helper()
	var (
		countryID  proto.ColStr
		provinceID proto.ColStr
		groupIDs   = new(proto.ColStr).Array()
	)
	body := fmt.Sprintf(
		"SELECT dictGetString('%s.flow_address', 'country_id', tuple(toIPv6({ip:String}))) AS country_id, "+
			"dictGetString('%s.flow_address', 'province_id', tuple(toIPv6({ip:String}))) AS province_id, "+
			"dictGet('%s.flow_address', 'group_ids', tuple(toIPv6({ip:String}))) AS group_ids", db, db, db)
	var result addressDictResult
	found := false
	query := ch.Query{
		Body:       body,
		Parameters: ch.Parameters(map[string]any{"ip": ip}),
		Result: proto.Results{
			{Name: "country_id", Data: &countryID},
			{Name: "province_id", Data: &provinceID},
			{Name: "group_ids", Data: groupIDs},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			if block.Rows == 0 {
				return nil
			}
			if block.Rows != 1 || found {
				return fmt.Errorf("address dict lookup returned %d rows", block.Rows)
			}
			result = addressDictResult{
				countryID:  countryID.Row(0),
				provinceID: provinceID.Row(0),
				groupIDs:   append([]string(nil), groupIDs.Row(0)...),
			}
			found = true
			return nil
		},
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("lookup address dict for %s: %v", ip, err)
	}
	if !found {
		t.Fatalf("lookup address dict for %s returned no row", ip)
	}
	return result
}

func addressDictContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
