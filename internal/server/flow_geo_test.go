// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
)

// flowGeoService is a thin adapter over the Flow data-plane's authoritative
// flowdimension.GeoCatalog. These tests exercise the adapter (reload/lookup/
// status, keep-serving-after-failed-reload, version isolation) and the v2 HTTP
// surface against a bundle built with flowdimension's exported flow-geo format;
// the loader's own validation is tested in the flowdimension package.

func writeFlowGeoBundle(t *testing.T) string {
	t.Helper()
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn"
	ipv4 := header + "\n" +
		"1.96.0.0,1.96.255.255,CN,810000,,,1,4760\n" +
		"8.8.8.0,8.8.8.255,US,,CA,,0,15169\n"
	ipv6 := header + "\n" +
		"2400::,2400:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CN,440000,,,1,4134\n"
	operators, _ := json.Marshal([]flowdimension.GeoOperator{
		{ID: 1, Name: "中国电信", ShortName: "电信", Category: "carrier", Enabled: true},
	})
	dictionary, _ := json.Marshal([]flowdimension.GeoDictionaryEntry{
		{Kind: "province", Code: "810000", Name: "香港", Enabled: true},
		{Kind: "province", Code: "440000", Name: "广东", Enabled: true},
	})
	return writeFlowGeoBundleData(t, flowdimension.GeoSchemaV1, "test-1", ipv4, ipv6, operators, dictionary,
		time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC))
}

func writeFlowGeoV2Bundle(t *testing.T, version string, effectiveFrom time.Time) string {
	t.Helper()
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn,geo_leaf_code"
	ipv4 := header + "\n203.0.113.0,203.0.113.255,CN,330100,浙江省,杭州市,0,0,330100\n"
	ipv6 := header + "\n2001:db8::,2001:db8::ffff,CN,330100,浙江省,杭州市,0,0,330100\n"
	operators, _ := json.Marshal([]flowdimension.GeoOperator{})
	dictionary, _ := json.Marshal([]flowdimension.GeoDictionaryEntry{
		{Kind: "continent", Code: "Asia", Name: "亚洲", Enabled: true},
		{Kind: "region", Code: "EastAsia", Name: "东亚", ParentCode: "Asia", Enabled: true},
		{Kind: "country", Code: "CN", Name: "中国", ParentCode: "EastAsia", Enabled: true},
		{Kind: "province", Code: "330000", Name: "浙江省", ParentCode: "CN", Enabled: true},
		{Kind: "city", Code: "330100", Name: "杭州市", ParentCode: "330000", Enabled: true},
		{Kind: "city", Code: "330200", Name: "宁波市", ParentCode: "330000", Enabled: true},
	})
	return writeFlowGeoBundleData(t, flowdimension.GeoSchemaV2, version, ipv4, ipv6, operators, dictionary, effectiveFrom)
}

func writeFlowGeoBundleData(t *testing.T, schema, version, ipv4, ipv6 string, operators, dictionary []byte, effectiveFrom time.Time) string {
	t.Helper()
	dir := t.TempDir()
	compress := func(csv string) []byte {
		encoder, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		return encoder.EncodeAll([]byte(csv), nil)
	}

	files := map[string]struct {
		data []byte
		rows uint64
	}{
		"ipv4.csv.zst":   {compress(ipv4), uint64(len(geoCSVRows(ipv4)))},
		"ipv6.csv.zst":   {compress(ipv6), uint64(len(geoCSVRows(ipv6)))},
		"operators.json": {operators, uint64(len(mustGeoOperators(t, operators)))},
		"geo_dict.json":  {dictionary, uint64(len(mustGeoDictionary(t, dictionary)))},
	}
	manifest := map[string]any{
		"schema":            schema,
		"version":           version,
		"generated_at":      time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
		"effective_from":    effectiveFrom,
		"admin_code_system": flowdimension.GeoAdminCodeSystem,
		"unknown_country":   flowdimension.GeoUnknownCountry,
	}
	specs := map[string]flowdimension.GeoFileSpec{}
	for name, f := range files {
		if err := os.WriteFile(filepath.Join(dir, name), f.data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(f.data)
		specs[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: f.rows}
	}
	manifest["files"] = specs
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func geoCSVRows(value string) []string {
	lines := strings.Split(strings.TrimSpace(value), "\n")
	if len(lines) < 2 {
		return nil
	}
	return lines[1:]
}

func mustGeoOperators(t *testing.T, value []byte) []flowdimension.GeoOperator {
	t.Helper()
	var entries []flowdimension.GeoOperator
	if err := json.Unmarshal(value, &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func mustGeoDictionary(t *testing.T, value []byte) []flowdimension.GeoDictionaryEntry {
	t.Helper()
	var entries []flowdimension.GeoDictionaryEntry
	if err := json.Unmarshal(value, &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestFlowGeoServiceReloadLookupStatus(t *testing.T) {
	dir := writeFlowGeoBundle(t)
	service := newFlowGeoService(dir)

	// Before a reload nothing is loaded.
	if service.Status().Loaded {
		t.Fatal("service must start unloaded")
	}
	if _, found := service.Lookup(netip.MustParseAddr("1.96.0.1")); found {
		t.Fatal("lookup before reload must not find anything")
	}

	if err := service.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	status := service.Status()
	if !status.Loaded || status.Version != "test-1" || status.RowsV4 != 2 || status.RowsV6 != 1 {
		t.Fatalf("status = %+v", status)
	}

	info, found := service.Lookup(netip.MustParseAddr("1.96.0.1"))
	if !found || info.Country != "CN" || info.AdminCode != "810000" {
		t.Fatalf("lookup = %+v found=%v", info, found)
	}
	label, found := service.Label("test-1", "810000")
	if !found || label.Name != "香港" || len(label.Breadcrumb) != 1 || label.Breadcrumb[0] != "香港" {
		t.Fatalf("label = %+v found=%v", label, found)
	}
	if _, found := service.Label("missing-version", "810000"); found {
		t.Fatal("label must not fall back across dictionary versions")
	}
	if _, found := service.Lookup(netip.MustParseAddr("203.0.113.1")); found {
		t.Fatal("uncovered address must not resolve")
	}
}

func TestFlowGeoServiceKeepsServingAfterFailedReload(t *testing.T) {
	dir := writeFlowGeoBundle(t)
	service := newFlowGeoService(dir)
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the bundle so the next reload fails.
	if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := service.Reload(); err == nil {
		t.Fatal("reload without manifest must fail")
	}
	// The previous index keeps serving and the error is surfaced.
	status := service.Status()
	if !status.Loaded || status.LastError == "" {
		t.Fatalf("status after failed reload = %+v", status)
	}
	if _, found := service.Lookup(netip.MustParseAddr("1.96.0.1")); !found {
		t.Fatal("failed reload must keep the previous index serving")
	}
}

func TestFlowGeoServiceCatalogKeepsVersionsSeparate(t *testing.T) {
	oldPath := writeFlowGeoV2Bundle(t, "geo-old", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	activePath := writeFlowGeoV2Bundle(t, "geo-current", time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	service := newFlowGeoService(activePath)
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := service.LoadHistorical(oldPath); err != nil {
		t.Fatal(err)
	}

	current, err := service.Catalog("", "city", "330000", 10)
	if err != nil || current.Version != "geo-current" || current.ParentID != "330000" || current.Total != 2 {
		t.Fatalf("current catalog = %+v, %v", current, err)
	}
	if current.Items[0].ID != "330100" || current.Items[0].Name != "杭州市" || !current.Items[0].Additive ||
		len(current.Items[0].Path) != 5 || current.Items[0].Path[0].ID != "Asia" {
		t.Fatalf("current item = %+v", current.Items[0])
	}
	historical, err := service.Catalog("geo-old", "country", "EastAsia", 10)
	if err != nil || historical.Version != "geo-old" || historical.Total != 1 || historical.Items[0].ID != "CN" {
		t.Fatalf("historical catalog = %+v, %v", historical, err)
	}
	if _, err := service.Catalog("missing", "country", "", 10); !errors.Is(err, ErrFlowGeoVersionNotFound) {
		t.Fatalf("missing version error = %v", err)
	}
	if _, err := service.Catalog("", "city", "missing", 10); !errors.Is(err, ErrFlowGeoNodeNotFound) {
		t.Fatalf("missing parent error = %v", err)
	}
}

func geoGetContext(target string) (*gin.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	return c, rec
}

func TestFlowGeoCatalogHandlerValidatesAndReturnsPublishedCodes(t *testing.T) {
	service := newFlowGeoService(writeFlowGeoV2Bundle(t, "geo-api", time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)))
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	s := &Server{flowGeo: service}

	c, rec := geoGetContext("/flow/geo/catalog?level=city&parent=330000&limit=10")
	s.flowGeoCatalog(c)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"version":"geo-api"`) ||
		!strings.Contains(rec.Body.String(), `"id":"330100"`) || !strings.Contains(rec.Body.String(), `"parent_id":"330000"`) {
		t.Fatalf("catalog status=%d body=%s", rec.Code, rec.Body.String())
	}

	for _, target := range []string{
		"/flow/geo/catalog?level=city&unknown=1",
		"/flow/geo/catalog?level=city&level=country",
		"/flow/geo/catalog?level=city&limit=5001",
		"/flow/geo/catalog?level=unknown",
		"/flow/geo/catalog?level=city&version=missing",
	} {
		c, rec := geoGetContext(target)
		s.flowGeoCatalog(c)
		if rec.Code < 400 {
			t.Fatalf("%s status=%d body=%s", target, rec.Code, rec.Body.String())
		}
	}
}

func TestFlowGeoCatalogHandlerUnavailableWithoutBundle(t *testing.T) {
	s := &Server{flowGeo: newFlowGeoService("")}
	c, rec := geoGetContext("/flow/geo/catalog?level=country")
	s.flowGeoCatalog(c)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestFlowGeoStatusHandlerToleratesNilService(t *testing.T) {
	s := &Server{}
	c, rec := geoGetContext("/flow/geo/status")
	s.flowGeoStatus(c)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"loaded":false`) {
		t.Fatalf("nil-service status=%d body=%s", rec.Code, rec.Body.String())
	}
	c, rec = geoGetContext("/flow/geo/catalog?level=country")
	s.flowGeoCatalog(c)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil-service catalog status=%d", rec.Code)
	}
}

// TestFlowOverseasGeoLabels resolves labels only for non-other geo rows with a
// code, deduplicates by version:value, and skips non-geo rows. A nil geo service
// yields an empty map so the overseas handler degrades cleanly.
func TestFlowOverseasGeoLabels(t *testing.T) {
	service := newFlowGeoService(writeFlowGeoV2Bundle(t, "geo-api", time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)))
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	s := &Server{flowGeo: service}
	result := flowquery.OverseasResult{Points: []flowquery.OverseasPoint{
		{Kind: flowquery.OverseasRowGeo, GeoVersion: "geo-api", GeoValue: "330100"},
		{Kind: flowquery.OverseasRowGeo, GeoVersion: "geo-api", GeoValue: "330100"},              // duplicate
		{Kind: flowquery.OverseasRowGeo, GeoVersion: "geo-api", GeoValue: ""},                    // empty -> skipped
		{Kind: flowquery.OverseasRowGeo, GeoVersion: "geo-api", GeoValue: "330200", Other: true}, // other -> skipped
		{Kind: flowquery.OverseasRowKind("total"), GeoVersion: "geo-api", GeoValue: "330200"},    // non-geo -> skipped
	}}
	labels := s.flowOverseasGeoLabels(result)
	if len(labels) != 1 {
		t.Fatalf("labels = %+v, want exactly one entry", labels)
	}
	label, ok := labels["geo-api:330100"]
	if !ok || label.Name != "杭州市" {
		t.Fatalf("label = %+v ok=%v", label, ok)
	}

	if empty := (&Server{}).flowOverseasGeoLabels(result); len(empty) != 0 {
		t.Fatalf("nil geo service must yield no labels, got %+v", empty)
	}
}
