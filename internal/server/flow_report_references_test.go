package server

import (
	"testing"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func TestBuildFlowReportReferenceCatalogUsesOnlyActiveCustomerValues(t *testing.T) {
	artifact := flowdimension.AddressSnapshotArtifact{
		Strings: []string{
			"", "country", "province", "city",
			"country-cn", "China", "CN", "province-zj", "Zhejiang", "330000",
			"city-hz", "Hangzhou", "330100", "country-us", "United States", "US",
			"province-unused", "Unused province", "999999",
			"op-mobile", "China Mobile", "CMCC", "carrier", "op-unused", "Unused operator",
		},
		GeoNodes: []flowdimension.AddressSnapshotGeoNode{
			{Namespace: flowdimension.AddressSnapshotGeoCustomer, ID: 4, Kind: 1, Code: 6, Name: 5, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotGeoCustomer, ID: 7, Kind: 2, Code: 9, Name: 8, ParentID: 4, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotGeoCustomer, ID: 10, Kind: 3, Code: 12, Name: 11, ParentID: 7, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotGeoCustomer, ID: 13, Kind: 1, Code: 15, Name: 14, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotGeoCustomer, ID: 16, Kind: 2, Code: 18, Name: 17, ParentID: 4, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotGeoSupplier, ID: 4, Kind: 1, Code: 6, Name: 5, Enabled: true},
		},
		Operators: []flowdimension.AddressSnapshotOperator{
			{Namespace: flowdimension.AddressSnapshotOperatorCustomer, ID: 12, StableID: 19, Code: 21, Name: 20, ShortName: 21, Category: 22, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotOperatorCustomer, ID: 13, StableID: 23, Name: 24, Enabled: true},
			{Namespace: flowdimension.AddressSnapshotOperatorSupplier, ID: 12, StableID: 19, Name: 20, Enabled: true},
		},
		Values: []flowdimension.AddressSnapshotValue{
			{},
			{CustomerGeo: flowdimension.AddressSnapshotGeoValue{CountryID: 4, ProvinceID: 7, CityID: 10}, CustomerISPID: 12},
			{CustomerGeo: flowdimension.AddressSnapshotGeoValue{CountryID: 13}},
		},
	}

	catalog, err := buildFlowReportReferenceCatalog(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if got := catalog.Geo["country"]; len(got) != 2 || got[0].ID != "country-cn" || got[1].ID != "country-us" {
		t.Fatalf("countries = %#v", got)
	}
	if got := catalog.Geo["province"]; len(got) != 1 || got[0].ID != "province-zj" || got[0].ParentID != "country-cn" {
		t.Fatalf("provinces = %#v", got)
	}
	if got := catalog.Geo["city"]; len(got) != 1 || got[0].ID != "city-hz" || got[0].ParentID != "province-zj" {
		t.Fatalf("cities = %#v", got)
	}
	if got := catalog.Operators; len(got) != 1 || got[0].ID != "op-mobile" || got[0].FlowISPID != 12 || got[0].Name != "China Mobile" {
		t.Fatalf("operators = %#v", got)
	}
}

func TestBuildFlowReportReferenceCatalogRejectsInvalidReferences(t *testing.T) {
	_, err := buildFlowReportReferenceCatalog(flowdimension.AddressSnapshotArtifact{
		Strings: []string{""},
		Values:  []flowdimension.AddressSnapshotValue{{CustomerGeo: flowdimension.AddressSnapshotGeoValue{CountryID: 2}}},
	})
	if err == nil {
		t.Fatal("invalid string reference was accepted")
	}
}
