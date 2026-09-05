package flowdimension

import "testing"

func TestClassifyCategoryUsesOrderedSixDimensionRules(t *testing.T) {
	home := HomeProfile{
		Province: "330000", City: "330100", ISPIDs: map[uint16]struct{}{100: {}},
		OverseasIncludesHMT: true, Version: 7,
	}
	tests := []struct {
		name      string
		direction BusinessDirection
		remote    GeoInfo
		mutate    func(*HomeProfile)
		want      Category
	}{
		{name: "unknown-country", direction: DirectionOut, remote: GeoInfo{AdminCode: "330100", ISPID: 100}, want: CategoryUnknown},
		{name: "malformed-country", direction: DirectionOut, remote: GeoInfo{Country: "USA", AdminCode: "330100", ISPID: 100}, want: CategoryUnknown},
		{name: "overseas", direction: DirectionOut, remote: GeoInfo{Country: "US", ISPID: 100}, want: CategoryOverseas},
		{name: "missing-province", direction: DirectionOut, remote: GeoInfo{Country: "CN", ISPID: 100}, want: CategoryUnknown},
		{name: "missing-isp", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "330100"}, want: CategoryUnknown},
		{name: "on-net-local-city", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "330199", ISPID: 100}, want: CategoryOnNetLocalCity},
		{name: "on-net-cross-city", direction: DirectionIn, remote: GeoInfo{Country: "CN", AdminCode: "330200", ISPID: 100}, want: CategoryOnNetCrossCity},
		{name: "on-net-cross-province", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "320100", ISPID: 100}, want: CategoryOnNetCrossProvince},
		{name: "off-net-in-province", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 200}, want: CategoryOffNetInProvince},
		{name: "off-net-cross-province", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "320100", ISPID: 200}, want: CategoryOffNetCrossProvince},
		{name: "missing-home-profile", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 100}, mutate: func(profile *HomeProfile) { profile.ISPIDs = nil }, want: CategoryUnknown},
		{name: "on-net-city-unavailable", direction: DirectionOut, remote: GeoInfo{Country: "CN", AdminCode: "330000", ISPID: 100}, want: CategoryUnknown},
		{name: "hmt-overseas-policy", direction: DirectionOut, remote: GeoInfo{Country: "HK", ISPID: 200}, want: CategoryOverseas},
		{name: "hmt-domestic-policy", direction: DirectionOut, remote: GeoInfo{Country: "HK", ISPID: 200}, mutate: func(profile *HomeProfile) { profile.OverseasIncludesHMT = false }, want: CategoryOffNetCrossProvince},
		{name: "internal", direction: DirectionInternal, want: CategoryInternal},
		{name: "transit", direction: DirectionTransit, want: CategoryTransit},
		{name: "ambiguous", direction: DirectionAmbiguous, want: CategoryAmbiguous},
		{name: "invalid-direction", direction: DirectionBoth, want: CategoryUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := home
			if test.mutate != nil {
				test.mutate(&profile)
			}
			if got := ClassifyCategory(test.direction, test.remote, profile); got != test.want {
				t.Fatalf("category = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNormalizeHMTPreservesDetailedCanonicalAdminCode(t *testing.T) {
	country, adminCode, isHMT := normalizeHMT("hk", "810101")
	if country != "CN" || adminCode != "810101" || !isHMT {
		t.Fatalf("normalization = %q %q %v", country, adminCode, isHMT)
	}
	country, adminCode, isHMT = normalizeHMT("MO", "wrong")
	if country != "CN" || adminCode != "820000" || !isHMT {
		t.Fatalf("fallback normalization = %q %q %v", country, adminCode, isHMT)
	}
}
