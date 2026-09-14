package address

import (
	"testing"
)

// Faithful port of internal/watchdog/dimension_publication_test.go (pure): the
// publication scope validation contract (module/dimension key).
func TestDimensionPublicationScopeValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope DimensionPublicationScope
		valid bool
	}{
		{name: "address", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "address"}, valid: true},
		{name: "vpn", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "vpn_rule_set"}, valid: true},
		{name: "missing module", scope: DimensionPublicationScope{DimensionKey: "address"}},
		{name: "trimmed", scope: DimensionPublicationScope{ModuleKey: " flow", DimensionKey: "address"}},
		{name: "uppercase", scope: DimensionPublicationScope{ModuleKey: "Flow", DimensionKey: "address"}},
		{name: "separator", scope: DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "vpn/rules"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.scope.validate(); (err == nil) != test.valid {
				t.Fatalf("validate() error = %v, valid = %v", err, test.valid)
			}
		})
	}
}
