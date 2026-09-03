//go:build !development

package hub

import "testing"

func TestStaticCacheControl(t *testing.T) {
	tests := []struct {
		path    string
		want    string
		matched bool
	}{
		{path: "/assets/index-abc123.js", want: "public, max-age=31536000, immutable", matched: true},
		{path: "/static/icon.svg", want: "no-cache", matched: true},
		{path: "/network", matched: false},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			got, matched := staticCacheControl(test.path)
			if got != test.want || matched != test.matched {
				t.Fatalf("staticCacheControl(%q) = (%q, %t), want (%q, %t)", test.path, got, matched, test.want, test.matched)
			}
		})
	}
}
