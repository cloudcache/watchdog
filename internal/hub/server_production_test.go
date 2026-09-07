//go:build !development

package hub

import (
	"strings"
	"testing"
)

func TestStandaloneRuntimeConfigurationUsesHubURLAsAPIURL(t *testing.T) {
	hub := &Hub{appURL: "https://api.watchdog.example/base"}
	info := getPublicAppInfo(hub)
	if info.API_URL != hub.appURL || info.BASE_PATH != "/base/" {
		t.Fatalf("unexpected public app info: %#v", info)
	}
	html := modifyIndexHTML(hub, []byte(`<script>globalThis.WATCHDOG = "{info}"</script>`))
	if !strings.Contains(html, `"API_URL":"https://api.watchdog.example/base"`) {
		t.Fatalf("API URL was not injected: %s", html)
	}
}

func TestStaticCacheControl(t *testing.T) {
	tests := []struct {
		path    string
		want    string
		matched bool
	}{
		{path: "/assets/index-abc123.js", want: "public, max-age=31536000, immutable", matched: true},
		{path: "/static/icon.svg", want: "no-cache", matched: true},
		{path: "/favicon.ico", want: "no-cache", matched: true},
		{path: "/watchdog/favicon.ico", want: "no-cache", matched: true},
		{path: "/favicon.ico/route", matched: false},
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
