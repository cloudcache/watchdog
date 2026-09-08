package server

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestNormalizeIfStatus(t *testing.T) {
	for input, want := range map[string]string{
		"1": "up", "2": "down", "3": "testing", "4": "unknown",
		"5": "dormant", "6": "notPresent", "7": "lowerLayerDown", "": "",
	} {
		if got := normalizeIfStatus(input); got != want || !validIfStatus(got) {
			t.Errorf("normalizeIfStatus(%q)=%q, want valid %q", input, got, want)
		}
	}
	if validIfStatus("invented") {
		t.Fatal("invented interface state was accepted")
	}
}

func TestParseInventoryPage(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	tests := []struct {
		query string
		ok    bool
	}{
		{"?limit=25&offset=50&sort=name&order=desc&state=up", true},
		{"?limit=0", false},
		{"?limit=501", false},
		{"?offset=-1", false},
		{"?sort=raw_sql", false},
		{"?order=sideways", false},
		{"?unknown=value", false},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/test"+test.query, nil)
		page, ok := parseInventoryPage(c, []string{"state"}, map[string]string{"name": "t.name"}, "name")
		if ok != test.ok {
			t.Errorf("query %q ok=%v status=%d, want %v", test.query, ok, w.Code, test.ok)
		}
		if ok && (page.Limit != 25 || page.Offset != 50 || page.Sort != "t.name" || page.Order != "DESC") {
			t.Errorf("query %q parsed unexpected page: %+v", test.query, page)
		}
	}
}
