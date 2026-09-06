package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFlowFilterAPIValidatesCanonicalizesAndCompletes(t *testing.T) {
	mux := http.NewServeMux()
	registerFlowFilterRoutes(mux, func(next http.Handler) http.Handler { return next })

	validated := httptest.NewRecorder()
	mux.ServeHTTP(validated, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters/validate", strings.NewReader(`{
		"filter":{"op":"and","args":[
			{"op":"predicate","field":"protocol","operator":"in","values":["UDP","tcp"]},
			{"op":"predicate","field":"src_ip","operator":"eq","values":["203.0.113.9/24"]}
		]}}
	`)))
	if validated.Code != http.StatusOK || !strings.Contains(validated.Body.String(), `"203.0.113.0/24"`) ||
		!strings.Contains(validated.Body.String(), `"values":["6","17"]`) {
		t.Fatalf("validate status=%d body=%s", validated.Code, validated.Body.String())
	}

	completed := httptest.NewRecorder()
	mux.ServeHTTP(completed, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters/complete", strings.NewReader(`{"kind":"field","prefix":"geo."}`)))
	if completed.Code != http.StatusOK || !strings.Contains(completed.Body.String(), `"geo.country"`) || strings.Contains(completed.Body.String(), `"src_ip"`) {
		t.Fatalf("complete status=%d body=%s", completed.Code, completed.Body.String())
	}

	catalog := httptest.NewRecorder()
	mux.ServeHTTP(catalog, httptest.NewRequest(http.MethodGet, "/api/v1/flow/filters/catalog", nil))
	if catalog.Code != http.StatusOK || !strings.Contains(catalog.Body.String(), `"ip_or_cidr"`) || !strings.Contains(catalog.Body.String(), `"protocol"`) {
		t.Fatalf("catalog status=%d body=%s", catalog.Code, catalog.Body.String())
	}
}

func TestFlowFilterAPIRejectsUnknownJSONAndInvalidAST(t *testing.T) {
	mux := http.NewServeMux()
	registerFlowFilterRoutes(mux, func(next http.Handler) http.Handler { return next })
	for _, body := range []string{
		`{"filter":{"op":"predicate","field":"src_ip","operator":"eq","values":["bad"]}}`,
		`{"filter":{"op":"predicate","field":"src_ip","operator":"eq","values":["203.0.113.1"],"sql":"1=1"}}`,
	} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters/validate", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"invalid_request"`) {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
}
