package watchdog

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const maxFlowFilterCompletionLimit = 100

func registerFlowFilterRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	api := flowFilterAPI{}
	mux.Handle("GET /api/v1/flow/filters/catalog", auth(http.HandlerFunc(api.catalog)))
	mux.Handle("POST /api/v1/flow/filters/validate", auth(http.HandlerFunc(api.validate)))
	mux.Handle("POST /api/v1/flow/filters/complete", auth(http.HandlerFunc(api.complete)))
}

type flowFilterAPI struct{}

func (flowFilterAPI) catalog(w http.ResponseWriter, _ *http.Request) {
	items := flowquery.FlowFilterCatalog()
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

type flowFilterValidateInput struct {
	Filter flowquery.FilterExpression `json:"filter"`
}

func (flowFilterAPI) validate(w http.ResponseWriter, r *http.Request) {
	var input flowFilterValidateInput
	if !decodeFlowFilterBody(w, r, &input) {
		return
	}
	canonical, err := flowquery.CanonicalFilter(input.Filter)
	if err != nil {
		writeFlowFilterError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"valid": true, "filter": canonical})
}

type flowFilterCompletionInput struct {
	Kind   string `json:"kind"`
	Field  string `json:"field,omitempty"`
	Prefix string `json:"prefix,omitempty"`
	Limit  uint16 `json:"limit,omitempty"`
}

func (flowFilterAPI) complete(w http.ResponseWriter, r *http.Request) {
	var input flowFilterCompletionInput
	if !decodeFlowFilterBody(w, r, &input) {
		return
	}
	input.Kind = strings.TrimSpace(input.Kind)
	input.Field = strings.TrimSpace(input.Field)
	input.Prefix = strings.ToLower(strings.TrimSpace(input.Prefix))
	limit := int(input.Limit)
	if limit == 0 {
		limit = 20
	}
	if limit > maxFlowFilterCompletionLimit || len(input.Prefix) > 256 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "completion limit or prefix is invalid", nil)
		return
	}
	catalog := flowquery.FlowFilterCatalog()
	items := make([]string, 0, limit)
	appendMatching := func(values []string) {
		for _, value := range values {
			if len(items) >= limit {
				return
			}
			if strings.HasPrefix(strings.ToLower(value), input.Prefix) {
				items = append(items, value)
			}
		}
	}
	switch input.Kind {
	case "field":
		fields := make([]string, 0, len(catalog))
		for _, definition := range catalog {
			fields = append(fields, definition.Name)
		}
		appendMatching(fields)
	case "operator", "value":
		definition, ok := findFlowFilterField(catalog, input.Field)
		if !ok {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "completion field is not in the Flow registry", map[string]any{"field": input.Field})
			return
		}
		if input.Kind == "operator" {
			operators := make([]string, 0, len(definition.Operators))
			for _, operator := range definition.Operators {
				operators = append(operators, string(operator))
			}
			appendMatching(operators)
		} else {
			appendMatching(definition.Values)
		}
	default:
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "completion kind must be field, operator or value", nil)
		return
	}
	sort.Strings(items)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func decodeFlowFilterBody(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return false
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return false
	}
	return true
}

func writeFlowFilterError(w http.ResponseWriter, err error) {
	var requestErr *flowquery.RequestError
	if errors.As(err, &requestErr) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, requestErr.Message, map[string]any{
			"field": requestErr.Field, "reason": requestErr.Code,
		})
		return
	}
	WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "Flow filter validation failed", nil)
}

func findFlowFilterField(catalog []flowquery.FilterFieldDefinition, field string) (flowquery.FilterFieldDefinition, bool) {
	for _, definition := range catalog {
		if definition.Name == field {
			return definition, true
		}
	}
	return flowquery.FilterFieldDefinition{}, false
}
