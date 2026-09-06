package watchdog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestAddressSetOperationPreviewEndpointIsStrictAndBounded(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-sets/actions/preview", strings.NewReader(`{
		"operation":"difference",
		"left":["192.0.2.1-192.0.2.4"],
		"right":["192.0.2.2"]
	}`))
	response := httptest.NewRecorder()
	(addressSetAPI{}).previewOperation(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var preview AddressSetOperationPreview
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.Result, []string{"192.0.2.1/32", "192.0.2.3/32", "192.0.2.4/32"}) {
		t.Fatalf("result = %#v", preview.Result)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/address-sets/actions/preview", strings.NewReader(`{"operation":"normalize","left":["192.0.2.0/24"],"unexpected":true}`))
	response = httptest.NewRecorder()
	(addressSetAPI{}).previewOperation(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("strict status = %d, want 400", response.Code)
	}
}
