package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	g "github.com/gosnmp/gosnmp"
)

func TestHandleTrapPreservesIngestionContract(t *testing.T) {
	type requestBody struct {
		SourceIP string        `json:"source_ip"`
		Hostname string        `json:"hostname"`
		TrapOID  string        `json:"trap_oid"`
		VarBinds []trapVarBind `json:"varbinds"`
	}
	var body requestBody
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/snmp/traps" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer agent-secret" {
			t.Errorf("authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    r,
		}, nil
	})}

	handleTrapWithClient(context.Background(), "http://127.0.0.1:8091", "agent-secret", &g.SnmpPacket{
		SnmpTrap:  g.SnmpTrap{AgentAddress: "192.0.2.10"},
		Community: "public",
		Variables: []g.SnmpPDU{
			{Name: ".1.3.6.1.6.3.1.1.4.1.0", Type: g.ObjectIdentifier, Value: ".1.3.6.1.6.3.1.1.5.3"},
			{Name: ".1.3.6.1.2.1.1.3.0", Type: g.TimeTicks, Value: uint32(123)},
		},
	}, &net.UDPAddr{IP: net.ParseIP("192.0.2.11"), Port: 162}, client)

	if body.SourceIP != "192.0.2.10" || body.Hostname != "public" || body.TrapOID != ".1.3.6.1.6.3.1.1.5.3" {
		t.Fatalf("body = %#v", body)
	}
	if len(body.VarBinds) != 2 || body.VarBinds[1].OID != ".1.3.6.1.2.1.1.3.0" || body.VarBinds[1].Value != "123" {
		t.Fatalf("varbinds = %#v", body.VarBinds)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
