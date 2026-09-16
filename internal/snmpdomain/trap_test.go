package snmpdomain

import (
	"context"
	"testing"
	"time"
)

func TestTrapLinkDownUpdatesPortAndRequestsPoll(t *testing.T) {
	dispatcher := NewTrapDispatcher(DefaultTrapHandlers(
		map[uint64]TrapPort{101: {ID: "port-a", IfIndex: 101, IfName: "Eth1/1", OperStatus: "up"}},
		map[string][]string{"port-a": {"recipe-a"}}, nil, nil,
	))
	result, err := dispatcher.Dispatch(context.Background(), TrapDevice{ID: "device-a"}, Trap{
		TrapOID: TrapOIDLinkDown, ReceivedAt: time.Unix(1, 0).UTC(),
		VarBinds: []TrapVarBind{{OID: "IF-MIB::ifIndex.101", Value: "101"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PortUpdates) != 1 || result.PortUpdates[0].OperStatus != "down" {
		t.Fatalf("unexpected port updates: %+v", result.PortUpdates)
	}
	if len(result.ImmediatePollRecipe) != 1 || result.ImmediatePollRecipe[0] != "recipe-a" {
		t.Fatalf("unexpected poll recipes: %v", result.ImmediatePollRecipe)
	}
	if len(result.Events) != 1 || result.Events[0].DeviceID != "device-a" || result.Events[0].EntityID != "port-a" || result.Events[0].ID == "" {
		t.Fatalf("unexpected event: %+v", result.Events)
	}
}

func TestTrapUnknownInterfaceRequestsRediscovery(t *testing.T) {
	dispatcher := NewTrapDispatcher(DefaultTrapHandlers(nil, nil, nil, nil))
	result, err := dispatcher.Dispatch(context.Background(), TrapDevice{ID: "device-a"}, Trap{
		TrapOID: TrapOIDLinkDown, VarBinds: []TrapVarBind{{OID: "IF-MIB::ifIndex.999", Value: "999"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.RediscoverDevice || len(result.Events) != 1 || result.Events[0].EventType != "unknown_interface_trap" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestUnhandledTrapCreatesEvent(t *testing.T) {
	result, err := NewTrapDispatcher(nil).Dispatch(context.Background(), TrapDevice{ID: "device-a"}, Trap{TrapOID: "1.2.3.4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 || result.Events[0].EventType != "unhandled_trap" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestBGPBackwardTransitionUpdatesSession(t *testing.T) {
	dispatcher := NewTrapDispatcher(DefaultTrapHandlers(nil, nil,
		map[string]TrapBGPSession{"192.0.2.2": {ID: "bgp-a", PeerAddr: "192.0.2.2", State: "established"}},
		map[string][]string{"bgp-a": {"recipe-bgp"}},
	))
	result, err := dispatcher.Dispatch(context.Background(), TrapDevice{ID: "device-a"}, Trap{
		TrapOID:  TrapOIDBGPBackwardTransition,
		VarBinds: []TrapVarBind{{OID: "BGP4-MIB::bgpPeerRemoteAddr", Value: "192.0.2.2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.BGPUpdates) != 1 || result.BGPUpdates[0].State != "idle" || len(result.ImmediatePollRecipe) != 1 || result.ImmediatePollRecipe[0] != "recipe-bgp" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestNormalizeIfStatus(t *testing.T) {
	for input, expected := range map[string]string{"1": "up", "2": "down", "7": "lowerLayerDown", "up": "up"} {
		if got := NormalizeIfStatus(input); got != expected {
			t.Fatalf("NormalizeIfStatus(%q)=%q, want %q", input, got, expected)
		}
	}
}
