package flowcollect

import (
	"encoding/binary"
	"testing"
)

func TestInspectDatagram(t *testing.T) {
	sflow := make([]byte, 8)
	binary.BigEndian.PutUint32(sflow[:4], 5)
	if protocol, _, err := InspectDatagram(sflow); err != nil || protocol != ProtocolSFlow5 {
		t.Fatalf("sFlow: %v %v", protocol, err)
	}
	nf5 := make([]byte, 24)
	binary.BigEndian.PutUint16(nf5[:2], 5)
	if protocol, _, err := InspectDatagram(nf5); err != nil || protocol != ProtocolNetFlow5 {
		t.Fatalf("NetFlow v5: %v %v", protocol, err)
	}
	nf9 := make([]byte, 20)
	binary.BigEndian.PutUint16(nf9[:2], 9)
	binary.BigEndian.PutUint32(nf9[16:20], 123)
	if protocol, domain, err := InspectDatagram(nf9); err != nil || protocol != ProtocolNetFlow9 || domain != 123 {
		t.Fatalf("NetFlow v9: %v %d %v", protocol, domain, err)
	}
	ipfix := make([]byte, 16)
	binary.BigEndian.PutUint16(ipfix[:2], 10)
	binary.BigEndian.PutUint32(ipfix[12:16], 456)
	if protocol, domain, err := InspectDatagram(ipfix); err != nil || protocol != ProtocolIPFIX || domain != 456 {
		t.Fatalf("IPFIX: %v %d %v", protocol, domain, err)
	}
}
