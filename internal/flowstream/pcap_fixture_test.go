package flowstream

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
)

func TestDecoderWithAkvoradoDeviceCaptureFixtures(t *testing.T) {
	t.Run("NetFlow v5", func(t *testing.T) {
		payload := oneFixturePayload(t, "netflow", "nfv5.pcap")
		decoder, err := NewDecoder(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close()
		batch, err := decoder.DecodeValue(rawFlowValue(t, flowpb.RawFlow_DECODER_NETFLOW, payload))
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Records) == 0 {
			t.Fatal("real NetFlow v5 capture produced no records")
		}
	})

	t.Run("NetFlow v9 template then data", func(t *testing.T) {
		decoders := NewPartitionDecoders(time.Minute)
		defer decoders.Close()
		if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 5, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "template.pcap"))); err != nil {
			t.Fatal(err)
		}
		batch, err := decoders.DecodeRecord(rawKafkaRecord(t, 5, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "data.pcap")))
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Records) == 0 {
			t.Fatal("real NetFlow v9 capture produced no records")
		}
	})

	t.Run("IPFIX template then data", func(t *testing.T) {
		decoders := NewPartitionDecoders(time.Minute)
		defer decoders.Close()
		if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 6, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "ipfixprobe-templates.pcap"))); err != nil {
			t.Fatal(err)
		}
		batch, err := decoders.DecodeRecord(rawKafkaRecord(t, 6, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "ipfixprobe-data.pcap")))
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Records) == 0 {
			t.Fatal("real IPFIX capture produced no records")
		}
	})

	t.Run("NetFlow v9 sampling options state", func(t *testing.T) {
		decoders := NewPartitionDecoders(time.Minute)
		defer func() { decoders.Close() }()
		fixtures := []string{
			"multiplesamplingrates-options-template.pcap",
			"multiplesamplingrates-options-data.pcap",
			"multiplesamplingrates-template.pcap",
		}
		for offset, fixture := range fixtures {
			if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 7, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", fixture))); err != nil {
				t.Fatalf("decode %s at offset %d: %v", fixture, offset, err)
			}
		}
		batch, err := decoders.DecodeRecord(rawKafkaRecord(t, 7, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "multiplesamplingrates-data.pcap")))
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Records) < 2 {
			t.Fatalf("real NetFlow v9 capture produced %d records, want at least 2", len(batch.Records))
		}
		if batch.Records[0].SamplingRate != 4000 || batch.Records[1].SamplingRate != 2000 {
			t.Fatalf("sampling options state was not applied: rates=%d,%d", batch.Records[0].SamplingRate, batch.Records[1].SamplingRate)
		}

		// A fresh partition owner must rebuild both template and sampling
		// state from Kafka; sampler values cannot leak from the old owner.
		decoders.Close()
		decoders = NewPartitionDecoders(time.Minute)
		if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 7, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "multiplesamplingrates-options-template.pcap"))); err != nil {
			t.Fatal(err)
		}
		if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 7, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "multiplesamplingrates-template.pcap"))); err != nil {
			t.Fatal(err)
		}
		batch, err = decoders.DecodeRecord(rawKafkaRecord(t, 7, flowpb.RawFlow_DECODER_NETFLOW, oneFixturePayload(t, "netflow", "multiplesamplingrates-data.pcap")))
		if err != nil {
			t.Fatal(err)
		}
		if batch.Records[0].SamplingRate != 0 || batch.Records[1].SamplingRate != 0 {
			t.Fatalf("sampling state leaked across partition decoder restart: rates=%d,%d", batch.Records[0].SamplingRate, batch.Records[1].SamplingRate)
		}
	})

	t.Run("sFlow expanded sample", func(t *testing.T) {
		payload := oneFixturePayload(t, "sflow", "data-sflow-expanded-sample.pcap")
		decoder, err := NewDecoder(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close()
		batch, err := decoder.DecodeValue(rawFlowValue(t, flowpb.RawFlow_DECODER_SFLOW, payload))
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Records) == 0 || len(batch.RecordMetadata) != len(batch.Records) || !batch.AgentIP.IsValid() {
			t.Fatalf("real sFlow capture metadata is incomplete: %+v", batch)
		}
	})
}

func oneFixturePayload(t testing.TB, family, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve fixture test path")
	}
	root := os.Getenv("WATCHDOG_AKVORADO_FIXTURE_DIR")
	if root == "" {
		root = filepath.Join(filepath.Dir(currentFile), "..", "..", "akvorado")
	}
	path := filepath.Join(root, "outlet", "flow", "decoder", family, "testdata", name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			t.Skipf("Akvorado fixture is not available at %s", path)
		}
		t.Fatalf("stat Akvorado fixture: %v", err)
	}
	payloads := readPCAPUDPPayloads(t, path)
	if len(payloads) != 1 {
		t.Fatalf("%s contains %d packets, want 1", path, len(payloads))
	}
	return payloads[0]
}

func readPCAPUDPPayloads(t testing.TB, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 24 {
		t.Fatal("pcap global header is truncated")
	}
	var order binary.ByteOrder
	switch string(data[:4]) {
	case "\xd4\xc3\xb2\xa1", "\x4d\x3c\xb2\xa1":
		order = binary.LittleEndian
	case "\xa1\xb2\xc3\xd4", "\xa1\xb2\x3c\x4d":
		order = binary.BigEndian
	default:
		t.Fatal("unsupported pcap byte order")
	}
	var payloads [][]byte
	for offset := 24; offset < len(data); {
		if len(data)-offset < 16 {
			t.Fatal("pcap packet header is truncated")
		}
		captured := int(order.Uint32(data[offset+8 : offset+12]))
		offset += 16
		if captured < 0 || captured > len(data)-offset {
			t.Fatal("pcap packet is truncated")
		}
		frame := data[offset : offset+captured]
		offset += captured
		payloads = append(payloads, udpPayload(t, frame))
	}
	return payloads
}

func udpPayload(t testing.TB, frame []byte) []byte {
	t.Helper()
	if len(frame) < 14 {
		t.Fatal("Ethernet frame is truncated")
	}
	offset := 14
	etherType := binary.BigEndian.Uint16(frame[12:14])
	for etherType == 0x8100 || etherType == 0x88a8 {
		if len(frame) < offset+4 {
			t.Fatal("VLAN frame is truncated")
		}
		etherType = binary.BigEndian.Uint16(frame[offset+2 : offset+4])
		offset += 4
	}
	if etherType != 0x0800 || len(frame) < offset+20 {
		t.Fatal("fixture is not Ethernet/IPv4")
	}
	headerLength := int(frame[offset]&0x0f) * 4
	if headerLength < 20 || len(frame) < offset+headerLength+8 || frame[offset+9] != 17 {
		t.Fatal("fixture is not a complete IPv4/UDP packet")
	}
	udpOffset := offset + headerLength
	udpLength := int(binary.BigEndian.Uint16(frame[udpOffset+4 : udpOffset+6]))
	if udpLength < 8 || udpOffset+udpLength > len(frame) {
		t.Fatal("UDP payload is truncated")
	}
	return append([]byte(nil), frame[udpOffset+8:udpOffset+udpLength]...)
}
