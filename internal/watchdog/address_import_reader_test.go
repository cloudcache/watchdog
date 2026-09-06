package watchdog

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStreamMMDBReadsAkvoradoFixtures(t *testing.T) {
	path := filepath.Join("..", "..", "akvorado", "orchestrator", "geoip", "testdata", "GeoLite2-City-Test.mmdb")
	var records []AddressImportRecord
	metadata, err := StreamMMDB(path, func(record AddressImportRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Format != "mmdb" || metadata.DatabaseType == "" || len(records) == 0 {
		t.Fatalf("metadata=%#v records=%d", metadata, len(records))
	}
	for _, record := range records {
		if _, err := addressRecordPrefix(record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStreamIPDBEnumeratesTrieBoundaries(t *testing.T) {
	file := syntheticIPv4IPDB(t)
	path := filepath.Join(t.TempDir(), "test.ipdb")
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatal(err)
	}
	var records []AddressImportRecord
	metadata, err := StreamIPDB(path, "CN", func(record AddressImportRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Format != "ipdb" || metadata.IPVersion != 4 {
		t.Fatalf("metadata = %#v", metadata)
	}
	wantPrefixes := []string{"0.0.0.0/1", "128.0.0.0/1"}
	gotPrefixes := make([]string, len(records))
	for index := range records {
		gotPrefixes[index] = records[index].Prefix
	}
	if !reflect.DeepEqual(gotPrefixes, wantPrefixes) {
		t.Fatalf("prefixes = %#v, want %#v", gotPrefixes, wantPrefixes)
	}
	if records[0].CountryCode != "CN" || records[0].ASN != 4134 || records[0].SubdivisionCode != "110000" || records[0].CityCode != "110100" {
		t.Fatalf("first record = %#v", records[0])
	}
	if records[1].CountryCode != "US" || records[1].ASN != 15169 {
		t.Fatalf("second record = %#v", records[1])
	}
}

func syntheticIPv4IPDB(t *testing.T) []byte {
	t.Helper()
	fields := []string{"country_name", "region_name", "city_name", "isp_domain", "latitude", "longitude", "china_admin_code", "country_code", "continent_code", "asn"}
	const nodeCount = 97
	tree := make([]byte, nodeCount*8)
	for node := 0; node < 96; node++ {
		chosenBit := 0
		if node >= 80 {
			chosenBit = 1
		}
		binary.BigEndian.PutUint32(tree[node*8+(1-chosenBit)*4:], nodeCount)
		binary.BigEndian.PutUint32(tree[node*8+chosenBit*4:], uint32(node+1))
	}
	padding := []byte{0}
	record1 := []byte("中国\t北京\t北京市\tchinatelecom.cn\t39.9\t116.4\t110101\tCN\tAS\tAS4134")
	record2 := []byte("United States\tCalifornia\tMountain View\tgoogle.com\t37.4\t-122.1\t\tUS\tNA\t15169")
	data := append(tree, padding...)
	firstOffset := len(data)
	data = appendIPDBTestRecord(data, record1)
	secondOffset := len(data)
	data = appendIPDBTestRecord(data, record2)
	firstPointer := uint32(firstOffset - nodeCount*8 + nodeCount)
	secondPointer := uint32(secondOffset - nodeCount*8 + nodeCount)
	binary.BigEndian.PutUint32(data[96*8:], firstPointer)
	binary.BigEndian.PutUint32(data[96*8+4:], secondPointer)
	metadata := ipdbMetadata{Build: 1_700_000_000, IPVersion: 1, Languages: map[string]int{"CN": 0}, NodeCount: nodeCount, TotalSize: len(data), Fields: fields}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	file := make([]byte, 4, 4+len(encoded)+len(data))
	binary.BigEndian.PutUint32(file, uint32(len(encoded)))
	file = append(file, encoded...)
	file = append(file, data...)
	return file
}

func appendIPDBTestRecord(destination, record []byte) []byte {
	size := make([]byte, 2)
	binary.BigEndian.PutUint16(size, uint16(len(record)))
	destination = append(destination, size...)
	return append(destination, record...)
}
