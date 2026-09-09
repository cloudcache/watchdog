package server

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
)

func TestWriteSNMPCSVArtifactIsAtomicAndChecksummed(t *testing.T) {
	dir := t.TempDir()
	result := snmpch.AggregateResult{
		Metric: snmpch.MetricIfInBPS, Method: "sum",
		Points: []snmpch.Point{{Time: time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC), Value: 8000}},
	}
	ref, err := writeSNMPCSVArtifact(context.Background(), dir, "job-a", result, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ref, "snmp-csv/job-a-") || !strings.HasSuffix(ref, ".csv") {
		t.Fatalf("ref=%q", ref)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 || strings.HasPrefix(files[0].Name(), ".snmp-export-") {
		t.Fatalf("files=%v err=%v", files, err)
	}
	file, err := os.Open(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || strings.Join(rows[0], ",") != "bucket_start,metric,aggregate,value" || rows[1][3] != "8000" {
		t.Fatalf("rows=%v", rows)
	}
}

func TestWriteSNMPCSVArtifactHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := writeSNMPCSVArtifact(ctx, t.TempDir(), "job-b", snmpch.AggregateResult{
		Metric: "m", Method: "sum", Points: []snmpch.Point{{Time: time.Now(), Value: 1}},
	}, nil)
	if err == nil {
		t.Fatal("canceled export unexpectedly succeeded")
	}
}

func TestParseSNMPIDsDeduplicatesAndBounds(t *testing.T) {
	ids, err := parseSNMPIDs(" b,a ", "a")
	if err != nil || strings.Join(ids, ",") != "a,b" {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	tooMany := make([]string, 1001)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("scope-%04d", i)
	}
	if _, err := parseSNMPIDs(strings.Join(tooMany, ",")); err == nil {
		t.Fatal("oversized scope was accepted")
	}
}
