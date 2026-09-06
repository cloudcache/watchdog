package watchdog

import (
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestAddressOperationScaleCertification runs the real address operation
// implementation at its documented management-plane limit. It is opt-in so
// the ordinary unit suite stays fast and deterministic.
func TestAddressOperationScaleCertification(t *testing.T) {
	if os.Getenv("WATCHDOG_ADDRESS_MANAGEMENT_SCALE") != "1" {
		t.Skip("set WATCHDOG_ADDRESS_MANAGEMENT_SCALE=1 to run")
	}

	t.Run("fifty-thousand-inputs", func(t *testing.T) {
		inputs := make([]string, MaxAddressOperationInputs)
		for index := range inputs {
			inputs[index] = scaleIPv4Address(uint32(index * 2))
		}
		started, allocatedBefore := scaleMeasurementStart()
		preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: inputs})
		if err != nil {
			t.Fatal(err)
		}
		elapsed, allocated := scaleMeasurementEnd(started, allocatedBefore)
		if preview.InputExpressions != MaxAddressOperationInputs || preview.ResultPrefixes != MaxAddressOperationInputs || preview.ResultAddressesV4 != strconv.Itoa(MaxAddressOperationInputs) {
			t.Fatalf("preview count = inputs:%d prefixes:%d addresses:%s", preview.InputExpressions, preview.ResultPrefixes, preview.ResultAddressesV4)
		}
		assertAddressScaleBudget(t, elapsed, allocated)
	})

	t.Run("worst-overlap", func(t *testing.T) {
		inputs := make([]string, MaxAddressOperationInputs)
		for index := range inputs {
			inputs[index] = "192.0.2.1"
		}
		started, allocatedBefore := scaleMeasurementStart()
		preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: inputs})
		if err != nil {
			t.Fatal(err)
		}
		elapsed, allocated := scaleMeasurementEnd(started, allocatedBefore)
		wantOverlaps := uint64(MaxAddressOperationInputs) * uint64(MaxAddressOperationInputs-1) / 2
		if preview.OverlapCount != wantOverlaps || len(preview.Overlaps) != maxAddressOverlapDetails || !preview.OverlapDetailsCutOff {
			t.Fatalf("overlap result = count:%d details:%d cutoff:%v", preview.OverlapCount, len(preview.Overlaps), preview.OverlapDetailsCutOff)
		}
		assertAddressScaleBudget(t, elapsed, allocated)
	})

	t.Run("result-limit", func(t *testing.T) {
		// Each range leaves the first and last address of its /24 unused, so
		// adjacent blocks cannot merge. Twenty thousand ranges expand beyond
		// MaxAddressOperationResults while remaining below the input limit.
		const ranges = 20_000
		inputs := make([]string, ranges)
		for index := range inputs {
			base := uint32(index) << 8
			inputs[index] = scaleIPv4Address(base+1) + "-" + scaleIPv4Address(base+254)
		}
		started, allocatedBefore := scaleMeasurementStart()
		_, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: inputs})
		elapsed, allocated := scaleMeasurementEnd(started, allocatedBefore)
		if err == nil || !strings.Contains(err.Error(), "limit") {
			t.Fatalf("result limit error = %v", err)
		}
		assertAddressScaleBudget(t, elapsed, allocated)
	})

	tooMany := make([]string, MaxAddressOperationInputs+1)
	if _, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: tooMany}); err == nil || !strings.Contains(err.Error(), "input") {
		t.Fatalf("input limit error = %v", err)
	}
}

func scaleIPv4Address(value uint32) string {
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}).String()
}

func scaleMeasurementStart() (time.Time, uint64) {
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return time.Now(), memory.TotalAlloc
}

func scaleMeasurementEnd(started time.Time, allocatedBefore uint64) (time.Duration, uint64) {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return time.Since(started), memory.TotalAlloc - allocatedBefore
}

func assertAddressScaleBudget(t testing.TB, elapsed time.Duration, allocated uint64) {
	t.Helper()
	t.Logf("elapsed=%s allocated=%.1f MiB", elapsed.Round(time.Millisecond), float64(allocated)/(1<<20))
	if elapsed > 10*time.Second {
		t.Fatalf("address operation exceeded 10s scale budget: %s", elapsed)
	}
	if allocated > 512<<20 {
		t.Fatalf("address operation exceeded 512 MiB allocation budget: %.1f MiB", float64(allocated)/(1<<20))
	}
}
