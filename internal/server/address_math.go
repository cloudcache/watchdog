package server

import (
	"container/heap"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"net/netip"
	"sort"
	"strings"
)

const (
	MaxAddressOperationInputs  = 50_000
	MaxAddressOperationResults = 200_000
	maxAddressOverlapDetails   = 1_000
)

type AddressSetOperationRequest struct {
	Operation      string   `json:"operation"`
	Left           []string `json:"left"`
	Right          []string `json:"right,omitempty"`
	Universe       []string `json:"universe,omitempty"`
	TargetPrefixV4 *int     `json:"target_prefix_v4,omitempty"`
	TargetPrefixV6 *int     `json:"target_prefix_v6,omitempty"`
}

type AddressOverlap struct {
	LeftOperand  string `json:"left_operand"`
	LeftIndex    int    `json:"left_index"`
	RightOperand string `json:"right_operand"`
	RightIndex   int    `json:"right_index"`
}

type AddressSetOperationPreview struct {
	Operation            string           `json:"operation"`
	CanonicalLeft        []string         `json:"canonical_left"`
	CanonicalRight       []string         `json:"canonical_right,omitempty"`
	CanonicalUniverse    []string         `json:"canonical_universe,omitempty"`
	Result               []string         `json:"result"`
	InputExpressions     int              `json:"input_expressions"`
	ResultPrefixes       int              `json:"result_prefixes"`
	ResultAddressesV4    string           `json:"result_addresses_v4"`
	ResultAddressesV6    string           `json:"result_addresses_v6"`
	AddedAddressesV4     string           `json:"added_addresses_v4"`
	AddedAddressesV6     string           `json:"added_addresses_v6"`
	OverlapCount         uint64           `json:"overlap_count"`
	Overlaps             []AddressOverlap `json:"overlaps,omitempty"`
	OverlapDetailsCutOff bool             `json:"overlap_details_cut_off"`
	Lossless             bool             `json:"lossless"`
	RequiresConfirmation bool             `json:"requires_confirmation"`
}

type addressNumber struct {
	hi uint64
	lo uint64
}

type addressInterval struct {
	family  uint8
	lo      addressNumber
	hi      addressNumber
	operand string
	index   int
}

func PreviewAddressSetOperation(request AddressSetOperationRequest) (AddressSetOperationPreview, error) {
	operation := strings.ToLower(strings.TrimSpace(request.Operation))
	if operation == "" {
		operation = "normalize"
	}
	switch operation {
	case "normalize", "union", "intersection", "difference", "complement", "cover":
	default:
		return AddressSetOperationPreview{}, fmt.Errorf("unsupported address operation %q", request.Operation)
	}
	inputCount := len(request.Left) + len(request.Right) + len(request.Universe)
	if inputCount == 0 {
		return AddressSetOperationPreview{}, errors.New("at least one address expression is required")
	}
	if inputCount > MaxAddressOperationInputs {
		return AddressSetOperationPreview{}, fmt.Errorf("address operation has %d inputs, limit is %d", inputCount, MaxAddressOperationInputs)
	}
	left, err := parseAddressExpressions("left", request.Left)
	if err != nil {
		return AddressSetOperationPreview{}, err
	}
	right, err := parseAddressExpressions("right", request.Right)
	if err != nil {
		return AddressSetOperationPreview{}, err
	}
	universe, err := parseAddressExpressions("universe", request.Universe)
	if err != nil {
		return AddressSetOperationPreview{}, err
	}
	leftUnion := mergeAddressIntervals(left)
	rightUnion := mergeAddressIntervals(right)
	universeUnion := mergeAddressIntervals(universe)

	var result []addressInterval
	switch operation {
	case "normalize":
		if len(right) != 0 || len(universe) != 0 {
			return AddressSetOperationPreview{}, errors.New("normalize only accepts left")
		}
		result = leftUnion
	case "union":
		if len(universe) != 0 {
			return AddressSetOperationPreview{}, errors.New("union does not accept universe")
		}
		result = mergeAddressIntervals(append(append([]addressInterval(nil), leftUnion...), rightUnion...))
	case "intersection":
		if len(right) == 0 || len(universe) != 0 {
			return AddressSetOperationPreview{}, errors.New("intersection requires right and does not accept universe")
		}
		result = intersectAddressIntervals(leftUnion, rightUnion)
	case "difference":
		if len(right) == 0 || len(universe) != 0 {
			return AddressSetOperationPreview{}, errors.New("difference requires right and does not accept universe")
		}
		result = subtractAddressIntervals(leftUnion, rightUnion)
	case "complement":
		if len(universe) == 0 || len(right) != 0 {
			return AddressSetOperationPreview{}, errors.New("complement requires an explicit finite universe and does not accept right")
		}
		result = subtractAddressIntervals(universeUnion, leftUnion)
	case "cover":
		if len(right) != 0 || len(universe) != 0 {
			return AddressSetOperationPreview{}, errors.New("cover only accepts left")
		}
		result, err = coverAddressIntervals(leftUnion, request.TargetPrefixV4, request.TargetPrefixV6)
		if err != nil {
			return AddressSetOperationPreview{}, err
		}
	}

	resultCIDRs, err := addressIntervalsToCIDRs(result, MaxAddressOperationResults)
	if err != nil {
		return AddressSetOperationPreview{}, err
	}
	leftCIDRs, err := addressIntervalsToCIDRs(leftUnion, MaxAddressOperationResults)
	if err != nil {
		return AddressSetOperationPreview{}, fmt.Errorf("normalize left: %w", err)
	}
	rightCIDRs, err := addressIntervalsToCIDRs(rightUnion, MaxAddressOperationResults)
	if err != nil {
		return AddressSetOperationPreview{}, fmt.Errorf("normalize right: %w", err)
	}
	universeCIDRs, err := addressIntervalsToCIDRs(universeUnion, MaxAddressOperationResults)
	if err != nil {
		return AddressSetOperationPreview{}, fmt.Errorf("normalize universe: %w", err)
	}
	resultV4, resultV6 := countAddressIntervals(result)
	var added []addressInterval
	if operation == "cover" {
		added = subtractAddressIntervals(result, leftUnion)
	}
	addedV4, addedV6 := countAddressIntervals(added)
	overlapCount, overlaps := detectAddressOverlaps(append(append([]addressInterval(nil), left...), right...))
	requiresConfirmation := operation == "cover" && (addedV4.Sign() != 0 || addedV6.Sign() != 0)
	return AddressSetOperationPreview{
		Operation: operation, CanonicalLeft: leftCIDRs, CanonicalRight: rightCIDRs,
		CanonicalUniverse: universeCIDRs, Result: resultCIDRs, InputExpressions: inputCount,
		ResultPrefixes: len(resultCIDRs), ResultAddressesV4: resultV4.String(), ResultAddressesV6: resultV6.String(),
		AddedAddressesV4: addedV4.String(), AddedAddressesV6: addedV6.String(), OverlapCount: overlapCount,
		Overlaps: overlaps, OverlapDetailsCutOff: overlapCount > uint64(len(overlaps)),
		Lossless: !requiresConfirmation, RequiresConfirmation: requiresConfirmation,
	}, nil
}

func parseAddressExpressions(operand string, expressions []string) ([]addressInterval, error) {
	result := make([]addressInterval, 0, len(expressions))
	for index, expression := range expressions {
		interval, err := parseAddressExpression(expression)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", operand, index, err)
		}
		interval.operand = operand
		interval.index = index
		result = append(result, interval)
	}
	return result, nil
}

func parseAddressExpression(expression string) (addressInterval, error) {
	value := strings.TrimSpace(expression)
	if value == "" {
		return addressInterval{}, errors.New("address expression is empty")
	}
	if strings.Count(value, "-") == 1 {
		parts := strings.SplitN(value, "-", 2)
		start, err := netip.ParseAddr(strings.TrimSpace(parts[0]))
		if err != nil {
			return addressInterval{}, fmt.Errorf("invalid range start: %w", err)
		}
		end, err := netip.ParseAddr(strings.TrimSpace(parts[1]))
		if err != nil {
			return addressInterval{}, fmt.Errorf("invalid range end: %w", err)
		}
		if start.Is4() != end.Is4() {
			return addressInterval{}, errors.New("range endpoints must use the same address family")
		}
		family := uint8(6)
		if start.Is4() {
			family = 4
		}
		lo, hi := addressNumberFromAddr(start), addressNumberFromAddr(end)
		if compareAddressNumber(lo, hi) > 0 {
			return addressInterval{}, errors.New("range start must not be greater than range end")
		}
		return addressInterval{family: family, lo: lo, hi: hi}, nil
	}
	if strings.Contains(value, "/") {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return addressInterval{}, fmt.Errorf("invalid CIDR: %w", err)
		}
		prefix = prefix.Masked()
		family := uint8(6)
		width := 128
		if prefix.Addr().Is4() {
			family, width = 4, 32
		}
		lo := addressNumberFromAddr(prefix.Addr())
		hi := addressBlockEnd(lo, width-prefix.Bits())
		return addressInterval{family: family, lo: lo, hi: hi}, nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil {
		return addressInterval{}, fmt.Errorf("invalid IP, CIDR, or start-end range: %w", err)
	}
	family := uint8(6)
	if address.Is4() {
		family = 4
	}
	number := addressNumberFromAddr(address)
	return addressInterval{family: family, lo: number, hi: number}, nil
}

func mergeAddressIntervals(input []addressInterval) []addressInterval {
	if len(input) == 0 {
		return nil
	}
	items := append([]addressInterval(nil), input...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].family != items[j].family {
			return items[i].family < items[j].family
		}
		if comparison := compareAddressNumber(items[i].lo, items[j].lo); comparison != 0 {
			return comparison < 0
		}
		return compareAddressNumber(items[i].hi, items[j].hi) < 0
	})
	result := make([]addressInterval, 0, len(items))
	for _, item := range items {
		if len(result) == 0 || result[len(result)-1].family != item.family {
			result = append(result, item)
			continue
		}
		last := &result[len(result)-1]
		touches := compareAddressNumber(item.lo, last.hi) <= 0
		if !touches {
			if next, ok := incrementAddressNumber(last.hi, last.family); ok {
				touches = compareAddressNumber(item.lo, next) <= 0
			}
		}
		if !touches {
			result = append(result, item)
			continue
		}
		if compareAddressNumber(item.hi, last.hi) > 0 {
			last.hi = item.hi
		}
	}
	return result
}

func intersectAddressIntervals(left, right []addressInterval) []addressInterval {
	var result []addressInterval
	for i, j := 0, 0; i < len(left) && j < len(right); {
		if left[i].family < right[j].family {
			i++
			continue
		}
		if left[i].family > right[j].family {
			j++
			continue
		}
		lo := maxAddressNumber(left[i].lo, right[j].lo)
		hi := minAddressNumber(left[i].hi, right[j].hi)
		if compareAddressNumber(lo, hi) <= 0 {
			result = append(result, addressInterval{family: left[i].family, lo: lo, hi: hi})
		}
		if compareAddressNumber(left[i].hi, right[j].hi) < 0 {
			i++
		} else {
			j++
		}
	}
	return mergeAddressIntervals(result)
}

func subtractAddressIntervals(left, right []addressInterval) []addressInterval {
	if len(left) == 0 {
		return nil
	}
	right = mergeAddressIntervals(right)
	result := make([]addressInterval, 0, len(left))
	j := 0
	for _, source := range mergeAddressIntervals(left) {
		cursor := source.lo
		for j < len(right) && (right[j].family < source.family || (right[j].family == source.family && compareAddressNumber(right[j].hi, cursor) < 0)) {
			j++
		}
		for k := j; k < len(right) && right[k].family == source.family && compareAddressNumber(right[k].lo, source.hi) <= 0; k++ {
			cut := right[k]
			if compareAddressNumber(cut.lo, cursor) > 0 {
				if end, ok := decrementAddressNumber(cut.lo); ok {
					result = append(result, addressInterval{family: source.family, lo: cursor, hi: minAddressNumber(end, source.hi)})
				}
			}
			if compareAddressNumber(cut.hi, source.hi) >= 0 {
				cursor = addressNumber{}
				goto nextSource
			}
			next, ok := incrementAddressNumber(cut.hi, source.family)
			if !ok {
				goto nextSource
			}
			if compareAddressNumber(next, cursor) > 0 {
				cursor = next
			}
		}
		if compareAddressNumber(cursor, source.hi) <= 0 {
			result = append(result, addressInterval{family: source.family, lo: cursor, hi: source.hi})
		}
	nextSource:
	}
	return mergeAddressIntervals(result)
}

func coverAddressIntervals(input []addressInterval, targetV4, targetV6 *int) ([]addressInterval, error) {
	result := make([]addressInterval, 0, len(input))
	for _, item := range input {
		target := targetV6
		width := 128
		if item.family == 4 {
			target, width = targetV4, 32
		}
		if target == nil {
			return nil, fmt.Errorf("target_prefix_v%d is required when cover includes IPv%d", item.family, item.family)
		}
		if *target < 0 || *target > width {
			return nil, fmt.Errorf("target_prefix_v%d must be between 0 and %d", item.family, width)
		}
		hostBits := width - *target
		result = append(result, addressInterval{
			family: item.family,
			lo:     addressBlockStart(item.lo, hostBits),
			hi:     addressBlockEnd(item.hi, hostBits),
		})
	}
	return mergeAddressIntervals(result), nil
}

func addressIntervalsToCIDRs(intervals []addressInterval, limit int) ([]string, error) {
	var result []string
	for _, interval := range mergeAddressIntervals(intervals) {
		cursor := interval.lo
		width := 128
		if interval.family == 4 {
			width = 32
		}
		for {
			blockBits := trailingAddressZeros(cursor, width)
			for blockBits > 0 && compareAddressNumber(addressBlockEnd(cursor, blockBits), interval.hi) > 0 {
				blockBits--
			}
			address := addressFromNumber(cursor, interval.family)
			result = append(result, netip.PrefixFrom(address, width-blockBits).String())
			if len(result) > limit {
				return nil, fmt.Errorf("normalized result exceeds prefix limit %d", limit)
			}
			end := addressBlockEnd(cursor, blockBits)
			if compareAddressNumber(end, interval.hi) >= 0 {
				break
			}
			next, ok := incrementAddressNumber(end, interval.family)
			if !ok {
				break
			}
			cursor = next
		}
	}
	return result, nil
}

func countAddressIntervals(intervals []addressInterval) (*big.Int, *big.Int) {
	v4, v6 := new(big.Int), new(big.Int)
	for _, interval := range mergeAddressIntervals(intervals) {
		span := addressNumberBig(interval.hi)
		span.Sub(span, addressNumberBig(interval.lo))
		span.Add(span, big.NewInt(1))
		if interval.family == 4 {
			v4.Add(v4, span)
		} else {
			v6.Add(v6, span)
		}
	}
	return v4, v6
}

type overlapHeapItem struct {
	interval addressInterval
}

type overlapHeap []overlapHeapItem

func (h overlapHeap) Len() int { return len(h) }
func (h overlapHeap) Less(i, j int) bool {
	return compareAddressNumber(h[i].interval.hi, h[j].interval.hi) < 0
}
func (h overlapHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *overlapHeap) Push(value any) { *h = append(*h, value.(overlapHeapItem)) }
func (h *overlapHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func detectAddressOverlaps(input []addressInterval) (uint64, []AddressOverlap) {
	items := append([]addressInterval(nil), input...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].family != items[j].family {
			return items[i].family < items[j].family
		}
		return compareAddressNumber(items[i].lo, items[j].lo) < 0
	})
	var active overlapHeap
	var family uint8
	var total uint64
	details := make([]AddressOverlap, 0)
	for _, item := range items {
		if family != item.family {
			active = active[:0]
			family = item.family
		}
		for active.Len() > 0 && compareAddressNumber(active[0].interval.hi, item.lo) < 0 {
			heap.Pop(&active)
		}
		if ^uint64(0)-total < uint64(active.Len()) {
			total = ^uint64(0)
		} else {
			total += uint64(active.Len())
		}
		if len(details) < maxAddressOverlapDetails {
			for _, prior := range active {
				if len(details) >= maxAddressOverlapDetails {
					break
				}
				details = append(details, AddressOverlap{
					LeftOperand: prior.interval.operand, LeftIndex: prior.interval.index,
					RightOperand: item.operand, RightIndex: item.index,
				})
			}
		}
		heap.Push(&active, overlapHeapItem{interval: item})
	}
	return total, details
}

func addressNumberFromAddr(address netip.Addr) addressNumber {
	if address.Is4() {
		bytes := address.As4()
		return addressNumber{lo: uint64(bytes[0])<<24 | uint64(bytes[1])<<16 | uint64(bytes[2])<<8 | uint64(bytes[3])}
	}
	bytes := address.As16()
	return addressNumber{
		hi: uint64(bytes[0])<<56 | uint64(bytes[1])<<48 | uint64(bytes[2])<<40 | uint64(bytes[3])<<32 |
			uint64(bytes[4])<<24 | uint64(bytes[5])<<16 | uint64(bytes[6])<<8 | uint64(bytes[7]),
		lo: uint64(bytes[8])<<56 | uint64(bytes[9])<<48 | uint64(bytes[10])<<40 | uint64(bytes[11])<<32 |
			uint64(bytes[12])<<24 | uint64(bytes[13])<<16 | uint64(bytes[14])<<8 | uint64(bytes[15]),
	}
}

func addressFromNumber(number addressNumber, family uint8) netip.Addr {
	if family == 4 {
		return netip.AddrFrom4([4]byte{byte(number.lo >> 24), byte(number.lo >> 16), byte(number.lo >> 8), byte(number.lo)})
	}
	return netip.AddrFrom16([16]byte{
		byte(number.hi >> 56), byte(number.hi >> 48), byte(number.hi >> 40), byte(number.hi >> 32),
		byte(number.hi >> 24), byte(number.hi >> 16), byte(number.hi >> 8), byte(number.hi),
		byte(number.lo >> 56), byte(number.lo >> 48), byte(number.lo >> 40), byte(number.lo >> 32),
		byte(number.lo >> 24), byte(number.lo >> 16), byte(number.lo >> 8), byte(number.lo),
	})
}

func addressBlockStart(number addressNumber, hostBits int) addressNumber {
	if hostBits <= 0 {
		return number
	}
	if hostBits < 64 {
		number.lo &= ^uint64(0) << hostBits
		return number
	}
	number.lo = 0
	if hostBits < 128 {
		number.hi &= ^uint64(0) << (hostBits - 64)
	} else {
		number.hi = 0
	}
	return number
}

func addressBlockEnd(number addressNumber, hostBits int) addressNumber {
	number = addressBlockStart(number, hostBits)
	if hostBits <= 0 {
		return number
	}
	if hostBits < 64 {
		number.lo |= ^uint64(0) >> (64 - hostBits)
		return number
	}
	number.lo = ^uint64(0)
	if hostBits < 128 {
		number.hi |= ^uint64(0) >> (128 - hostBits)
	} else {
		number.hi = ^uint64(0)
	}
	return number
}

func trailingAddressZeros(number addressNumber, width int) int {
	if width == 32 {
		return min(bits.TrailingZeros32(uint32(number.lo)), 32)
	}
	if number.lo != 0 {
		return bits.TrailingZeros64(number.lo)
	}
	if number.hi != 0 {
		return 64 + bits.TrailingZeros64(number.hi)
	}
	return 128
}

func compareAddressNumber(left, right addressNumber) int {
	if left.hi < right.hi {
		return -1
	}
	if left.hi > right.hi {
		return 1
	}
	if left.lo < right.lo {
		return -1
	}
	if left.lo > right.lo {
		return 1
	}
	return 0
}

func maxAddressNumber(left, right addressNumber) addressNumber {
	if compareAddressNumber(left, right) >= 0 {
		return left
	}
	return right
}

func minAddressNumber(left, right addressNumber) addressNumber {
	if compareAddressNumber(left, right) <= 0 {
		return left
	}
	return right
}

func incrementAddressNumber(number addressNumber, family uint8) (addressNumber, bool) {
	if family == 4 {
		if number.lo >= uint64(^uint32(0)) {
			return addressNumber{}, false
		}
		number.lo++
		return number, true
	}
	if number.lo == ^uint64(0) {
		if number.hi == ^uint64(0) {
			return addressNumber{}, false
		}
		number.hi++
		number.lo = 0
		return number, true
	}
	number.lo++
	return number, true
}

func decrementAddressNumber(number addressNumber) (addressNumber, bool) {
	if number.hi == 0 && number.lo == 0 {
		return addressNumber{}, false
	}
	if number.lo == 0 {
		number.hi--
		number.lo = ^uint64(0)
		return number, true
	}
	number.lo--
	return number, true
}

func addressNumberBig(number addressNumber) *big.Int {
	value := new(big.Int).SetUint64(number.hi)
	value.Lsh(value, 64)
	return value.Add(value, new(big.Int).SetUint64(number.lo))
}
