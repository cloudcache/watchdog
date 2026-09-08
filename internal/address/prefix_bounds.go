package address

import "net/netip"

// addressPrefixBounds returns the inclusive [start,end] BINARY(16) bounds of a
// prefix, used both for the address_prefixes range index and the base-prefix
// import. Reused verbatim from the SaaS store.
func addressPrefixBounds(prefix netip.Prefix) ([16]byte, [16]byte) {
	prefix = prefix.Masked()
	startNumber := addressNumberFromAddr(prefix.Addr())
	width := 128
	if prefix.Addr().Is4() {
		width = 32
	}
	endNumber := addressBlockEnd(startNumber, width-prefix.Bits())
	return addressNumberBytes(startNumber), addressNumberBytes(endNumber)
}

func addressNumberBytes(number addressNumber) [16]byte {
	address := addressFromNumber(number, 6)
	return address.As16()
}
