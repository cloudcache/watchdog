package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"
)

func stableID(prefix string, parts ...string) ID {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(part))))
		_, _ = hash.Write([]byte{0})
	}
	return ID(fmt.Sprintf("%s_%016x", prefix, hash.Sum64()))
}

func collectorStableID(parts ...string) ID {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(part))))
		_, _ = hash.Write([]byte{0})
	}
	sum := hash.Sum(nil)
	return ID("c_" + hex.EncodeToString(sum)[:24])
}
