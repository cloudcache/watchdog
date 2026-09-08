package address

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

// Keyset-pagination cursor codecs and the LIKE escaper, ported verbatim from the
// SaaS store so the paged list responses are byte-for-byte identical.

func encodeAuditCursor(createdAt time.Time, id ID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + string(id)))
}

func decodeAuditCursor(cursor string) (time.Time, ID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	return createdAt, ID(parts[1]), nil
}

// encodeStringCursor / decodeStringCursor page a list ordered by a (string, id)
// composite key. The id is written first because ids are "|"-free, so the sort
// value — which may itself contain "|" — is the remainder after the separator.
func encodeStringCursor(sortValue string, id ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(string(id) + "|" + sortValue))
}

func decodeStringCursor(cursor string) (string, ID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", errors.New("cursor is invalid")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return "", "", errors.New("cursor is invalid")
	}
	return parts[1], ID(parts[0]), nil
}

func escapeSQLLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "%", `\%`)
	return strings.ReplaceAll(value, "_", `\_`)
}
