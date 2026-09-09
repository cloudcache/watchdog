package watchdog

import (
	"errors"
	"strconv"
	"strings"
)

func parsePageInteger(raw string, fallback, minimum, maximum int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, errors.New("integer is outside the allowed range")
	}
	return value, nil
}

// parseAgentPageInteger preserves the existing API call sites while pagination
// validation is shared by non-Agent handlers.
func parseAgentPageInteger(raw string, fallback, minimum, maximum int) (int, error) {
	return parsePageInteger(raw, fallback, minimum, maximum)
}
