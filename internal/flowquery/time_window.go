// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// LocalTimeWindow selects source buckets by local wall-clock time. Days use
// ISO weekdays (Monday=1 ... Sunday=7). A window whose end is earlier than its
// start crosses midnight and attributes the after-midnight portion to the
// previous day's window.
type LocalTimeWindow struct {
	Days       []uint8 `json:"days"`
	StartLocal string  `json:"start_local"`
	EndLocal   string  `json:"end_local"`
}

func compileLocalTimeWindows(windows []LocalTimeWindow, timezone, column string) (string, []proto.Parameter, error) {
	if len(windows) == 0 {
		return "", nil, nil
	}
	if len(windows) > 8 {
		return "", nil, requestError("time_windows", ErrorLimitExceeded, "time_windows accepts at most 8 windows")
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return "", nil, requestError("timezone", ErrorInvalid, "timezone is not a valid IANA location")
	}
	local := fmt.Sprintf("toTimeZone(%s, {time_window_timezone:String})", column)
	weekday := "toDayOfWeek(" + local + ")"
	minute := "(toHour(" + local + ") * 60 + toMinute(" + local + "))"
	clauses := make([]string, 0, len(windows))
	parameters := []proto.Parameter{stringParameter("time_window_timezone", timezone)}
	for index, window := range windows {
		start, end, days, err := normalizeLocalTimeWindow(window, index)
		if err != nil {
			return "", nil, err
		}
		startKey, endKey := fmt.Sprintf("time_window_%d_start", index), fmt.Sprintf("time_window_%d_end", index)
		parameters = append(parameters, uintParameter(startKey, uint64(start)), uintParameter(endKey, uint64(end)))
		dayList, dayParameters := localTimeWindowDayParameters(index, "day", days)
		parameters = append(parameters, dayParameters...)
		if start < end {
			clauses = append(clauses, fmt.Sprintf(
				"(%s IN (%s) AND %s >= {%s:UInt16} AND %s < {%s:UInt16})",
				weekday, dayList, minute, startKey, minute, endKey,
			))
			continue
		}
		nextDays := make([]uint8, len(days))
		for dayIndex, day := range days {
			nextDays[dayIndex] = day%7 + 1
		}
		nextDayList, nextDayParameters := localTimeWindowDayParameters(index, "next_day", nextDays)
		parameters = append(parameters, nextDayParameters...)
		clauses = append(clauses, fmt.Sprintf(
			"((%s IN (%s) AND %s >= {%s:UInt16}) OR (%s IN (%s) AND %s < {%s:UInt16}))",
			weekday, dayList, minute, startKey, weekday, nextDayList, minute, endKey,
		))
	}
	return "AND (" + strings.Join(clauses, " OR ") + ")", parameters, nil
}

func localTimeWindowDayParameters(index int, prefix string, days []uint8) (string, []proto.Parameter) {
	placeholders := make([]string, len(days))
	parameters := make([]proto.Parameter, len(days))
	for dayIndex, day := range days {
		key := fmt.Sprintf("time_window_%d_%s_%d", index, prefix, dayIndex)
		placeholders[dayIndex] = "{" + key + ":UInt8}"
		parameters[dayIndex] = uintParameter(key, uint64(day))
	}
	return strings.Join(placeholders, ", "), parameters
}

func normalizeLocalTimeWindow(window LocalTimeWindow, index int) (int, int, []uint8, error) {
	if len(window.Days) == 0 || len(window.Days) > 7 {
		return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d].days must contain 1..7 ISO weekdays", index))
	}
	seen := make(map[uint8]struct{}, len(window.Days))
	days := make([]uint8, len(window.Days))
	copy(days, window.Days)
	for _, day := range days {
		if day < 1 || day > 7 {
			return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d].days must contain ISO weekdays 1..7", index))
		}
		if _, duplicate := seen[day]; duplicate {
			return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d].days contains duplicates", index))
		}
		seen[day] = struct{}{}
	}
	start, err := parseLocalTimeMinute(window.StartLocal)
	if err != nil {
		return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d].start_local must be HH:MM", index))
	}
	end, err := parseLocalTimeMinute(window.EndLocal)
	if err != nil {
		return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d].end_local must be HH:MM", index))
	}
	if start == end {
		return 0, 0, nil, requestError("time_windows", ErrorInvalid, fmt.Sprintf("time_windows[%d] cannot have an empty interval", index))
	}
	return start, end, days, nil
}

func parseLocalTimeMinute(value string) (int, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, err
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

// InLocalTimeWindows mirrors the compiler's bucket-start predicate for table
// statistics and tests. An empty window list includes every timestamp.
func InLocalTimeWindows(timestamp time.Time, windows []LocalTimeWindow, timezone string) bool {
	if len(windows) == 0 {
		return true
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return false
	}
	local := timestamp.In(location)
	minute := local.Hour()*60 + local.Minute()
	weekday := uint8(local.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	for index, window := range windows {
		start, end, days, err := normalizeLocalTimeWindow(window, index)
		if err != nil {
			return false
		}
		if start < end {
			if containsISOWeekday(days, weekday) && minute >= start && minute < end {
				return true
			}
			continue
		}
		if containsISOWeekday(days, weekday) && minute >= start {
			return true
		}
		previous := weekday - 1
		if previous == 0 {
			previous = 7
		}
		if containsISOWeekday(days, previous) && minute < end {
			return true
		}
	}
	return false
}

func containsISOWeekday(days []uint8, want uint8) bool {
	for _, day := range days {
		if day == want {
			return true
		}
	}
	return false
}
