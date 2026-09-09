package agentplan

import "time"

var (
	nowUTC   = func() time.Time { return time.Now().UTC() }
	zeroTime time.Time
)
