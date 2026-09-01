package watchdog

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SNMPAgentConfig struct {
	ID           ID
	TenantID     ID
	TargetID     ID
	AgentType    AgentType
	Mode         AgentMode
	Endpoint     string
	TokenHash    string `json:"-"`
	Status       string
	LastSeen     time.Time
	LastRun      time.Time
	LastSuccess  time.Time
	LastError    string
	RunCount     uint64
	FailureCount uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type AgentRunStatus string

const (
	AgentRunSuccess AgentRunStatus = "success"
	AgentRunFailure AgentRunStatus = "failure"
)

type AgentRunReport struct {
	AgentID   ID
	Status    AgentRunStatus
	Error     string
	Seen      bool
	StartedAt time.Time
	EndedAt   time.Time
}

type AgentRunHistory struct {
	ID         ID
	TenantID   ID
	AgentID    ID
	TargetID   ID
	Status     AgentRunStatus
	Error      string
	Seen       bool
	StartedAt  time.Time
	EndedAt    time.Time
	DurationMS uint64
	CreatedAt  time.Time
}

func ApplyDeviceSNMPOverrides(profile SNMPProfile, device NetworkDevice) SNMPProfile {
	merged := profile
	security := make(map[string]string, len(profile.Security)+len(device.SNMPSecurity))
	for key, value := range profile.Security {
		security[key] = value
	}
	for key, value := range device.SNMPSecurity {
		if value != "" {
			security[key] = value
		}
	}
	merged.Security = security
	return merged
}

func SNMPAgentURL(host string, device NetworkDevice) string {
	return SNMPAgentURLForProfile(host, device, SNMPProfile{})
}

func SNMPAgentURLForProfile(host string, device NetworkDevice, profile SNMPProfile) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if strings.Contains(host, "://") {
		return host
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return fmt.Sprintf("%s://%s", SNMPTransport(profile, device), host)
	}
	return fmt.Sprintf("%s://%s:%d", SNMPTransport(profile, device), host, normalizeSNMPPort(device.SNMPPort))
}

func SNMPTransport(profile SNMPProfile, device NetworkDevice) string {
	for _, security := range []map[string]string{device.SNMPSecurity, profile.Security} {
		for _, key := range []string{"transport", "protocol"} {
			if transport := strings.TrimSpace(strings.ToLower(security[key])); transport != "" {
				return transport
			}
		}
	}
	return "udp"
}

func SNMPNetSNMPTarget(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" {
		return endpoint
	}
	host := parsed.Host
	if parsed.Scheme == "udp" {
		return host
	}
	return parsed.Scheme + ":" + host
}

func normalizeSNMPPort(port uint16) uint16 {
	if port == 0 {
		return 161
	}
	return port
}

func snmpHostPort(host string) (string, uint16) {
	host = strings.TrimSpace(host)
	if parsed, err := url.Parse(host); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		host = parsed.Host
	}
	splitHost, splitPort, err := net.SplitHostPort(host)
	if err != nil {
		return strings.Trim(host, "[]"), 161
	}
	port, err := strconv.ParseUint(splitPort, 10, 16)
	if err != nil || port == 0 {
		return splitHost, 161
	}
	return splitHost, uint16(port)
}
