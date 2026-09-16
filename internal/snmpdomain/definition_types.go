package snmpdomain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type ModuleType string

const (
	ModuleDiscovery ModuleType = "discovery"
	ModulePoller    ModuleType = "poller"
	ModuleTrap      ModuleType = "trap"
)

type OSFingerprint struct {
	SysObjectID  string
	SysDescr     string
	SysName      string
	SysLocation  string
	SysUpTime    uint64
	SNMPEngineID string
}

type OSMatch struct {
	OSName  string
	OSGroup string
	Vendor  string
	Class   string
	Model   string
	Reason  string
}

type OSDefinition struct {
	ID            string
	OSName        string
	OSGroup       string
	Vendor        string
	Class         string
	Definition    map[string]any
	Source        string
	SourceVersion string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ModuleDefinition struct {
	ID            string
	ModuleName    string
	ModuleType    ModuleType
	Definition    map[string]any
	Source        string
	SourceVersion string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type StateTranslation struct {
	ID        string
	Name      string
	Source    string
	States    []SNMPStateValue
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TrapHandlerDefinition struct {
	ID         string
	TrapOID    string
	HandlerKey string
	Enabled    bool
	Options    map[string]string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type DefinitionImport struct {
	OSDefinitions     []OSDefinition
	ModuleDefinitions []ModuleDefinition
	StateTranslations []StateTranslation
	TrapHandlers      []TrapHandlerDefinition
}

func collectorStableID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(part))))
		_, _ = hash.Write([]byte{0})
	}
	return "c_" + hex.EncodeToString(hash.Sum(nil))[:24]
}
