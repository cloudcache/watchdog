package snmpdomain

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sleepinggenius2/gosmi"
	"github.com/sleepinggenius2/gosmi/types"
)

type SNMPConfig struct {
	MIBDirs []string
	MIBLoad string
}

type MIBModule struct {
	ID        string
	Name      string
	Source    string
	Version   string
	Checksum  string
	Enabled   bool
	Builtin   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type SNMPStateValue struct {
	Value   int
	Generic int
	Label   string
}

// The SNMP collector resolves every OID it uses from MIB modules — the
// LibreNMS approach — instead of hardcoding numeric OIDs one by one. A
// curated subset of the LibreNMS MIB library is embedded for core polling. A
// full MIB tree (for example a LibreNMS checkout's mibs directory) is layered
// on through `snmp.mib_dirs` for capability providers such as IP-MIB and
// vendor BGPv2 tables; `snmp.mib_load` can optionally preload modules.

//go:embed mibs
var watchdogEmbeddedMIBs embed.FS

// snmpMIBEmbeddedModules are loaded from the embedded bundle at startup.
// Modules they import (SNMPv2-SMI, SNMPv2-TC, ...) load transitively.
var snmpMIBEmbeddedModules = []string{
	"SNMPv2-MIB",
	"SNMP-FRAMEWORK-MIB",
	"IF-MIB",
	"BGP4-MIB",
	"HOST-RESOURCES-MIB",
	"ENTITY-MIB",
	"ENTITY-SENSOR-MIB",
	"BRIDGE-MIB",
	"Q-BRIDGE-MIB",
	"IEEE8023-LAG-MIB",
	"CISCO-BGP4-MIB",
	"HUAWEI-ENTITY-EXTENT-MIB",
}

const embeddedSNMPMIBSource = "watchdog-builtin"

// EmbeddedSNMPMIBModules returns the exact MIB modules preloaded by the SNMP
// collector. The management API uses this inventory so the MIB module page is
// an honest view of collector capability instead of an unrelated empty table.
// Source is reserved: built-in modules are versioned with the binary and cannot
// be changed or deleted through CRUD.
func EmbeddedSNMPMIBModules() ([]MIBModule, error) {
	modules := make([]MIBModule, 0, len(snmpMIBEmbeddedModules))
	for _, name := range snmpMIBEmbeddedModules {
		content, err := watchdogEmbeddedMIBs.ReadFile("mibs/" + name)
		if err != nil {
			return nil, fmt.Errorf("read embedded mib %s: %w", name, err)
		}
		digest := sha256.Sum256(content)
		modules = append(modules, MIBModule{
			Name:     name,
			Source:   embeddedSNMPMIBSource,
			Version:  "embedded",
			Checksum: "sha256:" + hex.EncodeToString(digest[:]),
			Enabled:  true,
			Builtin:  true,
		})
	}
	return modules, nil
}

// IsEmbeddedSNMPMIBSource identifies the source namespace reserved for the
// immutable MIB bundle compiled into the collector.
func IsEmbeddedSNMPMIBSource(source string) bool {
	return source == embeddedSNMPMIBSource
}

// SNMPMIBRegistry resolves MIB object names ("IF-MIB::ifHCInOctets",
// "sysUpTime.0") to numeric OIDs and back. gosmi keeps global parser state,
// so there is one process-wide registry guarded by a mutex; resolved names
// are cached so steady-state lookups never re-enter the parser.
type SNMPMIBRegistry struct {
	mu         sync.Mutex
	oids       map[string]string // name reference -> numeric OID (no leading dot)
	names      map[string]string // numeric object OID -> "MODULE::name"
	loadedDirs map[string]bool
	initErr    error
}

var (
	snmpMIBRegistry     *SNMPMIBRegistry
	snmpMIBRegistryOnce sync.Once
)

func DefaultSNMPMIBRegistry() *SNMPMIBRegistry {
	snmpMIBRegistryOnce.Do(func() {
		registry := &SNMPMIBRegistry{
			oids:       map[string]string{},
			names:      map[string]string{},
			loadedDirs: map[string]bool{},
		}
		gosmi.Init()
		// Replace the host default search path (SMIPATH, /usr/share/...) with
		// the embedded bundle so resolution is deterministic across machines.
		bundle, err := fs.Sub(watchdogEmbeddedMIBs, "mibs")
		if err != nil {
			registry.initErr = fmt.Errorf("open embedded mib bundle: %w", err)
			snmpMIBRegistry = registry
			return
		}
		gosmi.SetFS(gosmi.NamedFS("watchdog-builtin", bundle.(fs.ReadDirFS)))
		var loadErrs []error
		for _, module := range snmpMIBEmbeddedModules {
			if _, err := gosmi.LoadModule(module); err != nil {
				loadErrs = append(loadErrs, fmt.Errorf("load embedded mib %s: %w", module, err))
			}
		}
		registry.initErr = errors.Join(loadErrs...)
		snmpMIBRegistry = registry
	})
	return snmpMIBRegistry
}

// ConfigureSNMPMIBRegistry layers extra MIB directories and modules from the
// `snmp` config block onto the embedded bundle. Load failures of individual
// modules are joined into the returned error but leave the registry usable.
func ConfigureSNMPMIBRegistry(cfg SNMPConfig) error {
	return DefaultSNMPMIBRegistry().Configure(cfg)
}

func (r *SNMPMIBRegistry) Configure(cfg SNMPConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, dir := range cfg.MIBDirs {
		dir = strings.TrimSpace(dir)
		if dir == "" || r.loadedDirs[dir] {
			continue
		}
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			errs = append(errs, fmt.Errorf("snmp mib dir %q is not a readable directory", dir))
			continue
		}
		gosmi.AppendPath(dir)
		r.loadedDirs[dir] = true
		// LibreNMS has nested layouts such as mibs/juniper/junos. Register
		// every readable directory so MIB capability discovery is independent
		// of the detected OS/vendor name and modules can be loaded lazily.
		walkErr := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() || path == dir {
				return nil
			}
			if strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			if !r.loadedDirs[path] {
				gosmi.AppendPath(path)
				r.loadedDirs[path] = true
			}
			return nil
		})
		if walkErr != nil {
			errs = append(errs, fmt.Errorf("walk snmp mib dir %q: %w", dir, walkErr))
		}
	}
	load := strings.TrimSpace(cfg.MIBLoad)
	switch {
	case strings.EqualFold(load, "ALL"):
		for dir := range r.loadedDirs {
			entries, err := os.ReadDir(dir)
			if err != nil {
				errs = append(errs, fmt.Errorf("read snmp mib dir %q: %w", dir, err))
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
					continue
				}
				if _, err := gosmi.LoadModule(entry.Name()); err != nil {
					errs = append(errs, fmt.Errorf("load mib %s: %w", entry.Name(), err))
				}
			}
		}
	case load != "":
		for _, module := range strings.FieldsFunc(load, func(r rune) bool { return r == ',' || r == ' ' || r == ';' }) {
			if _, err := gosmi.LoadModule(module); err != nil {
				errs = append(errs, fmt.Errorf("load mib %s: %w", module, err))
			}
		}
	}
	return errors.Join(errs...)
}

// OID resolves a MIB object reference to its numeric OID without a leading
// dot. Accepted forms: "IF-MIB::ifDescr", "ifDescr", "sysUpTime.0" (numeric
// instance suffix preserved), and numeric OIDs, which pass through unchanged.
func (r *SNMPMIBRegistry) OID(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", errors.New("mib object reference is empty")
	}
	if IsNumericOID(ref) {
		return strings.TrimPrefix(ref, "."), nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if oid, ok := r.oids[ref]; ok {
		return oid, nil
	}
	moduleName, symbol, suffix := splitMIBReference(ref)
	node, err := lookupMIBNode(moduleName, symbol)
	if err != nil {
		return "", fmt.Errorf("resolve mib object %q: %w", ref, err)
	}
	oid := node.Oid.String()
	qualified := node.GetModule().Name + "::" + node.Name
	r.names[oid] = qualified
	r.oids[qualified] = oid
	if suffix != "" {
		oid += "." + suffix
	}
	r.oids[ref] = oid
	return oid, nil
}

// MustOID is OID for package-level object tables built from the embedded
// bundle; failure there is a broken build, so it panics.
func (r *SNMPMIBRegistry) MustOID(ref string) string {
	oid, err := r.OID(ref)
	if err != nil {
		panic(fmt.Sprintf("watchdog snmp mib registry: %v", err))
	}
	return oid
}

// Name returns the qualified "MODULE::object" name for a numeric object OID,
// resolving through loaded MIB modules when it is not already cached.
func (r *SNMPMIBRegistry) Name(oid string) (string, error) {
	oid = strings.TrimPrefix(strings.TrimSpace(oid), ".")
	if oid == "" {
		return "", errors.New("oid is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if name, ok := r.names[oid]; ok {
		return name, nil
	}
	parsed, err := types.OidFromString(oid)
	if err != nil {
		return "", fmt.Errorf("parse oid %q: %w", oid, err)
	}
	node, err := gosmi.GetNodeByOID(parsed)
	if err != nil {
		return "", fmt.Errorf("resolve oid %q: %w", oid, err)
	}
	base := node.Oid.String()
	if base != oid {
		return "", fmt.Errorf("oid %q resolves only to prefix %s", oid, base)
	}
	qualified := node.GetModule().Name + "::" + node.Name
	r.names[oid] = qualified
	r.oids[qualified] = oid
	return qualified, nil
}

// DisplayName renders a numeric OID (optionally carrying an instance suffix,
// like a collection recipe's "1.3.6.1.2.1.31.1.1.1.6.101") in the LibreNMS
// textual style "IF-MIB::ifHCInOctets.101". OIDs that do not land on a known
// scalar, column, or notification are returned unchanged.
func (r *SNMPMIBRegistry) DisplayName(oid string) string {
	trimmed := strings.TrimPrefix(strings.TrimSpace(oid), ".")
	if trimmed == "" || !IsNumericOID(trimmed) {
		return oid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if name, ok := r.names[trimmed]; ok {
		return name
	}
	parsed, err := types.OidFromString(trimmed)
	if err != nil {
		return trimmed
	}
	node, err := gosmi.GetNodeByOID(parsed)
	if err != nil {
		return trimmed
	}
	switch node.Kind {
	case types.NodeScalar, types.NodeColumn, types.NodeNotification:
	default:
		return trimmed
	}
	base := node.Oid.String()
	if base != trimmed && !strings.HasPrefix(trimmed, base+".") {
		return trimmed
	}
	qualified := node.GetModule().Name + "::" + node.Name
	r.names[base] = qualified
	r.oids[qualified] = base
	return qualified + trimmed[len(base):]
}

// StateValues returns the enumeration of an INTEGER object (for example
// IF-MIB::ifOperStatus -> up(1), down(2), ...) for use as state translations.
func (r *SNMPMIBRegistry) StateValues(ref string) []SNMPStateValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	moduleName, symbol, _ := splitMIBReference(strings.TrimSpace(ref))
	node, err := lookupMIBNode(moduleName, symbol)
	if err != nil || node.SmiType == nil || node.SmiType.Enum == nil {
		return nil
	}
	values := make([]SNMPStateValue, 0, len(node.SmiType.Enum.Values))
	for _, named := range node.SmiType.Enum.Values {
		values = append(values, SNMPStateValue{Value: int(named.Value), Label: named.Name})
	}
	return values
}

func lookupMIBNode(moduleName, symbol string) (gosmi.SmiNode, error) {
	if moduleName != "" {
		module, err := gosmi.GetModule(moduleName)
		if err != nil {
			if _, loadErr := gosmi.LoadModule(moduleName); loadErr == nil {
				module, err = gosmi.GetModule(moduleName)
			}
		}
		if err == nil {
			return module.GetNode(symbol)
		}
	}
	return gosmi.GetNode(symbol)
}

func splitMIBReference(ref string) (moduleName, symbol, suffix string) {
	symbol = ref
	if before, after, ok := strings.Cut(symbol, "::"); ok {
		moduleName, symbol = before, after
	}
	if dot := strings.IndexByte(symbol, '.'); dot >= 0 {
		symbol, suffix = symbol[:dot], symbol[dot+1:]
	}
	return moduleName, symbol, suffix
}

func IsNumericOID(ref string) bool {
	trimmed := strings.TrimPrefix(ref, ".")
	if trimmed == "" {
		return false
	}
	for _, r := range trimmed {
		if r != '.' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// snmpMIBOID resolves a MIB object reference against the default registry.
// It backs the package-level OID tables in the discovery modules, so a
// resolution failure (a broken embedded bundle) fails fast at startup.
func snmpMIBOID(ref string) string {
	return DefaultSNMPMIBRegistry().MustOID(ref)
}

// snmpMIBDisplayOID renders a numeric OID as "MODULE::object[.instance]" for
// human-facing fields (collection recipes keep the numeric form separately in
// NumericOID). Unknown OIDs render unchanged.
func snmpMIBDisplayOID(oid string) string {
	return DefaultSNMPMIBRegistry().DisplayName(oid)
}
