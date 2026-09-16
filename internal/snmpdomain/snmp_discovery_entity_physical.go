package snmpdomain

import (
	"context"
	"strconv"
	"strings"
)

const snmpCollectorModuleEntityPhysical = "entity-physical"

var (
	snmpOIDEntPhysicalDescr       = snmpMIBOID("ENTITY-MIB::entPhysicalDescr")
	snmpOIDEntPhysicalClass       = snmpMIBOID("ENTITY-MIB::entPhysicalClass")
	snmpOIDEntPhysicalName        = snmpMIBOID("ENTITY-MIB::entPhysicalName")
	snmpOIDEntPhysicalVendorType  = snmpMIBOID("ENTITY-MIB::entPhysicalVendorType")
	snmpOIDEntPhysicalContainedIn = snmpMIBOID("ENTITY-MIB::entPhysicalContainedIn")
	snmpOIDEntPhysicalHWRev       = snmpMIBOID("ENTITY-MIB::entPhysicalHardwareRev")
	snmpOIDEntPhysicalSerial      = snmpMIBOID("ENTITY-MIB::entPhysicalSerialNum")
	snmpOIDEntPhysicalMfgName     = snmpMIBOID("ENTITY-MIB::entPhysicalMfgName")
	snmpOIDEntPhysicalModelName   = snmpMIBOID("ENTITY-MIB::entPhysicalModelName")
	snmpOIDEntPhysicalIsFRU       = snmpMIBOID("ENTITY-MIB::entPhysicalIsFRU")
)

type SNMPEntityPhysicalDiscoveryModule struct{}

func (SNMPEntityPhysicalDiscoveryModule) Name() string { return snmpCollectorModuleEntityPhysical }

func (m SNMPEntityPhysicalDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	oids := []string{
		snmpOIDEntPhysicalDescr, snmpOIDEntPhysicalClass, snmpOIDEntPhysicalName,
		snmpOIDEntPhysicalVendorType, snmpOIDEntPhysicalContainedIn,
		snmpOIDEntPhysicalSerial, snmpOIDEntPhysicalMfgName, snmpOIDEntPhysicalModelName,
		snmpOIDEntPhysicalIsFRU,
	}
	columns := make(map[string]map[uint64]string, len(oids))
	for _, oid := range oids {
		resp, err := req.Query.Walk(ctx, WalkRequest{
			Target: req.Target, Profile: req.Profile, BaseOID: oid,
			Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
		})
		if err != nil {
			return DiscoveryResult{}, err
		}
		columns[oid] = valuesByNumericSuffix(oid, resp)
	}
	entities := make([]PhysicalEntity, 0, len(columns[snmpOIDEntPhysicalDescr]))
	for index := range columns[snmpOIDEntPhysicalDescr] {
		if index == 0 {
			continue
		}
		entities = append(entities, PhysicalEntity{
			Index:            index,
			Name:             cleanSNMPValue(columns[snmpOIDEntPhysicalName][index]),
			Description:      cleanSNMPValue(columns[snmpOIDEntPhysicalDescr][index]),
			Class:            entPhysicalClass(columns[snmpOIDEntPhysicalClass][index]),
			VendorType:       cleanSNMPValue(columns[snmpOIDEntPhysicalVendorType][index]),
			ContainedIn:      parseUintValue(columns[snmpOIDEntPhysicalContainedIn][index]),
			SerialNumber:     cleanSNMPValue(columns[snmpOIDEntPhysicalSerial][index]),
			ManufacturerName: cleanSNMPValue(columns[snmpOIDEntPhysicalMfgName][index]),
			ModelName:        cleanSNMPValue(columns[snmpOIDEntPhysicalModelName][index]),
			IsFRU:            parseUintValue(columns[snmpOIDEntPhysicalIsFRU][index]) == 1,
		})
	}
	return DiscoveryResult{PhysicalEntities: entities}, nil
}

func entPhysicalClass(value string) string {
	cleaned := cleanSNMPValue(value)
	if start := strings.LastIndex(cleaned, "("); start > 0 && strings.HasSuffix(cleaned, ")") {
		return strings.TrimSpace(cleaned[:start])
	}
	switch parseUintValue(cleaned) {
	case 3:
		return "module"
	case 4:
		return "port"
	case 6:
		return "backplane"
	case 7:
		return "container"
	case 8:
		return "powerSupply"
	case 9:
		return "fan"
	case 10:
		return "sensor"
	case 12:
		return "cpu"
	case 13:
		return "memory"
	default:
		return "other"
	}
}

func init() {
	_ = strconv.Itoa
}
