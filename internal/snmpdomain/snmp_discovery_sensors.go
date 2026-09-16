package snmpdomain

import (
	"context"
	"strconv"
	"strings"
)

const (
	snmpCollectorModuleSensors = "sensors"

	MetricSNMPSensorValue = "watchdog_snmp_sensor_value"
	MetricSNMPSensorState = "watchdog_snmp_sensor_state"
)

type SNMPSensorsDiscoveryModule struct{}

func (SNMPSensorsDiscoveryModule) Name() string {
	return snmpCollectorModuleSensors
}

func (m SNMPSensorsDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	columns, err := m.walkSensorColumns(ctx, req)
	if err != nil {
		return DiscoveryResult{}, err
	}
	sensors := make([]Sensor, 0, len(columns[oidEntPhySensorValue]))
	recipes := make([]Recipe, 0, len(columns[oidEntPhySensorValue])*2)
	for index, rawValue := range columns[oidEntPhySensorValue] {
		if index == 0 {
			continue
		}
		class, unit := entitySensorClassAndUnit(columns[oidEntPhySensorType][index], columns[oidEntPhySensorUnits][index])
		name := firstNonEmptySNMPString(
			cleanSNMPValue(columns[oidEntPhysicalName][index]),
			cleanSNMPValue(columns[oidEntPhysicalDescr][index]),
			class+" "+strconv.FormatUint(index, 10),
		)
		value, _ := parseFloatValue(rawValue)
		sensor := Sensor{
			ID:          collectorStableID("sensor", "", string(req.Device.ID), class, strconv.FormatUint(index, 10), name),
			DeviceID:    req.Device.ID,
			SensorIndex: index,
			Class:       class,
			Name:        name,
			OID:         trimSNMPCollectorOID(oidEntPhySensorValue) + "." + strconv.FormatUint(index, 10),
			Unit:        unit,
			Value:       applyEntitySensorScale(value, columns[oidEntPhySensorScale][index], columns[oidEntPhySensorPrecision][index]),
			Status:      entitySensorStatus(columns[oidEntPhySensorOper][index]),
			Metadata: map[string]string{
				"sensor_type": columns[oidEntPhySensorType][index],
				"scale":       columns[oidEntPhySensorScale][index],
				"precision":   columns[oidEntPhySensorPrecision][index],
			},
		}
		sensors = append(sensors, sensor)
		recipes = append(recipes, sensorRecipes(req, sensor, columns[oidEntPhySensorScale][index], columns[oidEntPhySensorPrecision][index])...)
	}
	defSensors, defRecipes := discoverDefinitionSensors(ctx, req)
	sensors = append(sensors, defSensors...)
	recipes = append(recipes, defRecipes...)
	candSensors, candRecipes := probeSensorCandidates(ctx, req, claimedOIDSet(recipes))
	sensors = append(sensors, candSensors...)
	recipes = append(recipes, candRecipes...)
	return DiscoveryResult{Sensors: sensors, Recipes: recipes}, nil
}

func (m SNMPSensorsDiscoveryModule) walkSensorColumns(ctx context.Context, req DiscoveryContext) (map[string]map[uint64]string, error) {
	oids := []string{
		oidEntPhySensorType,
		oidEntPhySensorScale,
		oidEntPhySensorPrecision,
		oidEntPhySensorValue,
		oidEntPhySensorOper,
		oidEntPhySensorUnits,
		oidEntPhysicalName,
		oidEntPhysicalDescr,
	}
	columns := make(map[string]map[uint64]string, len(oids))
	for _, oid := range oids {
		response, err := req.Query.Walk(ctx, WalkRequest{
			Target:  req.Target,
			Profile: req.Profile,
			Context: "",
			BaseOID: oid,
			Flags: QueryFlags{
				UseBulk:        true,
				MaxRepetitions: 25,
			},
		})
		if err != nil {
			return nil, err
		}
		columns[oid] = valuesByNumericSuffix(oid, response)
	}
	return columns, nil
}

func sensorRecipes(req DiscoveryContext, sensor Sensor, scale string, precision string) []Recipe {
	idx := strconv.FormatUint(sensor.SensorIndex, 10)
	multiplier := entitySensorScaleMultiplier(scale, precision)
	definitions := []struct {
		metric    string
		oid       string
		valueType ValueType
		unit      string
	}{
		{MetricSNMPSensorValue, trimSNMPCollectorOID(oidEntPhySensorValue), ValueGauge, sensor.Unit},
		{MetricSNMPSensorState, trimSNMPCollectorOID(oidEntPhySensorOper), ValueState, ""},
	}
	recipes := make([]Recipe, 0, len(definitions))
	for _, definition := range definitions {
		recipeID := collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleSensors, string(EntitySensor), string(sensor.ID), definition.metric, idx, "")
		recipe := Recipe{
			ID:                    recipeID,
			DeviceID:              req.Device.ID,
			EntityType:            EntitySensor,
			EntityID:              sensor.ID,
			ModuleName:            snmpCollectorModuleSensors,
			MetricName:            definition.metric,
			ValueType:             definition.valueType,
			OID:                   snmpMIBDisplayOID(definition.oid + "." + idx),
			NumericOID:            definition.oid + "." + idx,
			OIDIndex:              idx,
			MIB:                   "ENTITY-SENSOR-MIB",
			PollerType:            "snmp",
			Unit:                  definition.unit,
			SampleIntervalSeconds: 60,
			Labels: map[string]string{
				"target_id":    string(req.TargetID),
				"sensor_id":    string(sensor.ID),
				"sensor_class": sensor.Class,
				"module":       snmpCollectorModuleSensors,
			},
			Options: map[string]string{
				"scale":     scale,
				"precision": precision,
			},
			Enabled: true,
		}
		if definition.metric == MetricSNMPSensorValue {
			recipe.Multiplier = multiplier
			recipe.HasMultiplier = true
		}
		recipes = append(recipes, recipe)
	}
	return recipes
}

func entitySensorScaleMultiplier(scale string, precision string) float64 {
	multiplier := 1.0
	switch parseUintValue(scale) {
	case 1:
		multiplier = 1e-24
	case 2:
		multiplier = 1e-21
	case 3:
		multiplier = 1e-18
	case 4:
		multiplier = 1e-15
	case 5:
		multiplier = 1e-12
	case 6:
		multiplier = 1e-9
	case 7:
		multiplier = 1e-6
	case 8:
		multiplier = 1e-3
	case 10:
		multiplier = 1e3
	case 11:
		multiplier = 1e6
	case 12:
		multiplier = 1e9
	case 13:
		multiplier = 1e12
	case 14:
		multiplier = 1e15
	case 15:
		multiplier = 1e18
	}
	for i := uint64(0); i < parseUintValue(precision); i++ {
		multiplier /= 10
	}
	return multiplier
}

func trimSNMPCollectorOID(oid string) string {
	return strings.TrimPrefix(oid, ".")
}
