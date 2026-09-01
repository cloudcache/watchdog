package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
)

type SNMPCollectorQueryEngine interface {
	Get(ctx context.Context, req SNMPCollectorGetRequest) (SNMPCollectorResponse, error)
	Walk(ctx context.Context, req SNMPCollectorWalkRequest) (SNMPCollectorResponse, error)
}

type GoSNMPCollectorQueryEngine struct{}

func NewGoSNMPCollectorQueryEngine() GoSNMPCollectorQueryEngine {
	return GoSNMPCollectorQueryEngine{}
}

func (e GoSNMPCollectorQueryEngine) Get(ctx context.Context, req SNMPCollectorGetRequest) (SNMPCollectorResponse, error) {
	if len(req.OIDs) == 0 {
		return SNMPCollectorResponse{}, nil
	}
	session, err := e.connect(ctx, req.Target, req.Profile, req.Context, req.Flags)
	if err != nil {
		return SNMPCollectorResponse{}, err
	}
	defer session.Conn.Close()

	maxOids := req.Flags.MaxOids
	if maxOids <= 0 {
		maxOids = 60
	}
	var response SNMPCollectorResponse
	for start := 0; start < len(req.OIDs); start += maxOids {
		end := start + maxOids
		if end > len(req.OIDs) {
			end = len(req.OIDs)
		}
		packet, err := session.Get(req.OIDs[start:end])
		if err != nil {
			return response, err
		}
		for _, pdu := range packet.Variables {
			response.VarBinds = append(response.VarBinds, snmpCollectorVarBindFromPDU(pdu))
		}
	}
	return response, nil
}

func (e GoSNMPCollectorQueryEngine) Walk(ctx context.Context, req SNMPCollectorWalkRequest) (SNMPCollectorResponse, error) {
	if strings.TrimSpace(req.BaseOID) == "" {
		return SNMPCollectorResponse{}, errors.New("walk base oid is required")
	}
	session, err := e.connect(ctx, req.Target, req.Profile, req.Context, req.Flags)
	if err != nil {
		return SNMPCollectorResponse{}, err
	}
	defer session.Conn.Close()

	var response SNMPCollectorResponse
	walkFn := session.Walk
	if req.Flags.UseBulk {
		walkFn = session.BulkWalk
	}
	err = walkFn(req.BaseOID, func(pdu gosnmp.SnmpPDU) error {
		response.VarBinds = append(response.VarBinds, snmpCollectorVarBindFromPDU(pdu))
		return nil
	})
	return response, err
}

func (e GoSNMPCollectorQueryEngine) connect(ctx context.Context, target SNMPCollectorTarget, profile SNMPProfile, contextName string, flags SNMPCollectorQueryFlags) (*gosnmp.GoSNMP, error) {
	host := strings.TrimSpace(target.Host)
	if host == "" {
		return nil, errors.New("snmp target host is required")
	}
	host, port := snmpHostPort(host)
	if target.Port != 0 {
		port = target.Port
	}
	timeout := profile.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	maxOids := flags.MaxOids
	if maxOids <= 0 {
		maxOids = 60
	}
	maxRepetitions := flags.MaxRepetitions
	if maxRepetitions == 0 {
		maxRepetitions = 25
	}
	session := &gosnmp.GoSNMP{
		Target:         host,
		Port:           port,
		Context:        ctx,
		Timeout:        timeout,
		Retries:        int(profile.Retries),
		MaxOids:        maxOids,
		MaxRepetitions: maxRepetitions,
		ContextName:    contextName,
	}
	switch profile.Version {
	case SNMPVersion3:
		configureGoSNMPV3(session, profile.Security)
	case SNMPVersion2c, "":
		session.Version = gosnmp.Version2c
		session.Community = profile.Security["community"]
	default:
		return nil, fmt.Errorf("unsupported snmp version %q", profile.Version)
	}
	if session.Retries == 0 {
		session.Retries = 1
	}
	if err := session.Connect(); err != nil {
		return nil, fmt.Errorf("snmp connect %s:%d: %w", host, port, err)
	}
	return session, nil
}

func configureGoSNMPV3(session *gosnmp.GoSNMP, security map[string]string) {
	authProtocol := snmpV3AuthProtocol(security["auth_protocol"])
	privProtocol := snmpV3PrivProtocol(security["priv_protocol"])
	msgFlags := snmpV3MsgFlags(security["security_level"], authProtocol, privProtocol)
	session.Version = gosnmp.Version3
	session.MsgFlags = gosnmp.Reportable | msgFlags
	session.SecurityModel = gosnmp.UserSecurityModel
	session.SecurityParameters = &gosnmp.UsmSecurityParameters{
		UserName:                 firstNonEmptySNMPSecurity(security["username"], security["security_name"], security["user"]),
		AuthenticationProtocol:   authProtocol,
		AuthenticationPassphrase: firstNonEmptySNMPSecurity(security["auth_password"], security["auth_passphrase"]),
		PrivacyProtocol:          privProtocol,
		PrivacyPassphrase:        firstNonEmptySNMPSecurity(security["priv_password"], security["privacy_password"], security["priv_passphrase"]),
	}
}

func snmpV3MsgFlags(level string, auth gosnmp.SnmpV3AuthProtocol, priv gosnmp.SnmpV3PrivProtocol) gosnmp.SnmpV3MsgFlags {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "noauthnopriv", "no_auth_no_priv", "noauth":
		return gosnmp.NoAuthNoPriv
	case "authnopriv", "auth_no_priv", "auth":
		return gosnmp.AuthNoPriv
	case "authpriv", "auth_priv", "priv":
		return gosnmp.AuthPriv
	}
	if priv != gosnmp.NoPriv {
		return gosnmp.AuthPriv
	}
	if auth != gosnmp.NoAuth {
		return gosnmp.AuthNoPriv
	}
	return gosnmp.NoAuthNoPriv
}

func snmpV3AuthProtocol(value string) gosnmp.SnmpV3AuthProtocol {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "md5":
		return gosnmp.MD5
	case "sha", "sha1":
		return gosnmp.SHA
	case "sha224":
		return gosnmp.SHA224
	case "sha256":
		return gosnmp.SHA256
	case "sha384":
		return gosnmp.SHA384
	case "sha512":
		return gosnmp.SHA512
	default:
		return gosnmp.NoAuth
	}
}

func snmpV3PrivProtocol(value string) gosnmp.SnmpV3PrivProtocol {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "des":
		return gosnmp.DES
	case "aes", "aes128":
		return gosnmp.AES
	case "aes192":
		return gosnmp.AES192
	case "aes256":
		return gosnmp.AES256
	case "aes192c":
		return gosnmp.AES192C
	case "aes256c":
		return gosnmp.AES256C
	default:
		return gosnmp.NoPriv
	}
}

func snmpCollectorVarBindFromPDU(pdu gosnmp.SnmpPDU) SNMPCollectorVarBind {
	return SNMPCollectorVarBind{
		OID:       strings.TrimPrefix(pdu.Name, "."),
		Value:     pdu.Value,
		ValueType: snmpCollectorValueTypeFromPDU(pdu),
	}
}

func snmpCollectorValueTypeFromPDU(pdu gosnmp.SnmpPDU) SNMPCollectorValueType {
	switch pdu.Type {
	case gosnmp.Counter32:
		return SNMPCollectorValueCounter32
	case gosnmp.Counter64:
		return SNMPCollectorValueCounter64
	case gosnmp.Gauge32, gosnmp.Integer:
		return SNMPCollectorValueGauge
	case gosnmp.TimeTicks:
		return SNMPCollectorValueTimeTicks
	case gosnmp.IPAddress:
		return SNMPCollectorValueIPAddr
	case gosnmp.OctetString:
		if _, ok := pdu.Value.([]byte); ok {
			return SNMPCollectorValueString
		}
		return SNMPCollectorValueString
	default:
		return SNMPCollectorValueString
	}
}

func firstNonEmptySNMPSecurity(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func snmpCollectorFloatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case uint:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case int32:
		return float64(v), true
	case float64:
		return v, true
	case []byte:
		f, err := strconv.ParseFloat(string(v), 64)
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

var _ SNMPCollectorQueryEngine = GoSNMPCollectorQueryEngine{}
