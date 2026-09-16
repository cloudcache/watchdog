package snmpdomain

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
)

type GoSNMPQueryEngine struct{}

func NewGoSNMPQueryEngine() GoSNMPQueryEngine { return GoSNMPQueryEngine{} }

func (GoSNMPQueryEngine) Get(ctx context.Context, req GetRequest) (QueryResponse, error) {
	if len(req.OIDs) == 0 {
		return QueryResponse{}, nil
	}
	session, err := connectSNMP(ctx, req.Target, req.Profile, req.Context, req.Flags)
	if err != nil {
		return QueryResponse{}, err
	}
	defer session.Conn.Close()
	maxOids := req.Flags.MaxOids
	if maxOids <= 0 {
		maxOids = 60
	}
	var response QueryResponse
	var chunkErr error
	failedChunks, totalChunks := 0, 0
	for start := 0; start < len(req.OIDs); start += maxOids {
		end := start + maxOids
		if end > len(req.OIDs) {
			end = len(req.OIDs)
		}
		totalChunks++
		packet, getErr := session.Get(req.OIDs[start:end])
		if getErr != nil {
			failedChunks++
			chunkErr = getErr
			continue
		}
		for _, pdu := range packet.Variables {
			response.VarBinds = append(response.VarBinds, varBindFromPDU(pdu))
		}
	}
	if failedChunks == totalChunks && chunkErr != nil {
		return response, chunkErr
	}
	if failedChunks > 0 {
		log.Printf("snmp get target=%s partial: %d/%d chunks failed, last error: %v", req.Target.Host, failedChunks, totalChunks, chunkErr)
	}
	return response, nil
}

func (GoSNMPQueryEngine) Walk(ctx context.Context, req WalkRequest) (QueryResponse, error) {
	if strings.TrimSpace(req.BaseOID) == "" {
		return QueryResponse{}, errors.New("walk base oid is required")
	}
	session, err := connectSNMP(ctx, req.Target, req.Profile, req.Context, req.Flags)
	if err != nil {
		return QueryResponse{}, err
	}
	defer session.Conn.Close()
	var response QueryResponse
	walk := session.Walk
	if req.Flags.UseBulk {
		walk = session.BulkWalk
	}
	err = walk(req.BaseOID, func(pdu gosnmp.SnmpPDU) error {
		response.VarBinds = append(response.VarBinds, varBindFromPDU(pdu))
		return nil
	})
	return response, err
}

func connectSNMP(ctx context.Context, target QueryTarget, profile Profile, contextName string, flags QueryFlags) (*gosnmp.GoSNMP, error) {
	host := strings.TrimSpace(target.Host)
	if host == "" {
		return nil, errors.New("snmp target host is required")
	}
	host, port := splitSNMPHostPort(host)
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
	session := &gosnmp.GoSNMP{Target: host, Port: port, Context: ctx, Timeout: timeout, Retries: int(profile.Retries), MaxOids: maxOids, MaxRepetitions: maxRepetitions, ContextName: contextName}
	switch profile.Version {
	case Version3:
		configureV3(session, profile.Security)
	case Version1:
		session.Version, session.Community = gosnmp.Version1, profile.Security["community"]
	case Version2c, "":
		session.Version, session.Community = gosnmp.Version2c, profile.Security["community"]
	default:
		return nil, fmt.Errorf("unsupported snmp version %q", profile.Version)
	}
	if session.Retries == 0 {
		session.Retries = 1
	}
	if err := session.Connect(); err != nil {
		return nil, fmt.Errorf("snmp connect %s:%d: %w", session.Target, session.Port, err)
	}
	return session, nil
}

func splitSNMPHostPort(host string) (string, uint16) {
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

func configureV3(session *gosnmp.GoSNMP, security map[string]string) {
	auth := v3AuthProtocol(security["auth_protocol"])
	priv := v3PrivProtocol(security["priv_protocol"])
	flags := gosnmp.NoAuthNoPriv
	switch strings.ToLower(strings.TrimSpace(security["security_level"])) {
	case "authpriv", "auth_priv", "priv":
		flags = gosnmp.AuthPriv
	case "authnopriv", "auth_no_priv", "auth":
		flags = gosnmp.AuthNoPriv
	default:
		if priv != gosnmp.NoPriv {
			flags = gosnmp.AuthPriv
		} else if auth != gosnmp.NoAuth {
			flags = gosnmp.AuthNoPriv
		}
	}
	session.Version = gosnmp.Version3
	session.MsgFlags = gosnmp.Reportable | flags
	session.SecurityModel = gosnmp.UserSecurityModel
	session.SecurityParameters = &gosnmp.UsmSecurityParameters{
		UserName:                 firstNonEmpty(security["username"], security["security_name"], security["user"]),
		AuthenticationProtocol:   auth,
		AuthenticationPassphrase: firstNonEmpty(security["auth_password"], security["auth_passphrase"]),
		PrivacyProtocol:          priv,
		PrivacyPassphrase:        firstNonEmpty(security["priv_password"], security["privacy_password"], security["priv_passphrase"]),
	}
}

func v3AuthProtocol(value string) gosnmp.SnmpV3AuthProtocol {
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

func v3PrivProtocol(value string) gosnmp.SnmpV3PrivProtocol {
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func varBindFromPDU(pdu gosnmp.SnmpPDU) VarBind {
	valueType := ValueString
	switch pdu.Type {
	case gosnmp.Counter32:
		valueType = ValueCounter32
	case gosnmp.Counter64:
		valueType = ValueCounter64
	case gosnmp.Gauge32, gosnmp.Integer:
		valueType = ValueGauge
	case gosnmp.TimeTicks:
		valueType = ValueTimeTicks
	case gosnmp.IPAddress:
		valueType = ValueIPAddr
	}
	return VarBind{OID: strings.TrimPrefix(pdu.Name, "."), Value: pdu.Value, ValueType: valueType}
}

var _ QueryEngine = GoSNMPQueryEngine{}
