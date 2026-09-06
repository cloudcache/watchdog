package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/watchdog"
	g "github.com/gosnmp/gosnmp"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	apiURL := flag.String("api", "", "watchdog API base URL (e.g. http://127.0.0.1:8090)")
	token := flag.String("token", "", "watchdog API auth token")
	listen := flag.String("listen", "", "UDP listen address for SNMP traps")
	flag.Parse()

	cfg, err := watchdog.LoadWatchdogConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	trapCfg := cfg.SNMPTrapAgent
	if strings.TrimSpace(*apiURL) != "" {
		trapCfg.APIURL = *apiURL
	}
	if *token != "" {
		trapCfg.Token = *token
	}
	if strings.TrimSpace(*listen) != "" {
		trapCfg.Listen = *listen
	}
	trapCfg, err = watchdog.NormalizeAndValidateSNMPTrapAgentConfig(trapCfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	trapListener := g.NewTrapListener()
	trapListener.OnNewTrap = func(packet *g.SnmpPacket, addr *net.UDPAddr) {
		handleTrap(ctx, trapCfg.APIURL, trapCfg.Token, packet, addr)
	}
	trapListener.Params = &g.GoSNMP{
		Version: g.Version2c,
		Logger:  g.NewLogger(log.New(os.Stderr, "", 0)),
	}

	go func() {
		<-ctx.Done()
		trapListener.Close()
	}()

	log.Printf("watchdog snmp trap agent listening on UDP %s, forwarding to %s", trapCfg.Listen, trapCfg.APIURL)
	if err := trapListener.Listen(trapCfg.Listen); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("trap listener stopped: %v", err)
	}
}

type netUDPAddr = net.UDPAddr

func handleTrap(ctx context.Context, apiURL, token string, packet *g.SnmpPacket, addr *net.UDPAddr) {
	trapOID := ""
	varbinds := make([]watchdog.SNMPTrapVarBind, 0, len(packet.Variables))
	for _, vb := range packet.Variables {
		if vb.Type == g.ObjectIdentifier {
			trapOID = fmt.Sprintf("%v", vb.Value)
		}
		varbinds = append(varbinds, watchdog.SNMPTrapVarBind{
			OID:   vb.Name,
			Value: fmt.Sprintf("%v", vb.Value),
		})
	}
	sourceIP := packet.AgentAddress
	if sourceIP == "" {
		sourceIP = fmt.Sprintf("%v", addr)
	}
	payload := map[string]any{
		"source_ip": sourceIP,
		"hostname":  packet.Community,
		"trap_oid":  trapOID,
		"varbinds":  varbinds,
		"raw_text":  fmt.Sprintf("%v", packet),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("marshal trap payload: %v", err)
		return
	}
	endpoint, err := url.JoinPath(apiURL, "api/v1/snmp/traps")
	if err != nil {
		log.Printf("build trap API endpoint: %v", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("forward trap to API: %v", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("forward trap to API: unexpected status %s", resp.Status)
	}
}
