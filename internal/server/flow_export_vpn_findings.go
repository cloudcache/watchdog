// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/parquet-go/parquet-go"
)

// flow_export_vpn_findings.go is the "vpn_findings" flavor of the unified flow
// export (KISS-06). It migrates the hub's dedicated VPN-findings CSV export
// (export_flow_vpn.go) onto the opjob engine: the worker loads the materialized
// flow_vpn_findings matching the frozen filter/sort and renders CSV or Parquet.
// Sensitive columns (evidence, probe result, conversation key, disposition note)
// are intentionally omitted from artifacts, faithful to the hub.

// vpnFindingsExportSpec is the frozen findings selection persisted in the export
// job: the window, text search, column filters and sort the operator saw.
type vpnFindingsExportSpec struct {
	From          time.Time           `json:"from"`
	To            time.Time           `json:"to"`
	Search        string              `json:"search,omitempty"`
	ColumnFilters map[string][]string `json:"column_filters,omitempty"`
	SortBy        string              `json:"sort_by,omitempty"`
	SortDirection string              `json:"sort_direction,omitempty"`
}

// vpnFindingsExportSortColumns are the sort keys the export accepts; membership is
// the injection guard for the ORDER BY column.
var vpnFindingsExportSortColumns = map[string]struct{}{
	"window_end": {}, "generated_at": {}, "local_ip": {}, "remote_ip": {},
	"primary_protocol": {}, "primary_local_port": {}, "primary_remote_port": {},
	"local_to_remote_bytes": {}, "remote_to_local_bytes": {}, "remote_asn": {},
	"remote_country": {}, "score": {}, "risk_level": {}, "verdict": {}, "disposition": {}, "probe_status": {},
}

func (s *Server) runFlowVPNFindingsExportArtifact(ctx context.Context, p *principal, payload flowExportPayload) ([]byte, int, error) {
	if payload.VPNFindings == nil {
		return nil, 0, opjob.TerminalError(errors.New("invalid vpn findings export payload"))
	}
	if !p.can("flow.vpn.view") || !p.can("flow.export.raw") {
		return nil, 0, opjob.TerminalError(errors.New("vpn findings export requires flow.vpn.view and flow.export.raw"))
	}
	items, err := s.loadVPNFindingsForExport(ctx, *payload.VPNFindings, payload.MaxRows)
	if err != nil {
		return nil, 0, err
	}
	if payload.Format == "parquet" {
		data, err := renderVPNFindingsParquet(items)
		return data, len(items), err
	}
	data, err := renderVPNFindingsCSV(items)
	return data, len(items), err
}

func (s *Server) loadVPNFindingsForExport(ctx context.Context, spec vpnFindingsExportSpec, limit uint32) ([]vpnFindingDTO, error) {
	where, args, err := vpnFindingsExportWhere(spec)
	if err != nil {
		return nil, opjob.TerminalError(err)
	}
	order := "window_end"
	if spec.SortBy != "" {
		if _, ok := vpnFindingsExportSortColumns[spec.SortBy]; !ok {
			return nil, opjob.TerminalError(fmt.Errorf("unsupported sort field %q", spec.SortBy))
		}
		order = spec.SortBy
	}
	direction := "ASC"
	if strings.EqualFold(spec.SortDirection, "desc") {
		direction = "DESC"
	}
	if limit == 0 || limit > maxFlowDetailExportRows {
		limit = maxFlowDetailExportRows
	}
	query := vpnFindingSelect + " WHERE " + strings.Join(where, " AND ") +
		" ORDER BY " + order + " " + direction + ", id " + direction + " LIMIT ?"
	rows, err := s.db.QueryContext(ctx, query, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]vpnFindingDTO, 0, 256)
	for rows.Next() {
		item, err := scanVPNFinding(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// vpnFindingsExportWhere builds the findings WHERE clause from the frozen spec,
// validating every column filter against the shared facet allowlist.
func vpnFindingsExportWhere(spec vpnFindingsExportSpec) ([]string, []any, error) {
	where := []string{"1=1"}
	var args []any
	if !spec.From.IsZero() && !spec.To.IsZero() {
		if !spec.To.After(spec.From) {
			return nil, nil, errors.New("vpn findings export range is invalid")
		}
		where = append(where, "window_end>=?", "window_end<?")
		args = append(args, spec.From.UTC(), spec.To.UTC())
	}
	if search := strings.TrimSpace(spec.Search); search != "" {
		like := "%" + escapeLike(search) + "%"
		where = append(where, "(id LIKE ? OR local_ip LIKE ? OR remote_ip LIKE ? OR remote_prefix_id LIKE ?)")
		args = append(args, like, like, like, like)
	}
	for field, values := range spec.ColumnFilters {
		if _, ok := vpnFindingFacetColumns[field]; !ok {
			return nil, nil, fmt.Errorf("unsupported filter field %q", field)
		}
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if field == "remote_country" {
				value = strings.ToUpper(value)
			}
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		if len(placeholders) > 0 {
			where = append(where, field+" IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	return where, args, nil
}

var vpnFindingsExportHeader = []string{
	"id", "window_start", "window_end", "local_ip", "remote_ip", "primary_protocol",
	"primary_local_port", "primary_remote_port", "local_to_remote_bytes", "remote_to_local_bytes",
	"flow_record_count", "active_bucket_count", "max_duration_ms", "packet_bytes_p50", "remote_asn",
	"remote_country", "remote_prefix_id", "local_prefix_id", "complete_ratio", "score", "risk_level",
	"verdict", "symmetry_ratio", "dominance_ratio", "probe_recommended", "probe_block_reason",
	"decision_rule_id", "rule_set_version", "source_generation", "generated_at", "disposition", "probe_status",
}

func renderVPNFindingsCSV(items []vpnFindingDTO) ([]byte, error) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	if err := writer.Write(vpnFindingsExportHeader); err != nil {
		return nil, err
	}
	for _, item := range items {
		record := []string{
			safeSpreadsheetCell(item.ID), item.WindowStart.UTC().Format(time.RFC3339Nano), item.WindowEnd.UTC().Format(time.RFC3339Nano),
			safeSpreadsheetCell(item.LocalIP), safeSpreadsheetCell(item.RemoteIP), strconv.FormatUint(uint64(item.PrimaryProtocol), 10),
			strconv.FormatUint(uint64(item.PrimaryLocalPort), 10), strconv.FormatUint(uint64(item.PrimaryRemotePort), 10),
			strconv.FormatUint(item.LocalToRemoteBytes, 10), strconv.FormatUint(item.RemoteToLocalBytes, 10),
			strconv.FormatUint(item.FlowRecordCount, 10), strconv.FormatUint(uint64(item.ActiveBucketCount), 10),
			strconv.FormatUint(item.MaxDurationMS, 10), strconv.FormatUint(item.PacketBytesP50, 10), strconv.FormatUint(uint64(item.RemoteASN), 10),
			safeSpreadsheetCell(item.RemoteCountry), safeSpreadsheetCell(item.RemotePrefixID), safeSpreadsheetCell(item.LocalPrefixID),
			strconv.FormatFloat(item.CompleteRatio, 'f', -1, 64), strconv.FormatUint(uint64(item.Score), 10), safeSpreadsheetCell(item.RiskLevel),
			safeSpreadsheetCell(item.Verdict), strconv.FormatFloat(item.SymmetryRatio, 'f', -1, 64), strconv.FormatFloat(item.DominanceRatio, 'f', -1, 64),
			strconv.FormatBool(item.ProbeRecommended), safeSpreadsheetCell(item.ProbeBlockReason),
			safeSpreadsheetCell(item.DecisionRuleID), safeSpreadsheetCell(item.RuleSetVersion), strconv.FormatUint(item.SourceGeneration, 10),
			item.GeneratedAt.UTC().Format(time.RFC3339Nano), safeSpreadsheetCell(item.Disposition), safeSpreadsheetCell(item.ProbeStatus),
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

type vpnFindingsParquetRow struct {
	ID                 string  `parquet:"id,dict"`
	WindowStart        int64   `parquet:"window_start,timestamp(millisecond:utc)"`
	WindowEnd          int64   `parquet:"window_end,timestamp(millisecond:utc)"`
	LocalIP            string  `parquet:"local_ip,dict"`
	RemoteIP           string  `parquet:"remote_ip,dict"`
	PrimaryProtocol    uint32  `parquet:"primary_protocol"`
	PrimaryLocalPort   uint32  `parquet:"primary_local_port"`
	PrimaryRemotePort  uint32  `parquet:"primary_remote_port"`
	LocalToRemoteBytes uint64  `parquet:"local_to_remote_bytes"`
	RemoteToLocalBytes uint64  `parquet:"remote_to_local_bytes"`
	FlowRecordCount    uint64  `parquet:"flow_record_count"`
	ActiveBucketCount  uint32  `parquet:"active_bucket_count"`
	MaxDurationMS      uint64  `parquet:"max_duration_ms"`
	PacketBytesP50     uint64  `parquet:"packet_bytes_p50"`
	RemoteASN          uint32  `parquet:"remote_asn"`
	RemoteCountry      string  `parquet:"remote_country,dict"`
	RemotePrefixID     string  `parquet:"remote_prefix_id,dict"`
	LocalPrefixID      string  `parquet:"local_prefix_id,dict"`
	CompleteRatio      float64 `parquet:"complete_ratio"`
	Score              uint32  `parquet:"score"`
	RiskLevel          string  `parquet:"risk_level,dict"`
	Verdict            string  `parquet:"verdict,dict"`
	SymmetryRatio      float64 `parquet:"symmetry_ratio"`
	DominanceRatio     float64 `parquet:"dominance_ratio"`
	ProbeRecommended   bool    `parquet:"probe_recommended"`
	ProbeBlockReason   string  `parquet:"probe_block_reason"`
	DecisionRuleID     string  `parquet:"decision_rule_id,dict"`
	RuleSetVersion     string  `parquet:"rule_set_version,dict"`
	SourceGeneration   uint64  `parquet:"source_generation"`
	GeneratedAt        int64   `parquet:"generated_at,timestamp(millisecond:utc)"`
	Disposition        string  `parquet:"disposition,dict"`
	ProbeStatus        string  `parquet:"probe_status,dict"`
}

func renderVPNFindingsParquet(items []vpnFindingDTO) ([]byte, error) {
	rows := make([]vpnFindingsParquetRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, vpnFindingsParquetRow{
			ID: item.ID, WindowStart: item.WindowStart.UnixMilli(), WindowEnd: item.WindowEnd.UnixMilli(),
			LocalIP: item.LocalIP, RemoteIP: item.RemoteIP, PrimaryProtocol: uint32(item.PrimaryProtocol),
			PrimaryLocalPort: uint32(item.PrimaryLocalPort), PrimaryRemotePort: uint32(item.PrimaryRemotePort),
			LocalToRemoteBytes: item.LocalToRemoteBytes, RemoteToLocalBytes: item.RemoteToLocalBytes,
			FlowRecordCount: item.FlowRecordCount, ActiveBucketCount: item.ActiveBucketCount, MaxDurationMS: item.MaxDurationMS,
			PacketBytesP50: item.PacketBytesP50, RemoteASN: item.RemoteASN, RemoteCountry: item.RemoteCountry,
			RemotePrefixID: item.RemotePrefixID, LocalPrefixID: item.LocalPrefixID, CompleteRatio: item.CompleteRatio,
			Score: uint32(item.Score), RiskLevel: item.RiskLevel, Verdict: item.Verdict,
			SymmetryRatio: item.SymmetryRatio, DominanceRatio: item.DominanceRatio, ProbeRecommended: item.ProbeRecommended,
			ProbeBlockReason: item.ProbeBlockReason, DecisionRuleID: item.DecisionRuleID, RuleSetVersion: item.RuleSetVersion,
			SourceGeneration: item.SourceGeneration, GeneratedAt: item.GeneratedAt.UnixMilli(),
			Disposition: item.Disposition, ProbeStatus: item.ProbeStatus,
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, rows); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
