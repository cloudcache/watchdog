package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

var errMetricScopeForbidden = errors.New("metric is outside the caller's resource grants")

// metricAllowed applies an optional per-user metric allow-list. No rows means
// unrestricted for backward compatibility; one or more rows means explicit
// allow-list. Role abilities remain the separate operation gate.
func (s *Server) metricAllowed(ctx context.Context, p *principal, metric string) (bool, error) {
	if p == nil {
		return false, nil
	}
	if p.IsAdmin {
		return true, nil
	}
	var total, matching int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),COALESCE(SUM(metric=?),0)
		FROM user_metric_permissions WHERE user_id=?`, metric, p.UserID).Scan(&total, &matching)
	return total == 0 || matching > 0, err
}

func (s *Server) requireMetricAccess(c *gin.Context, metric string) bool {
	allowed, err := s.metricAllowed(c.Request.Context(), currentPrincipal(c), metric)
	if err != nil {
		writeSQLError(c, err)
		return false
	}
	if !allowed {
		fail(c, http.StatusForbidden, "forbidden", errMetricScopeForbidden.Error())
		return false
	}
	return true
}

func (s *Server) directlyGrantedAggregateGraph(ctx context.Context, p *principal, graphID string) (bool, error) {
	if p == nil {
		return false, nil
	}
	if p.IsAdmin {
		return true, nil
	}
	var granted bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM user_aggregate_graph_permissions
		WHERE user_id=? AND aggregate_graph_id=?)`, p.UserID, graphID).Scan(&granted)
	return granted, err
}

// appendAggregateGraphScopeSQL scopes aggregate_graphs alias "g". An explicit
// graph grant authorizes the saved aggregate output. Otherwise every selected
// port and, when configured, every metric must pass the ordinary grants.
func appendAggregateGraphScopeSQL(where []string, args []any, p *principal) ([]string, []any) {
	if p != nil && p.IsAdmin {
		return where, args
	}
	if p == nil {
		return append(where, "1=0"), args
	}
	conditions := []string{`(
		NOT EXISTS (SELECT 1 FROM user_metric_permissions any_metric WHERE any_metric.user_id=?)
		OR NOT EXISTS (
			SELECT 1 FROM aggregate_graph_items scoped_item
			WHERE scoped_item.aggregate_graph_id=g.id AND NOT EXISTS (
				SELECT 1 FROM user_metric_permissions allowed_metric
				WHERE allowed_metric.user_id=? AND allowed_metric.metric=scoped_item.metric
			)
		)
	)`}
	conditionArgs := []any{p.UserID, p.UserID}
	if !p.can("device.viewAll") && !p.can("port.viewAll") {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM aggregate_graph_ports scoped_graph_port
			JOIN ports scoped_port ON scoped_port.id=scoped_graph_port.port_id
			WHERE scoped_graph_port.aggregate_graph_id=g.id AND NOT (
				EXISTS (SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=scoped_port.id)
				OR EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=scoped_port.device_id)
				OR EXISTS (SELECT 1 FROM user_device_group_permissions ugp
					JOIN device_group_members gm ON gm.device_group_id=ugp.device_group_id
					WHERE ugp.user_id=? AND gm.device_id=scoped_port.device_id)
			)
		)`)
		conditionArgs = append(conditionArgs, p.UserID, p.UserID, p.UserID)
	}
	where = append(where, `(
		EXISTS (SELECT 1 FROM user_aggregate_graph_permissions direct_graph WHERE direct_graph.user_id=? AND direct_graph.aggregate_graph_id=g.id)
		OR (`+strings.Join(conditions, " AND ")+`)
	)`)
	args = append(args, p.UserID)
	args = append(args, conditionArgs...)
	return where, args
}

func (s *Server) requireAggregateGraphMetrics(ctx context.Context, p *principal, graphID string) error {
	if p == nil {
		return errSNMPScopeForbidden
	}
	if p.IsAdmin {
		return nil
	}
	var restricted int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_metric_permissions WHERE user_id=?`, p.UserID).Scan(&restricted); err != nil {
		return err
	}
	if restricted == 0 {
		return nil
	}
	var denied int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM aggregate_graph_items i
		WHERE i.aggregate_graph_id=? AND NOT EXISTS (
			SELECT 1 FROM user_metric_permissions ump WHERE ump.user_id=? AND ump.metric=i.metric
		)`, graphID, p.UserID).Scan(&denied)
	if err != nil {
		return err
	}
	if denied > 0 {
		return errMetricScopeForbidden
	}
	return nil
}

func writeRBACResourceError(c *gin.Context, err error) {
	if errors.Is(err, errMetricScopeForbidden) || errors.Is(err, errSNMPScopeForbidden) {
		fail(c, http.StatusForbidden, "forbidden", err.Error())
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	writeSQLError(c, err)
}
