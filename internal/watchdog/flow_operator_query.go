package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const flowOperatorQueryBindingSchemaVersion = uint16(1)

var (
	ErrFlowOperatorQueryInvalid     = errors.New("Flow operator query selection is invalid")
	ErrFlowOperatorQueryUnavailable = errors.New("Flow operator query classification is unavailable")
)

// FlowOperatorQueryBinding is the immutable bridge between a management-plane
// operator and the customer ISP identity already stored in Flow facts. The
// publication/version lists pin a request to the event-time enrichment pairs
// whose installed milestones were checked before query execution.
type FlowOperatorQueryBinding struct {
	OperatorID             ID
	FlowISPID              uint16
	PublicationIDs         []ID
	DimensionSnapshotIDs   []string
	ClassificationVersions []uint32
	ExpectedWorkers        uint32
}

type FlowOperatorQueryBindingReader interface {
	ResolveFlowOperatorQueryBinding(context.Context, ID, ID, time.Time, time.Time) (FlowOperatorQueryBinding, error)
}

type flowOperatorPublicationWindow struct {
	ID                    ID
	DimensionSnapshotID   string
	ClassificationVersion uint32
	EffectiveFrom         time.Time
}

// ResolveFlowOperatorQueryBinding uses one repeatable-read snapshot so an
// operator identity, publication timeline, active worker set and ACK counts
// cannot be combined from different control-plane moments.
func (s *MySQLStore) ResolveFlowOperatorQueryBinding(ctx context.Context, tenantID, operatorID ID, from, to time.Time) (FlowOperatorQueryBinding, error) {
	if s == nil || s.db == nil || ctx == nil || tenantID == "" || operatorID == "" || from.IsZero() || to.IsZero() || !to.After(from) {
		return FlowOperatorQueryBinding{}, ErrFlowOperatorQueryInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	defer tx.Rollback()

	var flowISPID uint16
	var enabled bool
	if err := tx.QueryRowContext(ctx, `
		SELECT flow_isp_id, enabled
		FROM isp_operators
		WHERE tenant_id = ? AND id = ?
	`, tenantID, operatorID).Scan(&flowISPID, &enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return FlowOperatorQueryBinding{}, ErrFlowOperatorQueryInvalid
		}
		return FlowOperatorQueryBinding{}, err
	}
	if flowISPID == 0 || !enabled {
		return FlowOperatorQueryBinding{}, ErrFlowOperatorQueryInvalid
	}

	rows, err := tx.QueryContext(ctx, `
		WITH publication_timeline AS (
			SELECT id, dimension_snapshot_id, classification_version, effective_from,
			       LEAD(effective_from) OVER (ORDER BY effective_from) AS next_effective_from
			FROM flow_enrichment_publications
			WHERE tenant_id = ?
		)
		SELECT id, dimension_snapshot_id, classification_version, effective_from
		FROM publication_timeline
		WHERE effective_from < ?
		  AND (next_effective_from IS NULL OR next_effective_from > ?)
		ORDER BY effective_from
	`, tenantID, to.UTC(), from.UTC())
	if err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	publications := make([]flowOperatorPublicationWindow, 0, 4)
	for rows.Next() {
		var publication flowOperatorPublicationWindow
		if err := rows.Scan(&publication.ID, &publication.DimensionSnapshotID, &publication.ClassificationVersion, &publication.EffectiveFrom); err != nil {
			rows.Close()
			return FlowOperatorQueryBinding{}, err
		}
		publication.EffectiveFrom = publication.EffectiveFrom.UTC()
		publications = append(publications, publication)
	}
	if err := rows.Close(); err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	if err := rows.Err(); err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	if len(publications) == 0 || publications[0].EffectiveFrom.After(from.UTC()) {
		return FlowOperatorQueryBinding{}, ErrFlowOperatorQueryUnavailable
	}
	if len(publications) > 100 {
		return FlowOperatorQueryBinding{}, fmt.Errorf("%w: query crosses more than 100 enrichment publications", ErrFlowOperatorQueryUnavailable)
	}

	var expectedWorkers uint32
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM collector_agents
		WHERE tenant_id = ? AND module_key = 'flow' AND agent_type = 'flow_worker'
		  AND status = 'active' AND deleted_at IS NULL
	`, tenantID).Scan(&expectedWorkers); err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	if expectedWorkers == 0 {
		return FlowOperatorQueryBinding{}, ErrFlowOperatorQueryUnavailable
	}

	placeholders := make([]string, len(publications))
	args := make([]any, 0, len(publications)+1)
	args = append(args, tenantID)
	for index, publication := range publications {
		placeholders[index] = "?"
		args = append(args, publication.ID)
	}
	readyRows, err := tx.QueryContext(ctx, `
		SELECT publications.id, COUNT(DISTINCT CASE WHEN acknowledgements.installed_at IS NOT NULL THEN workers.id END)
		FROM flow_enrichment_publications AS publications
		CROSS JOIN collector_agents AS workers
		LEFT JOIN flow_enrichment_publication_acks AS acknowledgements
		  ON acknowledgements.tenant_id = publications.tenant_id
		 AND acknowledgements.publication_id = publications.id
		 AND acknowledgements.worker_id = workers.id
		WHERE publications.tenant_id = ?
		  AND publications.id IN (`+strings.Join(placeholders, ",")+`)
		  AND workers.tenant_id = publications.tenant_id
		  AND workers.module_key = 'flow' AND workers.agent_type = 'flow_worker'
		  AND workers.status = 'active' AND workers.deleted_at IS NULL
		GROUP BY publications.id
	`, args...)
	if err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	readyByPublication := make(map[ID]uint32, len(publications))
	for readyRows.Next() {
		var publicationID ID
		var ready uint32
		if err := readyRows.Scan(&publicationID, &ready); err != nil {
			readyRows.Close()
			return FlowOperatorQueryBinding{}, err
		}
		readyByPublication[publicationID] = ready
	}
	if err := readyRows.Close(); err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	if err := readyRows.Err(); err != nil {
		return FlowOperatorQueryBinding{}, err
	}

	binding := FlowOperatorQueryBinding{
		OperatorID: operatorID, FlowISPID: flowISPID, ExpectedWorkers: expectedWorkers,
		PublicationIDs:         make([]ID, 0, len(publications)),
		DimensionSnapshotIDs:   make([]string, 0, len(publications)),
		ClassificationVersions: make([]uint32, 0, len(publications)),
	}
	dimensions := make(map[string]struct{}, len(publications))
	for _, publication := range publications {
		if readyByPublication[publication.ID] != expectedWorkers {
			return FlowOperatorQueryBinding{}, fmt.Errorf("%w: publication %s installed on %d of %d active workers", ErrFlowOperatorQueryUnavailable, publication.ID, readyByPublication[publication.ID], expectedWorkers)
		}
		binding.PublicationIDs = append(binding.PublicationIDs, publication.ID)
		binding.ClassificationVersions = append(binding.ClassificationVersions, publication.ClassificationVersion)
		dimensions[publication.DimensionSnapshotID] = struct{}{}
	}
	for dimension := range dimensions {
		binding.DimensionSnapshotIDs = append(binding.DimensionSnapshotIDs, dimension)
	}
	sort.Strings(binding.DimensionSnapshotIDs)
	if err := tx.Commit(); err != nil {
		return FlowOperatorQueryBinding{}, err
	}
	return binding, nil
}
