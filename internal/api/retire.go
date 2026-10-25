package api

import (
	"errors"
	"net/http"

	clientv3 "go.etcd.io/etcd/client/v3"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// RetireInstance permanently retires one agent instance (E12-D033).
//
// The etcd marker is written first, and only when that instance's AgentRecord
// is absent. A present record is 409 and writes nothing, including the Postgres
// row. The marker is not deleted. A repeat call while the agent is still absent
// is 204. Postgres is inserted after the marker so a retry covers a lost insert;
// the marker already blocks registration if the insert fails.
func (s *Server) RetireInstance(w http.ResponseWriter, r *http.Request, name apiv1.InstanceName) {
	inst := string(name)
	plane, ok := s.cfg.Planes[inst]
	if !ok || inst == state.ControllerInstance ||
		(plane != state.PlaneSTS && plane != state.PlaneMD && plane != state.PlaneTD) {
		s.notFound(w)
		return
	}
	region, err := s.cfg.Topology.RegionOf(inst)
	if err != nil {
		s.notFound(w)
		return
	}
	views, _ := s.regionViews()
	if _, avail := views[region]; !avail {
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegionUnavailable, "Region Unavailable", "region is unavailable", nil)
		return
	}
	resp, err := txn(r.Context(), s.cl.APIWriter(),
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(state.AgentKey(plane, inst)), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(state.RetiredKey(inst), inst)},
		nil,
	)
	if err != nil {
		if errors.Is(err, errRegionUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegionUnavailable, "Region Unavailable", "region is unavailable", nil)
			return
		}
		s.log.Error().Err(err).Str("instance", inst).Msg("retire")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
		return
	}
	if !resp.Succeeded {
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeConflict, "Conflict", "agent record is present", nil)
		return
	}
	pool, err := s.db()
	if err != nil {
		s.log.Error().Err(err).Str("instance", inst).Msg("retire")
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeStorageUnavailable, "Storage Unavailable", "storage is unavailable", nil)
		return
	}
	_, err = pool.Exec(r.Context(), `
		INSERT INTO retired_instances (instance, plane, region, retired_by, retired_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (instance) DO NOTHING`,
		inst, string(plane), region, actorName(r), s.now().UTC())
	if err != nil {
		s.log.Error().Err(err).Str("instance", inst).Msg("retire")
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeStorageUnavailable, "Storage Unavailable", "storage is unavailable", nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
