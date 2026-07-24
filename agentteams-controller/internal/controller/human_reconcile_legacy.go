package controller

import (
	"context"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcileHumanLegacy syncs humans-registry.json in embedded mode.
//
// This is a best-effort, non-fatal operation: it runs last in the phase
// chain and its errors are logged but never returned, matching the
// pre-refactor behavior. The registry remains only as an import/diagnostics
// compatibility projection and its absence is acceptable — the Human
// resource and Matrix room membership remain authoritative.
//
// No-op when Legacy is nil (incluster/cloud mode) or not Enabled().
func (r *HumanReconciler) reconcileHumanLegacy(ctx context.Context, s *humanScope) {
	if r.Legacy == nil || !r.Legacy.Enabled() {
		return
	}
	logger := log.FromContext(ctx)
	h := s.human

	if err := r.Legacy.UpdateHumansRegistry(service.HumanRegistryEntry{
		Name:            h.Name,
		MatrixUserID:    h.Status.MatrixUserID,
		DisplayName:     h.Spec.DisplayName,
		PermissionLevel: h.Spec.PermissionLevel,
		AccessibleTeams: h.Spec.AccessibleTeams,
	}); err != nil {
		logger.Error(err, "humans-registry update failed (non-fatal)")
	}
}
