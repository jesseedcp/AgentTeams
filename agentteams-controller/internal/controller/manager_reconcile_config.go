package controller

import (
	"context"
	"fmt"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileManagerConfig publishes all Manager desired state to object storage.
// DeployManagerConfig writes referenced artifacts before the generation-stamped
// runtime document, which acts as the AgentScope activation barrier.
func (r *ManagerReconciler) reconcileManagerConfig(ctx context.Context, s *managerScope) (reconcile.Result, error) {
	if s.provResult == nil {
		return reconcile.Result{}, nil
	}

	m := s.manager
	isUpdate := m.Status.Phase != "" && m.Status.Phase != "Pending" && m.Status.Phase != "Failed"

	if err := r.Deployer.DeployPackage(ctx, m.Name, m.Spec.Package, isUpdate); err != nil {
		return reconcile.Result{}, fmt.Errorf("deploy package: %w", err)
	}

	var aiGatewayURL string
	if s.modelProviderInfo != nil {
		aiGatewayURL = s.modelProviderInfo.IntranetURL
	}
	if err := r.Deployer.DeployManagerConfig(ctx, service.ManagerDeployRequest{
		Name:            m.Name,
		RuntimeRevision: m.Generation,
		MatrixUserID:    s.provResult.MatrixUserID,
		Spec:            m.Spec,
		MatrixToken:     s.provResult.MatrixToken,
		GatewayKey:      s.provResult.GatewayKey,
		MatrixPassword:  s.provResult.MatrixPassword,
		MinIOPassword:   s.provResult.MinIOPassword,
		McpServers:      m.Spec.McpServers,
		AIGatewayURL:    aiGatewayURL,
		IsUpdate:        isUpdate,
	}); err != nil {
		return reconcile.Result{}, fmt.Errorf("deploy manager config: %w", err)
	}

	return reconcile.Result{}, nil
}
