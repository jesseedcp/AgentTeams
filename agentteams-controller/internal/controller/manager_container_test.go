package controller

import (
	"context"
	"sync"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/auth"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/test/testutil/mocks"
)

// captureManagerCreateLabels exercises createManagerContainer with fully
// mocked dependencies and returns the Labels map the reconciler handed
// to the backend.Create call. Using a capturing CreateFn lets us lock
// in the exact merged Pod-label set without spinning up envtest.
func captureManagerCreateLabels(t *testing.T, mgr *v1beta1.Manager) map[string]string {
	t.Helper()
	return captureManagerCreateRequest(t, mgr, nil).Labels
}

func captureManagerCreateRequest(t *testing.T, mgr *v1beta1.Manager, defaults *backend.ResourceRequirements) backend.CreateRequest {
	t.Helper()
	mockBackend := mocks.NewMockWorkerBackend()
	var (
		mu      sync.Mutex
		capture backend.CreateRequest
	)
	mockBackend.CreateFn = func(_ context.Context, req backend.CreateRequest) (*backend.WorkerResult, error) {
		mu.Lock()
		capture = req
		mu.Unlock()
		return &backend.WorkerResult{Name: req.Name, Backend: "mock", Status: backend.StatusStarting}, nil
	}

	r := &ManagerReconciler{
		Provisioner:      mocks.NewMockManagerProvisioner(),
		EnvBuilder:       mocks.NewMockManagerEnvBuilder(),
		ResourcePrefix:   auth.DefaultResourcePrefix,
		ControllerName:   "real-ctl",
		DefaultRuntime:   backend.RuntimeAgentScope,
		ManagerResources: defaults,
	}

	scope := &managerScope{
		manager: mgr,
		provResult: &service.ManagerProvisionResult{
			MatrixUserID: "@manager:localhost",
			MatrixToken:  "mock-token",
			RoomID:       "!room:localhost",
			GatewayKey:   "gw-key",
		},
	}

	if _, err := r.createManagerContainer(context.Background(), scope, mockBackend); err != nil {
		t.Fatalf("createManagerContainer: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return capture
}

// TestCreateManagerContainer_MergesMetadataAndSpecLabels verifies the
// full three-layer composition the Manager reconciler performs: CR
// metadata.labels and CR spec.labels both reach the Pod, spec wins over
// metadata on collision, and controller-forced system labels (app,
// agentteams.io/manager, agentteams.io/controller, agentteams.io/role,
// agentteams.io/runtime) are always present and correct.
func TestCreateManagerContainer_MergesMetadataAndSpecLabels(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.ObjectMeta.Labels = map[string]string{
		"owner": "alice",
		"tier":  "metadata-tier",
	}
	m.Spec.Labels = map[string]string{
		"env":  "prod",
		"tier": "spec-tier", // overrides metadata
	}
	m.Spec.Runtime = backend.RuntimeAgentScope

	labels := captureManagerCreateLabels(t, m)

	cases := map[string]string{
		"owner":                 "alice",      // metadata.labels propagated
		"env":                   "prod",       // spec.labels propagated
		"tier":                  "spec-tier",  // spec beats metadata
		"agentteams.io/manager": "default",    // system label
		"agentteams.io/role":    "manager",    // system label
		"agentteams.io/runtime": "agentscope", // system label
		"app":                   "agentteams-manager",
		v1beta1.LabelController: "real-ctl",
	}
	for k, want := range cases {
		if got := labels[k]; got != want {
			t.Errorf("label %q = %q, want %q (full=%v)", k, got, want, labels)
		}
	}
}

// TestCreateManagerContainer_SystemLabelsOverrideUserLabels verifies
// the reserved-key contract: a user putting agentteams.io/controller or
// app into their CR labels (metadata or spec) cannot spoof the
// controller's identity — the system layer is applied last and wins
// silently.
func TestCreateManagerContainer_SystemLabelsOverrideUserLabels(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.ObjectMeta.Labels = map[string]string{
		v1beta1.LabelController: "metadata-attacker",
		"app":                   "evil-app",
	}
	m.Spec.Labels = map[string]string{
		v1beta1.LabelController: "spec-attacker",
		"agentteams.io/role":    "evil-role",
		"agentteams.io/manager": "spoofed",
	}
	m.Spec.Runtime = backend.RuntimeAgentScope

	labels := captureManagerCreateLabels(t, m)

	if got := labels[v1beta1.LabelController]; got != "real-ctl" {
		t.Errorf("controller label got %q, want real-ctl (full=%v)", got, labels)
	}
	if got := labels["app"]; got != "agentteams-manager" {
		t.Errorf("app label got %q, want agentteams-manager", got)
	}
	if got := labels["agentteams.io/role"]; got != "manager" {
		t.Errorf("role label got %q, want manager", got)
	}
	if got := labels["agentteams.io/manager"]; got != "default" {
		t.Errorf("manager label got %q, want default", got)
	}
}

// TestCreateManagerContainer_NilLabelsSafe ensures a Manager CR with no
// user labels at all still emits exactly the system label set without
// panicking.
func TestCreateManagerContainer_NilLabelsSafe(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.Spec.Runtime = backend.RuntimeAgentScope

	labels := captureManagerCreateLabels(t, m)

	for _, k := range []string{
		"app",
		"agentteams.io/manager",
		"agentteams.io/role",
		"agentteams.io/runtime",
		v1beta1.LabelController,
	} {
		if _, ok := labels[k]; !ok {
			t.Errorf("missing system label %q on labelless Manager (full=%v)", k, labels)
		}
	}
}

func TestCreateManagerContainerSpecResourcesOverrideDefaults(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.Spec.Resources = &v1beta1.AgentResourceRequirements{
		Requests: v1beta1.AgentResourceValues{CPU: "750m", Memory: "1536Mi"},
		Limits:   v1beta1.AgentResourceValues{CPU: "3", Memory: "5Gi"},
	}

	req := captureManagerCreateRequest(t, m, &backend.ResourceRequirements{
		CPURequest:    "100m",
		MemoryRequest: "256Mi",
		CPULimit:      "1",
		MemoryLimit:   "2Gi",
	})

	if req.Resources == nil {
		t.Fatal("CreateRequest.Resources = nil, want manager spec resources")
	}
	if req.Resources.CPURequest != "750m" || req.Resources.MemoryRequest != "1536Mi" ||
		req.Resources.CPULimit != "3" || req.Resources.MemoryLimit != "5Gi" {
		t.Fatalf("CreateRequest.Resources = %+v", req.Resources)
	}
}

func TestCreateManagerContainerSpecResourcesPartiallyOverrideDefaults(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.Spec.Resources = &v1beta1.AgentResourceRequirements{
		Limits: v1beta1.AgentResourceValues{CPU: "3"},
	}

	req := captureManagerCreateRequest(t, m, &backend.ResourceRequirements{
		CPURequest:    "100m",
		MemoryRequest: "256Mi",
		CPULimit:      "1",
		MemoryLimit:   "2Gi",
	})

	if req.Resources == nil {
		t.Fatal("CreateRequest.Resources = nil, want merged manager resources")
	}
	if req.Resources.CPURequest != "100m" || req.Resources.MemoryRequest != "256Mi" ||
		req.Resources.CPULimit != "3" || req.Resources.MemoryLimit != "2Gi" {
		t.Fatalf("CreateRequest.Resources = %+v", req.Resources)
	}
}

func TestCreateManagerContainerUsesDefaultResourcesWhenSpecResourcesUnset(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	defaults := &backend.ResourceRequirements{
		CPURequest:    "100m",
		MemoryRequest: "256Mi",
		CPULimit:      "1",
		MemoryLimit:   "2Gi",
	}

	req := captureManagerCreateRequest(t, m, defaults)

	if req.Resources != defaults {
		t.Fatalf("CreateRequest.Resources = %+v, want default pointer %+v", req.Resources, defaults)
	}
}

func TestCreateManagerContainerMountsCodingCLIBinariesReadOnly(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.Spec.Runtime = backend.RuntimeAgentScope
	m.Spec.CodingCLI = &v1beta1.ManagerCodingCLISpec{
		Enabled:          true,
		Providers:        []string{"claude"},
		HostPath:         "/srv/agentteams-coding-cli",
		MountPath:        "/opt/vendor-coding",
		TrustedDirectory: "/opt/vendor-coding/bin",
	}

	req := captureManagerCreateRequest(t, m, nil)
	if len(req.Volumes) != 1 {
		t.Fatalf("CreateRequest.Volumes = %+v, want one coding CLI mount", req.Volumes)
	}
	got := req.Volumes[0]
	if got.HostPath != "/srv/agentteams-coding-cli" ||
		got.ContainerPath != "/opt/vendor-coding" ||
		!got.ReadOnly {
		t.Fatalf("coding CLI mount = %+v", got)
	}
}

func TestCreateManagerContainerRejectsInvalidCodingCLI(t *testing.T) {
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Namespace = "agentteams"
	m.Spec.Runtime = backend.RuntimeAgentScope
	m.Spec.CodingCLI = &v1beta1.ManagerCodingCLISpec{
		Enabled:   true,
		Providers: []string{"shell"},
	}

	mockBackend := mocks.NewMockWorkerBackend()
	r := &ManagerReconciler{
		Provisioner:    mocks.NewMockManagerProvisioner(),
		EnvBuilder:     mocks.NewMockManagerEnvBuilder(),
		ResourcePrefix: auth.DefaultResourcePrefix,
	}
	_, err := r.createManagerContainer(
		context.Background(),
		&managerScope{
			manager: m,
			provResult: &service.ManagerProvisionResult{
				MatrixToken: "token",
			},
		},
		mockBackend,
	)
	if err == nil {
		t.Fatal("createManagerContainer accepted invalid coding CLI spec")
	}
}

func TestModelAndMCPChangeDoNotChangeManagerPodHash(t *testing.T) {
	before := v1beta1.ManagerSpec{
		Model:   "qwen3.6-plus",
		Runtime: backend.RuntimeAgentScope,
		Image:   "agentteams/agentteams-manager:v1",
		McpServers: []v1beta1.MCPServer{{
			Name: "github",
			URL:  "http://higress/mcp/github",
		}},
	}
	after := before
	after.Model = "new-model"
	after.McpServers = []v1beta1.MCPServer{{
		Name: "jira",
		URL:  "http://higress/mcp/jira",
	}}
	after.Soul = "new soul"
	after.Agents = "new agents"
	after.Skills = []string{"worker-management"}
	after.Config.HeartbeatInterval = "15m"

	if got, want := hashAppliedManagerSpec(after), hashAppliedManagerSpec(before); got != want {
		t.Fatalf("config-only hash=%q, want unchanged %q", got, want)
	}
}

func TestPodAffectingManagerFieldsChangePodHash(t *testing.T) {
	base := v1beta1.ManagerSpec{
		Runtime: backend.RuntimeAgentScope,
		Image:   "agentteams/agentteams-manager:v1",
		Resources: &v1beta1.AgentResourceRequirements{
			Limits: v1beta1.AgentResourceValues{CPU: "2"},
		},
		AccessEntries: []v1beta1.AccessEntry{{Service: "storage"}},
		Env:           map[string]string{"EXTRA": "one"},
		Labels:        map[string]string{"tier": "manager"},
	}
	baseHash := hashAppliedManagerSpec(base)
	cases := map[string]v1beta1.ManagerSpec{}

	changed := base
	changed.Image = "agentteams/agentteams-manager:v2"
	cases["image"] = changed
	changed = base
	changed.Runtime = "future-manager"
	cases["runtime"] = changed
	changed = base
	changed.Resources = &v1beta1.AgentResourceRequirements{
		Limits: v1beta1.AgentResourceValues{CPU: "3"},
	}
	cases["resources"] = changed
	changed = base
	changed.AccessEntries = []v1beta1.AccessEntry{{Service: "other"}}
	cases["accessEntries"] = changed
	changed = base
	changed.Env = map[string]string{"EXTRA": "two"}
	cases["env"] = changed
	changed = base
	changed.Labels = map[string]string{"tier": "other"}
	cases["labels"] = changed
	changed = base
	changed.CodingCLI = &v1beta1.ManagerCodingCLISpec{
		Enabled:   true,
		Providers: []string{"claude"},
	}
	cases["codingCLI"] = changed

	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			if got := hashAppliedManagerSpec(spec); got == baseHash {
				t.Fatalf("%s did not change pod hash %q", name, got)
			}
		})
	}
}

func TestReconcileManagerConfigPublishesGenerationStampedDocument(t *testing.T) {
	deployer := mocks.NewMockManagerDeployer()
	reconciler := &ManagerReconciler{Deployer: deployer}
	manager := &v1beta1.Manager{}
	manager.Name = "default"
	manager.Generation = 9
	manager.Spec = v1beta1.ManagerSpec{
		Model:      "qwen3.6-plus",
		Runtime:    backend.RuntimeAgentScope,
		Skills:     []string{"worker-management"},
		McpServers: []v1beta1.MCPServer{{Name: "github", URL: "http://mcp"}},
	}
	provisioned := &service.ManagerProvisionResult{
		MatrixUserID:   "@manager:matrix.example.com",
		MatrixToken:    "matrix-token",
		GatewayKey:     "gateway-key",
		MinIOPassword:  "minio-password",
		MatrixPassword: "matrix-password",
	}

	_, err := reconciler.reconcileManagerConfig(
		context.Background(),
		&managerScope{manager: manager, provResult: provisioned},
	)
	if err != nil {
		t.Fatalf("reconcileManagerConfig: %v", err)
	}
	if got := len(deployer.Calls.DeployManagerConfig); got != 1 {
		t.Fatalf("DeployManagerConfig calls=%d, want 1", got)
	}
	request := deployer.Calls.DeployManagerConfig[0]
	if got, want := request.RuntimeRevision, manager.Generation; got != want {
		t.Fatalf("RuntimeRevision=%d, want generation %d", got, want)
	}
	if got, want := request.MatrixUserID, provisioned.MatrixUserID; got != want {
		t.Fatalf("MatrixUserID=%q, want %q", got, want)
	}
	if got := len(deployer.Calls.PushOnDemandSkills); got != 0 {
		t.Fatalf("legacy PushOnDemandSkills calls=%d, want 0", got)
	}
}

func TestReconcileManagerInfrastructureKeepsModelProviderOutOfProvision(t *testing.T) {
	prov := mocks.NewMockManagerProvisioner()
	r := &ManagerReconciler{Provisioner: prov}
	m := &v1beta1.Manager{}
	m.Name = "default"

	scope := &managerScope{
		manager:           m,
		modelProviderInfo: &gateway.ModelProviderInfo{HttpApiID: "qwen-http-api"},
	}

	if _, err := r.reconcileManagerInfrastructure(context.Background(), scope); err != nil {
		t.Fatalf("reconcileManagerInfrastructure: %v", err)
	}
	if len(prov.Calls.ProvisionManager) != 1 {
		t.Fatalf("ProvisionManager calls=%d, want 1", len(prov.Calls.ProvisionManager))
	}
	if got := prov.Calls.ProvisionManager[0].Name; got != "default" {
		t.Fatalf("ProvisionManager Name=%q, want default", got)
	}
}

func TestReconcileManagerInfrastructureRestoresGatewayAuth(t *testing.T) {
	prov := mocks.NewMockManagerProvisioner()
	r := &ManagerReconciler{Provisioner: prov}
	m := &v1beta1.Manager{}
	m.Name = "default"
	m.Status.MatrixUserID = "@manager:localhost"

	scope := &managerScope{
		manager:           m,
		modelProviderInfo: &gateway.ModelProviderInfo{HttpApiID: "openai-http-api"},
	}

	if _, err := r.reconcileManagerInfrastructure(context.Background(), scope); err != nil {
		t.Fatalf("reconcileManagerInfrastructure: %v", err)
	}
	if len(prov.Calls.EnsureManagerGatewayAuth) != 1 {
		t.Fatalf("EnsureManagerGatewayAuth calls=%d, want 1", len(prov.Calls.EnsureManagerGatewayAuth))
	}
	call := prov.Calls.EnsureManagerGatewayAuth[0]
	if call.Name != "default" {
		t.Fatalf("EnsureManagerGatewayAuth name=%q, want default", call.Name)
	}
	if call.GatewayKey == "" {
		t.Fatal("EnsureManagerGatewayAuth GatewayKey is empty")
	}
}
