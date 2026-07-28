package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUpdateWorkerClearsDesiredFieldsAndReturnsProof(t *testing.T) {
	scheme := newServerTestScheme(t)
	worker := &v1beta1.Worker{}
	worker.Name = "clearable"
	worker.Namespace = "default"
	worker.Spec.Model = "qwen3.5-plus"
	worker.Spec.Runtime = "copaw"
	worker.Spec.Identity = "old identity"
	worker.Spec.Soul = "old soul"
	worker.Spec.Skills = []string{"git"}
	worker.Spec.Package = "oss://workers/old.zip"
	worker.Spec.Expose = []v1beta1.ExposePort{{Port: 8080}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(worker).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")

	body := []byte(`{
		"identity":"",
		"soul":"",
		"skills":[],
		"package":"",
		"expose":[]
	}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/workers/clearable",
		bytes.NewReader(body),
	)
	req.SetPathValue("name", "clearable")
	rec := httptest.NewRecorder()
	handler.UpdateWorker(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusOK,
			rec.Code,
			rec.Body.String(),
		)
	}
	var got v1beta1.Worker
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKey{Name: "clearable", Namespace: "default"},
		&got,
	); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if got.Spec.Identity != "" ||
		got.Spec.Soul != "" ||
		got.Spec.Package != "" ||
		len(got.Spec.Skills) != 0 ||
		len(got.Spec.Expose) != 0 {
		t.Fatalf("desired fields were not cleared: %+v", got.Spec)
	}
	var resp WorkerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Identity != "" ||
		resp.Soul != "" ||
		resp.Package != "" ||
		len(resp.Skills) != 0 ||
		len(resp.Expose) != 0 {
		t.Fatalf("response does not prove cleared fields: %+v", resp)
	}
}

// /api/v1/workers/{name} must synthesize a response for a team member even
// though no Worker CR exists. The synthesized response MUST carry the
// RoomID + MatrixUserID recorded in Team.Status.Members so that clients like
// the Manager Agent and `agt get workers <name> -o json | jq .roomID`
// (exercised by test-21-team-project-dag) can resolve a member's room.
//
// This is the regression guard for the PR #666 bug where teamMemberToResponse
// synthesized an empty RoomID because Team.Status had no per-member RoomID
// field.

func TestResourceResponsesIncludeMCPServers(t *testing.T) {
	servers := []v1beta1.MCPServer{
		{
			Name:      "github",
			URL:       "https://gateway/mcp/github",
			Transport: "http",
		},
	}
	worker := &v1beta1.Worker{}
	worker.Name = "alice"
	worker.Spec.McpServers = servers
	manager := &v1beta1.Manager{}
	manager.Name = "default"
	manager.Spec.McpServers = servers

	workerResponse := workerToResponse(worker)
	managerResponse := managerToResponse(manager)

	if len(workerResponse.McpServers) != 1 ||
		workerResponse.McpServers[0].Name != "github" {
		t.Fatalf("worker mcpServers = %#v", workerResponse.McpServers)
	}
	if len(managerResponse.McpServers) != 1 ||
		managerResponse.McpServers[0].Name != "github" {
		t.Fatalf("manager mcpServers = %#v", managerResponse.McpServers)
	}
}

func TestCreateWorkerRejectsManagerRuntime(t *testing.T) {
	scheme := newServerTestScheme(t)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")

	body := []byte(`{"name":"worker-cr","runtime":"agentscope"}`)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/workers",
		bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()
	handler.CreateWorker(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusBadRequest,
			rec.Code,
			rec.Body.String(),
		)
	}
}

func TestUpdateWorkerRejectsManagerRuntime(t *testing.T) {
	scheme := newServerTestScheme(t)
	worker := &v1beta1.Worker{}
	worker.Name = "worker-cr"
	worker.Namespace = "default"
	worker.Spec.Runtime = backend.RuntimeOpenClaw
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(worker).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")
	body := []byte(`{"runtime":"agentscope"}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/workers/worker-cr",
		bytes.NewReader(body),
	)
	req.SetPathValue("name", "worker-cr")
	rec := httptest.NewRecorder()

	handler.UpdateWorker(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusBadRequest,
			rec.Code,
			rec.Body.String(),
		)
	}
}

func TestCreateWorkerAcceptsEveryWorkerRuntime(t *testing.T) {
	for _, workerRuntime := range []string{
		backend.RuntimeOpenClaw,
		backend.RuntimeCopaw,
		backend.RuntimeHermes,
		backend.RuntimeQwenPaw,
	} {
		t.Run(workerRuntime, func(t *testing.T) {
			scheme := newServerTestScheme(t)
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				Build()
			handler := NewResourceHandler(
				k8sClient,
				"default",
				nil,
				"",
			)
			body := []byte(
				`{"name":"worker-cr","runtime":"` +
					workerRuntime +
					`"}`,
			)
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/workers",
				bytes.NewReader(body),
			)
			rec := httptest.NewRecorder()

			handler.CreateWorker(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf(
					"expected status %d, got %d: %s",
					http.StatusCreated,
					rec.Code,
					rec.Body.String(),
				)
			}
		})
	}
}

func TestWorkerConsoleCreateAndUpdateRoundTrip(t *testing.T) {
	scheme := newServerTestScheme(t)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")

	createReq := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/workers",
		bytes.NewReader([]byte(`{
			"name":"console-worker",
			"runtime":"copaw",
			"console":{"enabled":true,"port":9090}
		}`)),
	)
	createRec := httptest.NewRecorder()
	handler.CreateWorker(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d: %s", createRec.Code, createRec.Body.String())
	}

	var created WorkerResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Console == nil || !created.Console.Enabled || created.Console.Port != 9090 {
		t.Fatalf("create response console = %#v", created.Console)
	}

	updateReq := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/workers/console-worker",
		bytes.NewReader([]byte(`{"console":{"enabled":false}}`)),
	)
	updateReq.SetPathValue("name", "console-worker")
	updateRec := httptest.NewRecorder()
	handler.UpdateWorker(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d: %s", updateRec.Code, updateRec.Body.String())
	}

	var updated WorkerResponse
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	if updated.Console == nil || updated.Console.Enabled {
		t.Fatalf("update response console = %#v, want disabled", updated.Console)
	}

	var got v1beta1.Worker
	if err := k8sClient.Get(
		context.Background(),
		client.ObjectKey{Name: "console-worker", Namespace: "default"},
		&got,
	); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if got.Spec.Console == nil || got.Spec.Console.Enabled {
		t.Fatalf("persisted console = %#v, want disabled", got.Spec.Console)
	}
}

func TestWorkerConsoleRejectsUnsupportedRuntimeAndInvalidPort(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unsupported runtime",
			body: `{"name":"bad-console","runtime":"openclaw","console":{"enabled":true}}`,
		},
		{
			name: "invalid port",
			body: `{"name":"bad-console","runtime":"copaw","console":{"enabled":true,"port":70000}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newServerTestScheme(t)
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			handler := NewResourceHandler(k8sClient, "default", nil, "")
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/workers",
				bytes.NewReader([]byte(tt.body)),
			)
			rec := httptest.NewRecorder()
			handler.CreateWorker(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestWorkerConsoleRejectsRuntimeChangeThatWouldInvalidateConsole(t *testing.T) {
	scheme := newServerTestScheme(t)
	worker := &v1beta1.Worker{}
	worker.Name = "console-worker"
	worker.Namespace = "default"
	worker.Spec.Runtime = backend.RuntimeCopaw
	worker.Spec.Console = &v1beta1.WorkerConsoleSpec{Enabled: true}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(worker).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")

	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/workers/console-worker",
		bytes.NewReader([]byte(`{"runtime":"hermes"}`)),
	)
	req.SetPathValue("name", "console-worker")
	rec := httptest.NewRecorder()
	handler.UpdateWorker(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestGetHumanIncludesPermissionScope(t *testing.T) {
	scheme := newServerTestScheme(t)
	human := &v1beta1.Human{}
	human.Name = "reviewer"
	human.Namespace = "default"
	human.Spec = v1beta1.HumanSpec{
		DisplayName:       "Reviewer",
		Email:             "reviewer@example.com",
		PermissionLevel:   2,
		AccessibleTeams:   []string{"alpha"},
		AccessibleWorkers: []string{"alpha-dev"},
		Note:              "release reviewer",
	}
	human.Status = v1beta1.HumanStatus{
		Phase:        "Active",
		MatrixUserID: "@reviewer:example",
		Rooms:        []string{"!alpha:example"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(human).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/humans/reviewer", nil)
	req.SetPathValue("name", "reviewer")
	rec := httptest.NewRecorder()

	handler.GetHuman(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var response HumanResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode human response: %v", err)
	}
	if response.PermissionLevel != 2 {
		t.Fatalf("permissionLevel=%d, want 2", response.PermissionLevel)
	}
	if len(response.AccessibleTeams) != 1 || response.AccessibleTeams[0] != "alpha" {
		t.Fatalf("accessibleTeams=%v, want [alpha]", response.AccessibleTeams)
	}
	if len(response.AccessibleWorkers) != 1 || response.AccessibleWorkers[0] != "alpha-dev" {
		t.Fatalf("accessibleWorkers=%v, want [alpha-dev]", response.AccessibleWorkers)
	}
	if response.Email != "reviewer@example.com" || response.Note != "release reviewer" {
		t.Fatalf("human metadata not preserved: %#v", response)
	}
}

func TestUpdateHumanChangesPermissionScope(t *testing.T) {
	scheme := newServerTestScheme(t)
	human := &v1beta1.Human{}
	human.Name = "reviewer"
	human.Namespace = "default"
	human.Spec = v1beta1.HumanSpec{
		DisplayName:       "Reviewer",
		Email:             "old@example.com",
		PermissionLevel:   2,
		AccessibleTeams:   []string{"alpha"},
		AccessibleWorkers: []string{"alpha-dev"},
		Note:              "old",
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(human).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")
	body := []byte(`{"email":"","permissionLevel":3,"accessibleTeams":[],"accessibleWorkers":["beta-dev"],"note":""}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/humans/reviewer", bytes.NewReader(body))
	req.SetPathValue("name", "reviewer")
	rec := httptest.NewRecorder()

	handler.UpdateHuman(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var updated v1beta1.Human
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "reviewer", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get human: %v", err)
	}
	if updated.Spec.PermissionLevel != 3 ||
		updated.Spec.Email != "" ||
		len(updated.Spec.AccessibleTeams) != 0 ||
		len(updated.Spec.AccessibleWorkers) != 1 ||
		updated.Spec.AccessibleWorkers[0] != "beta-dev" ||
		updated.Spec.Note != "" {
		t.Fatalf("unexpected updated human: %#v", updated.Spec)
	}
}

func TestCreateManagerRejectsWorkerRuntime(t *testing.T) {
	scheme := newServerTestScheme(t)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")
	body := []byte(
		`{"name":"m1","model":"qwen3.5-plus","runtime":"openclaw"}`,
	)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/managers",
		bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()

	handler.CreateManager(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusBadRequest,
			rec.Code,
			rec.Body.String(),
		)
	}
}

func TestUpdateManagerRejectsWorkerRuntime(t *testing.T) {
	scheme := newServerTestScheme(t)
	manager := &v1beta1.Manager{}
	manager.Name = "m1"
	manager.Namespace = "default"
	manager.Spec.Model = "qwen3.5-plus"
	manager.Spec.Runtime = backend.RuntimeAgentScope
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(manager).
		Build()
	handler := NewResourceHandler(k8sClient, "default", nil, "")
	body := []byte(`{"runtime":"copaw"}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/managers/m1",
		bytes.NewReader(body),
	)
	req.SetPathValue("name", "m1")
	rec := httptest.NewRecorder()

	handler.UpdateManager(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d: %s",
			http.StatusBadRequest,
			rec.Code,
			rec.Body.String(),
		)
	}
}

// TestCreate_EmptyControllerName_NoLabel verifies embedded-mode behavior:
// when controllerName is empty, the handler does not stamp any controller
// label (and does not introduce a stray labels map on resources that had
// none), preserving existing embedded deployments.
