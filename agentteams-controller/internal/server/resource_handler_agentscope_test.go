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
		backend.RuntimeOpenHuman,
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
