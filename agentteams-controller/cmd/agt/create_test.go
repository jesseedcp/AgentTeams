package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultWorkerModel(t *testing.T) {
	t.Run("falls back to qwen3.6-plus when env var unset", func(t *testing.T) {
		t.Setenv("AGENTTEAMS_DEFAULT_MODEL", "")
		if got := defaultWorkerModel(); got != "qwen3.6-plus" {
			t.Fatalf("defaultWorkerModel() = %q, want qwen3.6-plus", got)
		}
	})
	t.Run("prefers AGENTTEAMS_DEFAULT_MODEL when set", func(t *testing.T) {
		t.Setenv("AGENTTEAMS_DEFAULT_MODEL", "claude-sonnet-4-6")
		if got := defaultWorkerModel(); got != "claude-sonnet-4-6" {
			t.Fatalf("defaultWorkerModel() = %q, want claude-sonnet-4-6", got)
		}
	})
	t.Run("trims whitespace before falling back", func(t *testing.T) {
		t.Setenv("AGENTTEAMS_DEFAULT_MODEL", "   ")
		if got := defaultWorkerModel(); got != "qwen3.6-plus" {
			t.Fatalf("defaultWorkerModel() = %q, want qwen3.6-plus", got)
		}
	})
}

func TestWaitForWorkerReady(t *testing.T) {
	var calls int32
	client := &APIClient{
		BaseURL: "http://controller.test",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/v1/workers/alice/status" {
					return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
				}
				call := atomic.AddInt32(&calls, 1)
				if call < 3 {
					return jsonResponse(http.StatusOK, `{"name":"alice","phase":"Running","containerState":"running"}`), nil
				}
				return jsonResponse(http.StatusOK, `{"name":"alice","phase":"Ready","containerState":"running"}`), nil
			}),
			Timeout: 5 * time.Second,
		},
	}

	resp, err := waitForWorkerReady(client, "alice", 5*time.Second)
	if err != nil {
		t.Fatalf("waitForWorkerReady returned error: %v", err)
	}
	if resp.Phase != "Ready" {
		t.Fatalf("expected Ready phase, got %q", resp.Phase)
	}
	if atomic.LoadInt32(&calls) < 3 {
		t.Fatalf("expected multiple polls, got %d", calls)
	}
}

func TestWaitForWorkerReadyTimeout(t *testing.T) {
	client := &APIClient{
		BaseURL: "http://controller.test",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusOK, `{"name":"alice","phase":"Running","containerState":"running","message":"booting"}`), nil
			}),
			Timeout: 5 * time.Second,
		},
	}

	_, err := waitForWorkerReady(client, "alice", 1500*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "did not become ready") {
		t.Fatalf("expected timeout error, got %q", msg)
	}
	if !strings.Contains(msg, "phase=Running") {
		t.Fatalf("expected last phase in error, got %q", msg)
	}
}

func TestCreateTeamPostsStandaloneWorkerReferences(t *testing.T) {
	var got map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/teams" {
			t.Fatalf("request = %s %s, want POST /api/v1/teams", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("AGENTTEAMS_CONTROLLER_URL", server.URL)
	t.Setenv("AGENTTEAMS_AUTH_TOKEN", "")
	t.Setenv("AGENTTEAMS_AUTH_TOKEN_FILE", "")

	cmd := createTeamCmd()
	cmd.SetArgs([]string{
		"--name", "alpha",
		"--leader-name", "alpha-lead",
		"--workers", "alice,bob",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("create team: %v", err)
	}

	want := []interface{}{
		map[string]interface{}{"name": "alpha-lead", "role": "team_leader"},
		map[string]interface{}{"name": "alice", "role": "worker"},
		map[string]interface{}{"name": "bob", "role": "worker"},
	}
	if !reflect.DeepEqual(got["workerMembers"], want) {
		t.Fatalf("workerMembers = %#v, want %#v", got["workerMembers"], want)
	}
	if _, exists := got["leader"]; exists {
		t.Fatalf("legacy leader payload must be absent: %#v", got)
	}
	if _, exists := got["workers"]; exists {
		t.Fatalf("legacy workers payload must be absent: %#v", got)
	}
}

func TestCreateWorkerRejectsLegacyTeamOwnershipFlags(t *testing.T) {
	for _, flag := range []string{"--team", "--role"} {
		t.Run(flag, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{}`)
			}))
			t.Cleanup(server.Close)
			t.Setenv("AGENTTEAMS_CONTROLLER_URL", server.URL)
			t.Setenv("AGENTTEAMS_AUTH_TOKEN", "")
			t.Setenv("AGENTTEAMS_AUTH_TOKEN_FILE", "")

			cmd := createWorkerCmd()
			cmd.SetArgs([]string{"--name", "alice", "--no-wait", flag, "legacy"})
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "unknown flag") {
				t.Fatalf("error = %v, want unknown flag rejection for %s", err, flag)
			}
		})
	}
}

func TestCreateWorkerConsoleFlagsSendDesiredState(t *testing.T) {
	var got map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/workers" {
			t.Fatalf("request = %s %s, want POST /api/v1/workers", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("AGENTTEAMS_CONTROLLER_URL", server.URL)
	t.Setenv("AGENTTEAMS_AUTH_TOKEN", "")
	t.Setenv("AGENTTEAMS_AUTH_TOKEN_FILE", "")

	cmd := createWorkerCmd()
	cmd.SetArgs([]string{
		"--name", "alice",
		"--runtime", "copaw",
		"--console",
		"--console-port", "9090",
		"--no-wait",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	console, ok := got["console"].(map[string]interface{})
	if !ok || console["enabled"] != true || console["port"] != float64(9090) {
		t.Fatalf("console payload = %#v", got["console"])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}
