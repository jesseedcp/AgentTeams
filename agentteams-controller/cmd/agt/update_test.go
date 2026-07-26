package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestUpdateTeamPostsStandaloneWorkerReferences(t *testing.T) {
	var got map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/teams/alpha" {
			t.Fatalf("request = %s %s, want PUT /api/v1/teams/alpha", r.Method, r.URL.Path)
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

	cmd := updateTeamCmd()
	cmd.SetArgs([]string{
		"--name", "alpha",
		"--leader-name", "alpha-lead",
		"--workers", "alice,bob",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("update team: %v", err)
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
}

func TestUpdateTeamRejectsLegacyEmbeddedRuntimeFlags(t *testing.T) {
	for _, flag := range []string{"--leader-model", "--worker-idle-timeout"} {
		t.Run(flag, func(t *testing.T) {
			cmd := updateTeamCmd()
			cmd.SetArgs([]string{"--name", "alpha", flag, "legacy"})
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "unknown flag") {
				t.Fatalf("error = %v, want unknown flag rejection for %s", err, flag)
			}
		})
	}
}
