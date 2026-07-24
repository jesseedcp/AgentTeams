package backend

import "testing"

func TestResolveRuntime(t *testing.T) {
	cases := []struct {
		name       string
		reqRuntime string
		fallback   string
		want       string
	}{
		{"explicit_request_wins_over_fallback", RuntimeCopaw, RuntimeHermes, RuntimeCopaw},
		{"explicit_over_empty_fallback", RuntimeOpenClaw, "", RuntimeOpenClaw},
		{"empty_uses_fallback_hermes", "", RuntimeHermes, RuntimeHermes},
		{"empty_uses_fallback_copaw", "", RuntimeCopaw, RuntimeCopaw},
		{"empty_uses_fallback_qwenpaw", "", RuntimeQwenPaw, RuntimeQwenPaw},
		{"empty_and_no_fallback_uses_openclaw", "", "", RuntimeOpenClaw},
		{"explicit_openclaw_preserved", RuntimeOpenClaw, RuntimeHermes, RuntimeOpenClaw},
		{"explicit_hermes_preserved", RuntimeHermes, RuntimeCopaw, RuntimeHermes},
		{"explicit_qwenpaw_preserved", RuntimeQwenPaw, RuntimeCopaw, RuntimeQwenPaw},
		{"explicit_openhuman_preserved", RuntimeOpenHuman, RuntimeCopaw, RuntimeOpenHuman},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveRuntime(tc.reqRuntime, tc.fallback)
			if got != tc.want {
				t.Fatalf("ResolveRuntime(%q, %q) = %q, want %q", tc.reqRuntime, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestValidRuntime(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{RuntimeOpenClaw, true},
		{RuntimeCopaw, true},
		{RuntimeHermes, true},
		{RuntimeQwenPaw, true},
		{RuntimeOpenHuman, true},
		{"unknown", false},
	}
	for _, tc := range cases {
		if got := ValidRuntime(tc.in); got != tc.want {
			t.Fatalf("ValidRuntime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestRuntimeSetsAreSeparated(t *testing.T) {
	if !ValidManagerRuntime(RuntimeAgentScope) {
		t.Fatal("agentscope must be valid for Manager")
	}
	if ValidManagerRuntime(RuntimeOpenClaw) ||
		ValidManagerRuntime(RuntimeCopaw) {
		t.Fatal("legacy runtimes must be invalid for Manager")
	}

	for _, runtime := range []string{
		RuntimeOpenClaw,
		RuntimeCopaw,
		RuntimeHermes,
		RuntimeQwenPaw,
		RuntimeOpenHuman,
	} {
		if !ValidRuntime(runtime) {
			t.Fatalf("%s must remain valid for Worker", runtime)
		}
	}
	if ValidRuntime(RuntimeAgentScope) {
		t.Fatal("agentscope is not a Worker runtime")
	}
}

func TestResolveManagerRuntime(t *testing.T) {
	if got := ResolveManagerRuntime(""); got != RuntimeAgentScope {
		t.Fatalf("ResolveManagerRuntime(\"\") = %q, want %q", got, RuntimeAgentScope)
	}
	if got := ResolveManagerRuntime(RuntimeAgentScope); got != RuntimeAgentScope {
		t.Fatalf(
			"ResolveManagerRuntime(%q) = %q, want %q",
			RuntimeAgentScope,
			got,
			RuntimeAgentScope,
		)
	}
}
