package initializer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

type recordingMCPBootstrapper struct {
	request gateway.RESTMCPServerRequest
}

func (f *recordingMCPBootstrapper) EnsureRESTMCPServer(
	_ context.Context,
	request gateway.RESTMCPServerRequest,
) (gateway.MCPServerEndpoint, error) {
	f.request = request
	return gateway.MCPServerEndpoint{
		Name:      request.Name,
		URL:       "http://aigw.example.com:8080/mcp-servers/mcp-github/mcp",
		Transport: "http",
	}, nil
}

func TestBootstrapGitHubMCPRendersOneCredentialSlot(t *testing.T) {
	skillsDir := t.TempDir()
	templateDir := filepath.Join(
		skillsDir,
		"mcp-server-management",
		"references",
	)
	if err := os.MkdirAll(templateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	template := "server:\n  accessToken: \"\"\n"
	if err := os.WriteFile(
		filepath.Join(templateDir, "mcp-github.yaml"),
		[]byte(template),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	recorder := &recordingMCPBootstrapper{}
	initializer := &Initializer{
		MCP: recorder,
		Config: Config{
			GitHubToken: "ghp_quote\"safe",
			SkillsDir:   skillsDir,
		},
	}

	endpoint, err := initializer.bootstrapGitHubMCP(context.Background())
	if err != nil {
		t.Fatalf("bootstrapGitHubMCP: %v", err)
	}
	if endpoint.Name != "github" {
		t.Fatalf("endpoint name = %q", endpoint.Name)
	}
	raw := recorder.request.RawConfiguration
	if strings.Contains(raw, `accessToken: ""`) {
		t.Fatal("credential slot was not replaced")
	}
	if !strings.Contains(raw, `accessToken: "ghp_quote\"safe"`) {
		t.Fatalf("credential was not safely quoted: %s", raw)
	}
	if recorder.request.ServiceDomain != "api.github.com" ||
		recorder.request.ServicePort != 443 {
		t.Fatalf("request = %#v", recorder.request)
	}
}

func TestManagerMCPServersPreserveExistingAndUpsertGitHub(t *testing.T) {
	existing := []interface{}{
		map[string]interface{}{
			"name":      "jira",
			"url":       "https://gateway.example.com/mcp/jira",
			"transport": "http",
		},
		map[string]interface{}{
			"name":      "github",
			"url":       "https://stale.example.com/mcp",
			"transport": "sse",
		},
	}
	endpoint := gateway.MCPServerEndpoint{
		Name:      "github",
		URL:       "http://aigw.example.com:8080/mcp-servers/mcp-github/mcp",
		Transport: "http",
	}

	merged := mergeManagerMCPServers(existing, []v1beta1.MCPServer{{
		Name:      endpoint.Name,
		URL:       endpoint.URL,
		Transport: endpoint.Transport,
	}})
	if len(merged) != 2 {
		t.Fatalf("merged MCP servers = %#v", merged)
	}
	byName := map[string]map[string]interface{}{}
	for _, raw := range merged {
		item, ok := raw.(map[string]interface{})
		if !ok {
			t.Fatalf("MCP entry = %#v", raw)
		}
		byName[item["name"].(string)] = item
	}
	if byName["jira"]["url"] != "https://gateway.example.com/mcp/jira" {
		t.Fatalf("jira entry = %#v", byName["jira"])
	}
	if byName["github"]["url"] != endpoint.URL ||
		byName["github"]["transport"] != "http" {
		t.Fatalf("github entry = %#v", byName["github"])
	}
}

func TestEnsureManagerCRUpdatesExistingMCPStateIdempotently(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    v1beta1.GroupName,
		Version:  v1beta1.Version,
		Resource: "managers",
	}
	current := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": v1beta1.GroupName + "/" + v1beta1.Version,
		"kind":       "Manager",
		"metadata": map[string]interface{}{
			"name":      "default",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"model":   "qwen",
			"runtime": "agentscope",
			"mcpServers": []interface{}{
				map[string]interface{}{
					"name":      "jira",
					"url":       "https://gateway.example.com/mcp/jira",
					"transport": "http",
				},
			},
		},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "ManagerList"},
		current,
	)
	initializer := &Initializer{
		Dynamic: client,
		Config: Config{
			Namespace:      "default",
			ManagerEnabled: true,
		},
	}
	desired := []v1beta1.MCPServer{{
		Name:      "github",
		URL:       "http://aigw.example.com:8080/mcp-servers/mcp-github/mcp",
		Transport: "http",
	}}

	if err := initializer.ensureManagerCR(
		context.Background(),
		desired,
	); err != nil {
		t.Fatalf("ensureManagerCR first call: %v", err)
	}
	if err := initializer.ensureManagerCR(
		context.Background(),
		desired,
	); err != nil {
		t.Fatalf("ensureManagerCR second call: %v", err)
	}
	observed, err := client.Resource(gvr).Namespace("default").Get(
		context.Background(),
		"default",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	servers, found, err := unstructured.NestedSlice(
		observed.Object,
		"spec",
		"mcpServers",
	)
	if err != nil || !found {
		t.Fatalf("Manager MCP state: found=%v err=%v", found, err)
	}
	if len(servers) != 2 {
		t.Fatalf("Manager MCP state = %#v", servers)
	}
	counts := map[string]int{}
	for _, raw := range servers {
		item := raw.(map[string]interface{})
		counts[item["name"].(string)]++
	}
	if counts["jira"] != 1 || counts["github"] != 1 {
		t.Fatalf("Manager MCP counts = %#v", counts)
	}
}

func TestEnsureManagerCRUpdatesExistingCodingCLIState(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    v1beta1.GroupName,
		Version:  v1beta1.Version,
		Resource: "managers",
	}
	current := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": v1beta1.GroupName + "/" + v1beta1.Version,
		"kind":       "Manager",
		"metadata": map[string]interface{}{
			"name":      "default",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"model":   "qwen",
			"runtime": "agentscope",
			"codingCLI": map[string]interface{}{
				"enabled":   false,
				"providers": []interface{}{},
			},
		},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "ManagerList"},
		current,
	)
	initializer := &Initializer{
		Dynamic: client,
		Config: Config{
			Namespace:      "default",
			ManagerEnabled: true,
			ManagerCodingCLI: &v1beta1.ManagerCodingCLISpec{
				Enabled:          true,
				Providers:        []string{"claude"},
				HostPath:         "/srv/coding-cli",
				MountPath:        "/opt/coding-cli",
				TrustedDirectory: "/opt/coding-cli/bin",
			},
		},
	}

	if err := initializer.ensureManagerCR(context.Background(), nil); err != nil {
		t.Fatalf("ensureManagerCR: %v", err)
	}
	observed, err := client.Resource(gvr).Namespace("default").Get(
		context.Background(),
		"default",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	codingCLI, found, err := unstructured.NestedMap(
		observed.Object,
		"spec",
		"codingCLI",
	)
	if err != nil || !found {
		t.Fatalf("Manager coding CLI state: found=%v err=%v", found, err)
	}
	if codingCLI["enabled"] != true {
		t.Fatalf("Manager coding CLI state = %#v", codingCLI)
	}
	providers, ok := codingCLI["providers"].([]interface{})
	if !ok || len(providers) != 1 || providers[0] != "claude" {
		t.Fatalf("Manager coding CLI providers = %#v", providers)
	}
}
