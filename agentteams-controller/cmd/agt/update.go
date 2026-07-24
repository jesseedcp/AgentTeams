package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

const maxMCPServersDocumentBytes = 1 << 20

var mcpServerNamePattern = regexp.MustCompile(
	`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`,
)

type mcpServerSpec struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Transport string `json:"transport,omitempty"`
}

func updateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update a resource",
	}
	cmd.AddCommand(updateWorkerCmd())
	cmd.AddCommand(updateTeamCmd())
	cmd.AddCommand(updateHumanCmd())
	cmd.AddCommand(updateManagerCmd())
	return cmd
}

// ---------------------------------------------------------------------------
// update worker
// ---------------------------------------------------------------------------

func updateWorkerCmd() *cobra.Command {
	var (
		name       string
		model      string
		runtime    string
		image      string
		identity   string
		soul       string
		skills     string
		packageURI string
		expose     string
		mcpFile    string
	)

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Update a Worker",
		Long: `Update an existing Worker resource. Only specified fields are changed.

  agt update worker --name alice --model claude-sonnet-4-6
  agt update worker --name alice --image agentteams/agentteams-worker:v1.2.0
  agt update worker --name alice --skills github-operations,code-review
  To update CPU/memory resources, use a YAML manifest and pass it with 'agt apply -f worker.yaml'.
  agt update worker --name alice --mcp-servers-file mcp-servers.json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}

			if packageURI != "" {
				var err error
				packageURI, err = expandPackageURI(packageURI)
				if err != nil {
					return err
				}
			}

			req := map[string]interface{}{}
			if cmd.Flags().Changed("model") {
				req["model"] = model
			}
			if cmd.Flags().Changed("runtime") {
				req["runtime"] = runtime
			}
			if cmd.Flags().Changed("image") {
				req["image"] = image
			}
			if cmd.Flags().Changed("identity") {
				req["identity"] = identity
			}
			if cmd.Flags().Changed("soul") {
				req["soul"] = soul
			}
			if cmd.Flags().Changed("package") {
				req["package"] = packageURI
			}
			if cmd.Flags().Changed("skills") {
				req["skills"] = splitCSV(skills)
			}
			if cmd.Flags().Changed("expose") {
				req["expose"] = parseExposePorts(expose)
			}
			if cmd.Flags().Changed("mcp-servers-file") {
				servers, err := readMCPServers(cmd, mcpFile)
				if err != nil {
					return err
				}
				req["mcpServers"] = servers
			}

			if len(req) == 0 {
				return fmt.Errorf("at least one field must be specified for update")
			}

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("PUT", "/api/v1/workers/"+name, req, &resp); err != nil {
				return fmt.Errorf("update worker: %w", err)
			}
			fmt.Printf("worker/%s configured\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Worker name (required)")
	cmd.Flags().StringVar(&model, "model", "", "LLM model ID")
	cmd.Flags().StringVar(&runtime, "runtime", "", "Agent runtime (openclaw|copaw|hermes|qwenpaw|openhuman)")
	cmd.Flags().StringVar(&image, "image", "", "Container image override")
	cmd.Flags().StringVar(&identity, "identity", "", "Worker identity description")
	cmd.Flags().StringVar(&soul, "soul", "", "Worker SOUL.md content")
	cmd.Flags().StringVar(&skills, "skills", "", "Comma-separated built-in skills")
	cmd.Flags().StringVar(&packageURI, "package", "", "Package URI")
	cmd.Flags().StringVar(&expose, "expose", "", "Comma-separated ports to expose")
	cmd.Flags().StringVar(
		&mcpFile,
		"mcp-servers-file",
		"",
		"JSON MCP server array path, or - for stdin (replaces all)",
	)
	return cmd
}

// ---------------------------------------------------------------------------
// update human
// ---------------------------------------------------------------------------

func updateHumanCmd() *cobra.Command {
	var (
		name              string
		displayName       string
		email             string
		permissionLevel   int
		accessibleTeams   string
		accessibleWorkers string
		note              string
	)

	cmd := &cobra.Command{
		Use:   "human",
		Short: "Update a Human permission scope",
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			req := map[string]interface{}{}
			if cmd.Flags().Changed("display-name") {
				req["displayName"] = displayName
			}
			if cmd.Flags().Changed("email") {
				req["email"] = email
			}
			if cmd.Flags().Changed("permission-level") {
				if permissionLevel < 1 || permissionLevel > 3 {
					return fmt.Errorf("--permission-level must be between 1 and 3")
				}
				req["permissionLevel"] = permissionLevel
			}
			if cmd.Flags().Changed("accessible-teams") {
				req["accessibleTeams"] = splitCSV(accessibleTeams)
			}
			if cmd.Flags().Changed("accessible-workers") {
				req["accessibleWorkers"] = splitCSV(accessibleWorkers)
			}
			if cmd.Flags().Changed("note") {
				req["note"] = note
			}
			if len(req) == 0 {
				return fmt.Errorf("at least one field must be specified for update")
			}
			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("PUT", "/api/v1/humans/"+name, req, &resp); err != nil {
				return fmt.Errorf("update human: %w", err)
			}
			fmt.Printf("human/%s configured\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Human resource name (required)")
	cmd.Flags().StringVar(&displayName, "display-name", "", "Matrix display name")
	cmd.Flags().StringVar(&email, "email", "", "Email address (empty clears it)")
	cmd.Flags().IntVar(&permissionLevel, "permission-level", 0, "Permission level (1, 2, or 3)")
	cmd.Flags().StringVar(&accessibleTeams, "accessible-teams", "", "Comma-separated Team names (empty clears them)")
	cmd.Flags().StringVar(&accessibleWorkers, "accessible-workers", "", "Comma-separated Worker names (empty clears them)")
	cmd.Flags().StringVar(&note, "note", "", "Administrative note (empty clears it)")
	return cmd
}

// ---------------------------------------------------------------------------
// update team
// ---------------------------------------------------------------------------

func updateTeamCmd() *cobra.Command {
	var (
		name                 string
		teamName             string
		description          string
		leaderModel          string
		leaderHeartbeatEvery string
		workerIdleTimeout    string
	)

	cmd := &cobra.Command{
		Use:   "team",
		Short: "Update a Team",
		Long: `Update an existing Team resource. Only specified fields are changed.

  agt update team --name alpha --description "Updated description"
  agt update team --name alpha --leader-model claude-sonnet-4-6
  agt update team --name alpha --leader-heartbeat-every 30m --worker-idle-timeout 12h
  To update per-member CPU/memory resources, use a YAML manifest and pass it with 'agt apply -f team.yaml'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}

			req := map[string]interface{}{}
			setIfNotEmpty(req, "teamName", teamName)
			setIfNotEmpty(req, "description", description)
			leader := map[string]interface{}{}
			setIfNotEmpty(leader, "model", leaderModel)
			if leaderHeartbeatEvery != "" {
				leader["heartbeat"] = map[string]interface{}{
					"enabled": true,
					"every":   leaderHeartbeatEvery,
				}
			}
			setIfNotEmpty(leader, "workerIdleTimeout", workerIdleTimeout)
			if len(leader) > 0 {
				req["leader"] = leader
			}

			if len(req) == 0 {
				return fmt.Errorf("at least one field must be specified for update")
			}

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("PUT", "/api/v1/teams/"+name, req, &resp); err != nil {
				return fmt.Errorf("update team: %w", err)
			}
			fmt.Printf("team/%s configured\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Team name (required)")
	cmd.Flags().StringVar(&teamName, "team-name", "", "Runtime/storage team name")
	cmd.Flags().StringVar(&description, "description", "", "Team description")
	cmd.Flags().StringVar(&leaderModel, "leader-model", "", "Leader LLM model")
	cmd.Flags().StringVar(&leaderHeartbeatEvery, "leader-heartbeat-every", "", "Leader heartbeat interval (e.g. 30m)")
	cmd.Flags().StringVar(&workerIdleTimeout, "worker-idle-timeout", "", "Idle timeout before the leader may sleep workers (e.g. 12h)")
	return cmd
}

// ---------------------------------------------------------------------------
// update manager
// ---------------------------------------------------------------------------

func updateManagerCmd() *cobra.Command {
	var (
		name    string
		model   string
		runtime string
		image   string
		soul    string
		mcpFile string
	)

	cmd := &cobra.Command{
		Use:   "manager",
		Short: "Update a Manager",
		Long: `Update an existing Manager resource. Only specified fields are changed.

  agt update manager --name default --model claude-sonnet-4-6
  agt update manager --name default --image agentteams/agentteams-manager:v1.2.0
  agt update manager --name default --mcp-servers-file mcp-servers.json
  To update CPU/memory resources, use a YAML manifest and pass it with 'agt apply -f manager.yaml'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}

			req := map[string]interface{}{}
			setIfNotEmpty(req, "model", model)
			setIfNotEmpty(req, "runtime", runtime)
			setIfNotEmpty(req, "image", image)
			setIfNotEmpty(req, "soul", soul)
			if cmd.Flags().Changed("mcp-servers-file") {
				servers, err := readMCPServers(cmd, mcpFile)
				if err != nil {
					return err
				}
				req["mcpServers"] = servers
			}

			if len(req) == 0 {
				return fmt.Errorf("at least one field must be specified for update")
			}

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("PUT", "/api/v1/managers/"+name, req, &resp); err != nil {
				return fmt.Errorf("update manager: %w", err)
			}
			fmt.Printf("manager/%s configured\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Manager name (required)")
	cmd.Flags().StringVar(&model, "model", "", "LLM model ID")
	cmd.Flags().StringVar(&runtime, "runtime", "", "Agent runtime (openclaw|copaw|hermes|qwenpaw|openhuman)")
	cmd.Flags().StringVar(&image, "image", "", "Container image override")
	cmd.Flags().StringVar(&soul, "soul", "", "Manager SOUL.md content")
	cmd.Flags().StringVar(
		&mcpFile,
		"mcp-servers-file",
		"",
		"JSON MCP server array path, or - for stdin (replaces all)",
	)
	return cmd
}

func readMCPServers(
	cmd *cobra.Command,
	path string,
) ([]mcpServerSpec, error) {
	var (
		reader io.Reader
		file   *os.File
		err    error
	)
	if path == "-" {
		reader = cmd.InOrStdin()
	} else {
		file, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf(
				"read --mcp-servers-file %q: %w",
				path,
				err,
			)
		}
		defer file.Close()
		reader = file
	}

	data, err := io.ReadAll(
		io.LimitReader(reader, maxMCPServersDocumentBytes+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read MCP server document: %w", err)
	}
	if len(data) > maxMCPServersDocumentBytes {
		return nil, fmt.Errorf("MCP server document exceeds 1 MiB")
	}

	var servers []mcpServerSpec
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&servers); err != nil {
		return nil, fmt.Errorf("decode MCP server array: %w", err)
	}
	if servers == nil {
		return nil, fmt.Errorf("MCP server document must be an array")
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf(
				"MCP server document contains trailing JSON",
			)
		}
		return nil, fmt.Errorf(
			"decode trailing MCP server data: %w",
			err,
		)
	}

	names := make(map[string]struct{}, len(servers))
	for i := range servers {
		server := &servers[i]
		if !mcpServerNamePattern.MatchString(server.Name) {
			return nil, fmt.Errorf(
				"MCP server name %q must be a DNS label",
				server.Name,
			)
		}
		if _, exists := names[server.Name]; exists {
			return nil, fmt.Errorf(
				"MCP server names must be unique: %q",
				server.Name,
			)
		}
		names[server.Name] = struct{}{}
		if server.Transport == "" {
			server.Transport = "http"
		}
		if server.Transport != "http" && server.Transport != "sse" {
			return nil, fmt.Errorf(
				"MCP server %q transport must be http or sse",
				server.Name,
			)
		}
		parsed, err := url.ParseRequestURI(server.URL)
		if err != nil ||
			(parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" ||
			parsed.User != nil {
			return nil, fmt.Errorf(
				"MCP server %q URL must be an http(s) URL "+
					"without embedded credentials",
				server.Name,
			)
		}
		if strings.ContainsAny(server.URL, "\r\n") {
			return nil, fmt.Errorf(
				"MCP server %q URL contains invalid characters",
				server.Name,
			)
		}
	}
	return servers, nil
}
