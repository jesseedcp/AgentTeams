package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
)

// LegacyCompat handles backward-compatible operations that only apply in
// embedded mode: workers/teams/humans registry JSON files retained for
// migration, diagnostics, and older external consumers.
//
// AgentScope Manager does not consume these files; it reads Controller
// resources through the typed API. All compatibility state is persisted to
// OSS (MinIO).
//
// In incluster mode, construct with nil OSS — Enabled() will return false
// and all methods become no-ops.
type LegacyCompat struct {
	OSS          oss.StorageClient
	MatrixDomain string
	ManagerName  string // Manager agent name, default "manager"

	mu sync.Mutex // serializes read-modify-write cycles on registry files
}

// LegacyConfig holds configuration for constructing a LegacyCompat.
type LegacyConfig struct {
	OSS          oss.StorageClient
	MatrixDomain string
	ManagerName  string
}

func NewLegacyCompat(cfg LegacyConfig) *LegacyCompat {
	managerName := cfg.ManagerName
	if managerName == "" {
		managerName = "manager"
	}
	return &LegacyCompat{
		OSS:          cfg.OSS,
		MatrixDomain: cfg.MatrixDomain,
		ManagerName:  managerName,
	}
}

// Enabled reports whether legacy operations are configured.
func (l *LegacyCompat) Enabled() bool {
	return l != nil && l.OSS != nil
}

// MatrixUserID builds a full Matrix user ID from a localpart username.
func (l *LegacyCompat) MatrixUserID(name string) string {
	return fmt.Sprintf("@%s:%s", name, l.MatrixDomain)
}

func (l *LegacyCompat) managerAgentPrefix() string {
	return fmt.Sprintf("agents/%s", l.ManagerName)
}

// --- Workers Registry ---

// WorkerRegistryEntry describes a worker entry in workers-registry.json.
type WorkerRegistryEntry struct {
	Name            string   `json:"-"`
	MatrixUserID    string   `json:"matrix_user_id"`
	RoomID          string   `json:"room_id"`
	Runtime         string   `json:"runtime"`
	Deployment      string   `json:"deployment"`
	Skills          []string `json:"skills"`
	Role            string   `json:"role"`
	TeamID          *string  `json:"team_id"`
	Image           *string  `json:"image"`
	CreatedAt       string   `json:"created_at,omitempty"`
	SkillsUpdatedAt string   `json:"skills_updated_at"`
}

type workersRegistry struct {
	Version   int                            `json:"version"`
	UpdatedAt string                         `json:"updated_at"`
	Workers   map[string]WorkerRegistryEntry `json:"workers"`
}

// UpdateWorkersRegistry upserts a worker entry in workers-registry.json via OSS.
func (l *LegacyCompat) UpdateWorkersRegistry(entry WorkerRegistryEntry) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/workers-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &workersRegistry{Version: 1, Workers: make(map[string]WorkerRegistryEntry)}
	})
	if err != nil {
		return err
	}
	wr := reg.(*workersRegistry)

	now := time.Now().UTC().Format(time.RFC3339)
	existing, exists := wr.Workers[entry.Name]
	if exists && existing.CreatedAt != "" {
		entry.CreatedAt = existing.CreatedAt
	} else {
		entry.CreatedAt = now
	}
	entry.SkillsUpdatedAt = now
	wr.Workers[entry.Name] = entry
	wr.UpdatedAt = now

	return l.saveRegistry(ctx, key, wr)
}

// RemoveFromWorkersRegistry removes a worker entry from workers-registry.json via OSS.
func (l *LegacyCompat) RemoveFromWorkersRegistry(workerName string) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/workers-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &workersRegistry{Version: 1, Workers: make(map[string]WorkerRegistryEntry)}
	})
	if err != nil {
		return err
	}
	wr := reg.(*workersRegistry)

	delete(wr.Workers, workerName)
	wr.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return l.saveRegistry(ctx, key, wr)
}

// --- Teams Registry ---

// TeamRegistryEntry describes a team entry in teams-registry.json.
type TeamRegistryEntry struct {
	Name           string            `json:"-"`
	Leader         string            `json:"leader"`
	Workers        []string          `json:"workers"`
	TeamRoomID     string            `json:"team_room_id"`
	LeaderDMRoomID string            `json:"leader_dm_room_id,omitempty"`
	Admin          *TeamAdminEntry   `json:"admin,omitempty"`
	Members        []TeamMemberEntry `json:"members,omitempty"`
	CreatedAt      string            `json:"created_at,omitempty"`
}

type TeamAdminEntry struct {
	Name         string `json:"name"`
	MatrixUserID string `json:"matrix_user_id"`
}

type TeamMemberEntry struct {
	Name         string `json:"name"`
	MatrixUserID string `json:"matrix_user_id,omitempty"`
	Role         string `json:"role,omitempty"`
}

type teamsRegistry struct {
	Version   int                          `json:"version"`
	UpdatedAt string                       `json:"updated_at"`
	Teams     map[string]TeamRegistryEntry `json:"teams"`
}

// UpdateTeamsRegistry upserts a team entry in teams-registry.json via OSS.
func (l *LegacyCompat) UpdateTeamsRegistry(entry TeamRegistryEntry) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/teams-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &teamsRegistry{Version: 1, Teams: make(map[string]TeamRegistryEntry)}
	})
	if err != nil {
		return err
	}
	tr := reg.(*teamsRegistry)

	now := time.Now().UTC().Format(time.RFC3339)
	existing, exists := tr.Teams[entry.Name]
	if exists && existing.CreatedAt != "" {
		entry.CreatedAt = existing.CreatedAt
	} else {
		entry.CreatedAt = now
	}
	tr.Teams[entry.Name] = entry
	tr.UpdatedAt = now

	return l.saveRegistry(ctx, key, tr)
}

// RemoveFromTeamsRegistry removes a team from teams-registry.json via OSS.
func (l *LegacyCompat) RemoveFromTeamsRegistry(ctx context.Context, teamName string) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	key := l.managerAgentPrefix() + "/teams-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &teamsRegistry{Version: 1, Teams: make(map[string]TeamRegistryEntry)}
	})
	if err != nil {
		return err
	}
	tr := reg.(*teamsRegistry)

	delete(tr.Teams, teamName)
	tr.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return l.saveRegistry(ctx, key, tr)
}

// --- Humans Registry ---

// HumanRegistryEntry describes a human entry in humans-registry.json.
type HumanRegistryEntry struct {
	Name            string   `json:"-"`
	MatrixUserID    string   `json:"matrix_user_id"`
	DisplayName     string   `json:"display_name"`
	PermissionLevel int      `json:"permission_level"`
	AccessibleTeams []string `json:"accessible_teams,omitempty"`
	CreatedAt       string   `json:"created_at,omitempty"`
}

type humansRegistry struct {
	Version   int                           `json:"version"`
	UpdatedAt string                        `json:"updated_at"`
	Humans    map[string]HumanRegistryEntry `json:"humans"`
}

// UpdateHumansRegistry upserts a human entry in humans-registry.json via OSS.
func (l *LegacyCompat) UpdateHumansRegistry(entry HumanRegistryEntry) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/humans-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &humansRegistry{Version: 1, Humans: make(map[string]HumanRegistryEntry)}
	})
	if err != nil {
		return err
	}
	hr := reg.(*humansRegistry)

	now := time.Now().UTC().Format(time.RFC3339)
	existing, exists := hr.Humans[entry.Name]
	if exists && existing.CreatedAt != "" {
		entry.CreatedAt = existing.CreatedAt
	} else {
		entry.CreatedAt = now
	}
	hr.Humans[entry.Name] = entry
	hr.UpdatedAt = now

	return l.saveRegistry(ctx, key, hr)
}

// RemoveFromHumansRegistry removes a human from humans-registry.json via OSS.
func (l *LegacyCompat) RemoveFromHumansRegistry(ctx context.Context, humanName string) error {
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	key := l.managerAgentPrefix() + "/humans-registry.json"

	reg, err := l.loadRegistry(ctx, key, func() interface{} {
		return &humansRegistry{Version: 1, Humans: make(map[string]HumanRegistryEntry)}
	})
	if err != nil {
		return err
	}
	hr := reg.(*humansRegistry)

	delete(hr.Humans, humanName)
	hr.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return l.saveRegistry(ctx, key, hr)
}

// --- Generic OSS registry helpers ---

func (l *LegacyCompat) loadRegistry(ctx context.Context, key string, empty func() interface{}) (interface{}, error) {
	data, err := l.OSS.GetObject(ctx, key)
	if err != nil {
		if os.IsNotExist(err) {
			return empty(), nil
		}
		return nil, fmt.Errorf("read registry %s: %w", key, err)
	}

	result := empty()
	if err := json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("parse registry %s: %w", key, err)
	}
	return result, nil
}

func (l *LegacyCompat) saveRegistry(ctx context.Context, key string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal registry: %w", err)
	}
	return l.OSS.PutObject(ctx, key, data)
}
