package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ManagerConfigStore serializes embedded-mode updates to the Manager Agent's
// openclaw.json.
//
// All state is persisted to OSS (MinIO) so that the Manager Agent container
// can access it via mc mirror — this avoids cross-container filesystem issues.
//
// In incluster mode, construct with nil OSS — Enabled() will return false
// and all methods become no-ops.
type ManagerConfigStore struct {
	OSS          oss.StorageClient
	MatrixDomain string
	ManagerName  string // Manager agent name, default "manager"
	AgentFSDir   string // local filesystem root for agent workspaces (embedded: shared mount with manager)

	mu sync.Mutex // serializes openclaw.json read-modify-write cycles
}

// ManagerConfigStoreConfig holds configuration for constructing a ManagerConfigStore.
type ManagerConfigStoreConfig struct {
	OSS          oss.StorageClient
	MatrixDomain string
	ManagerName  string
	AgentFSDir   string
}

func NewManagerConfigStore(cfg ManagerConfigStoreConfig) *ManagerConfigStore {
	// 逻辑说明：NewManagerConfigStore 接收 cfg(ManagerConfigStoreConfig)，按本函数中的条件与转换步骤处理运行时配置的期望结果。
	// 返回/状态：返回 *ManagerConfigStore；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	managerName := cfg.ManagerName
	if managerName == "" {
		managerName = "manager"
	}
	return &ManagerConfigStore{
		OSS:          cfg.OSS,
		MatrixDomain: cfg.MatrixDomain,
		ManagerName:  managerName,
		AgentFSDir:   cfg.AgentFSDir,
	}
}

// Enabled reports whether Manager config storage is configured.
func (l *ManagerConfigStore) Enabled() bool {
	return l != nil && l.OSS != nil
}

// MatrixUserID builds a full Matrix user ID from a localpart username.
func (l *ManagerConfigStore) MatrixUserID(name string) string {
	return fmt.Sprintf("@%s:%s", name, l.MatrixDomain)
}

func (l *ManagerConfigStore) managerAgentPrefix() string {
	return fmt.Sprintf("agents/%s", l.ManagerName)
}

// managerLocalConfigPath returns the local filesystem path for the manager's openclaw.json.
// In embedded mode, this is a shared mount with the manager container.
func (l *ManagerConfigStore) managerLocalConfigPath() string {
	// 逻辑说明：managerLocalConfigPath 接收 无，按本函数中的条件与转换步骤处理运行时配置的期望结果。
	// 返回/状态：返回 string；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if l.AgentFSDir == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/openclaw.json", l.AgentFSDir, l.ManagerName)
}

// writeManagerLocalConfig writes openclaw.json to the local filesystem (shared mount).
// This ensures the manager container sees changes immediately without MinIO sync.
func (l *ManagerConfigStore) writeManagerLocalConfig(data []byte) {
	// 逻辑说明：writeManagerLocalConfig 接收 data([]byte)，依次借助 managerLocalConfigPath、WriteFile、Printf写入运行时配置的期望结果。
	// 返回/状态：返回 无；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	path := l.managerLocalConfigPath()
	if path == "" {
		return
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		// Non-fatal: MinIO is the source of truth
		fmt.Printf("warning: failed to write manager local config %s: %v\n", path, err)
	}
}

// --- Manager Config ---

// PutManagerConfig writes the Manager's openclaw.json to OSS, merging the
// new config with any existing groupAllowFrom entries to avoid overwriting
// additions made by UpdateManagerGroupAllowFrom (e.g. team leader IDs).
func (l *ManagerConfigStore) PutManagerConfig(configJSON []byte) error {
	// 逻辑说明：PutManagerConfig 接收 configJSON([]byte)，依次借助 Enabled、Lock、Unlock、Background处理运行时配置的期望结果。
	// 返回/状态：返回 error；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/openclaw.json"

	// Read existing config to preserve user customizations on top of the
	// generated defaults: groupAllowFrom additions plus user-modified plugin
	// entries (e.g. memory-core dreaming schedule, extra load paths).
	existingData, err := l.OSS.GetObject(ctx, key)
	if err == nil && len(existingData) > 0 {
		var existingCfg map[string]interface{}
		var newCfg map[string]interface{}
		if json.Unmarshal(existingData, &existingCfg) == nil && json.Unmarshal(configJSON, &newCfg) == nil {
			mergeGroupAllowFrom(existingCfg, newCfg)
			if merged, mErr := json.MarshalIndent(newCfg, "", "  "); mErr == nil {
				configJSON = merged
			}
		}
		if pluginMerged, pErr := mergeUserPluginConfig(configJSON, existingData); pErr != nil {
			log.Log.WithName("manager-config").Error(pErr, "plugin config merge failed, using generated config", "key", key)
		} else {
			configJSON = pluginMerged
		}
	}

	if err := l.OSS.PutObject(ctx, key, configJSON); err != nil {
		return err
	}
	l.writeManagerLocalConfig(configJSON)
	return nil
}

// mergeGroupAllowFrom copies any extra groupAllowFrom entries from old config
// into new config, preserving IDs added by UpdateManagerGroupAllowFrom.
func mergeGroupAllowFrom(oldCfg, newCfg map[string]interface{}) {
	// 逻辑说明：mergeGroupAllowFrom 接收 oldCfg/newCfg(map[string]interface{})，依次借助 extractStringSlice合并运行时配置的期望结果。
	// 返回/状态：返回 无；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	oldChannels, _ := oldCfg["channels"].(map[string]interface{})
	newChannels, _ := newCfg["channels"].(map[string]interface{})
	if oldChannels == nil || newChannels == nil {
		return
	}
	oldMatrix, _ := oldChannels["matrix"].(map[string]interface{})
	newMatrix, _ := newChannels["matrix"].(map[string]interface{})
	if oldMatrix == nil || newMatrix == nil {
		return
	}

	oldAllow := extractStringSlice(oldMatrix["groupAllowFrom"])
	newAllow := extractStringSlice(newMatrix["groupAllowFrom"])

	// Add any entries from old that are missing in new
	for _, id := range oldAllow {
		found := false
		for _, nid := range newAllow {
			if nid == id {
				found = true
				break
			}
		}
		if !found {
			newAllow = append(newAllow, id)
		}
	}
	newMatrix["groupAllowFrom"] = newAllow
}

// UpdateManagerGroupAllowFrom adds or removes a worker Matrix ID from the
// Manager's openclaw.json groupAllowFrom list via OSS.
func (l *ManagerConfigStore) UpdateManagerGroupAllowFrom(workerMatrixID string, add bool) error {
	// 逻辑说明：UpdateManagerGroupAllowFrom 接收 workerMatrixID(string)、add(bool)，依次借助 Enabled、Lock、Unlock、Background更新运行时配置的期望结果。
	// 返回/状态：返回 error；会更新 运行时配置的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if !l.Enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ctx := context.Background()
	key := l.managerAgentPrefix() + "/openclaw.json"

	data, err := l.OSS.GetObject(ctx, key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read manager config from OSS: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("parse manager config: %w", err)
	}

	channels, _ := config["channels"].(map[string]interface{})
	if channels == nil {
		return nil
	}
	matrixCfg, _ := channels["matrix"].(map[string]interface{})
	if matrixCfg == nil {
		return nil
	}

	allowList := extractStringSlice(matrixCfg["groupAllowFrom"])

	if add {
		for _, id := range allowList {
			if id == workerMatrixID {
				return nil
			}
		}
		allowList = append(allowList, workerMatrixID)
	} else {
		filtered := make([]string, 0, len(allowList))
		for _, id := range allowList {
			if id != workerMatrixID {
				filtered = append(filtered, id)
			}
		}
		allowList = filtered
	}

	matrixCfg["groupAllowFrom"] = allowList

	out, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manager config: %w", err)
	}
	if err := l.OSS.PutObject(ctx, key, out); err != nil {
		return err
	}
	l.writeManagerLocalConfig(out)
	return nil
}

func extractStringSlice(v interface{}) []string {
	// 逻辑说明：extractStringSlice 接收 v(interface{})，按本函数中的条件与转换步骤提取运行时配置的期望结果。
	// 返回/状态：返回 []string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if v == nil {
		return nil
	}
	switch arr := v.(type) {
	case []interface{}:
		result := make([]string, 0, len(arr))
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return arr
	}
	return nil
}
