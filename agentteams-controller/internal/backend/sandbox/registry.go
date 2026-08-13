package sandbox

import "fmt"

// PluginRegistry manages sandbox plugin registrations keyed by type name.
type PluginRegistry struct {
	plugins map[string]SandboxPlugin
}

// NewPluginRegistry creates an empty plugin registry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{plugins: make(map[string]SandboxPlugin)}
}

// Register adds a plugin under the given type name. Panics on duplicate registration.
func (r *PluginRegistry) Register(typeName string, p SandboxPlugin) {
	// 逻辑说明：按类型名写入插件注册表；重复名称立即 panic 暴露启动期装配错误，避免后注册实现静默覆盖生产行为。
	if _, exists := r.plugins[typeName]; exists {
		panic(fmt.Sprintf("sandbox plugin %q already registered", typeName))
	}
	r.plugins[typeName] = p
}

// Get returns the plugin for the given type, or an error if not found.
func (r *PluginRegistry) Get(typeName string) (SandboxPlugin, error) {
	// 逻辑说明：按精确类型名查找插件并返回；未注册时给出包含名称的错误，不自动选择其他 provider。
	p, ok := r.plugins[typeName]
	if !ok {
		return nil, fmt.Errorf("sandbox plugin %q not registered", typeName)
	}
	return p, nil
}
