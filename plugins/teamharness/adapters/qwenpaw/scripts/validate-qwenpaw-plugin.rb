#!/usr/bin/env ruby
# frozen_string_literal: true

# 初学者导读：检查“已经生成”的 QwenPaw TeamHarness 包，而不是源目录。这样能发现
# 打包 include 漏项、plugin.json 错误和入口不可导入等发布问题；脚本只读并验证。

require "json"
require "open3"
require "pathname"
require "tmpdir"
require "yaml"

abort("usage: validate-qwenpaw-plugin.rb <generated-plugin-dir>") if ARGV.empty?

package_dir = Pathname.new(ARGV[0]).expand_path

# 逻辑说明：接收生成包的契约错误，写到 stderr 并以状态码 1 结束校验；调用方据此阻止错误 zip 进入发布流程。
def fail!(message)
  warn "ERROR: #{message}"
  exit 1
end

# 逻辑说明：确认生成包中的目标路径确实是文件；成功时静默返回，缺失时通过统一失败出口报告精确路径，不会补造文件。
def assert_file(path)
  fail!("missing file: #{path}") unless path.file?
end

# 逻辑说明：确认生成包中的目标路径确实是目录；只读取文件系统元数据，缺失或类型错误都会立即终止发布校验。
def assert_dir(path)
  fail!("missing directory: #{path}") unless path.directory?
end

# 逻辑说明：在一次性缓存目录中调用 `python3 -m py_compile` 检查指定 Python 文件，并把 stdout、stderr、退出状态原样返回；缓存不会写进待发布包，调用方负责判定失败。
def py_compile(path)
  Dir.mktmpdir("teamharness-pycache-") do |cache_dir|
    Open3.capture3(
      { "PYTHONPYCACHEPREFIX" => cache_dir },
      "python3",
      "-m",
      "py_compile",
      path.to_s
    )
  end
end

assert_dir(package_dir)
plugin_json = package_dir / "plugin.json"
plugin_py = package_dir / "plugin.py"
task_trace_py = package_dir / "task_trace.py"
asset_dir = package_dir / "teamharness"

assert_file(plugin_json)
assert_file(plugin_py)
assert_file(task_trace_py)
assert_dir(asset_dir)

manifest = JSON.parse(plugin_json.read)
fail!("plugin id must be teamharness") unless manifest["id"] == "teamharness"
fail!("plugin type must be general") unless manifest["type"] == "general"
fail!("backend entry must be plugin.py") unless manifest.dig("entry", "backend") == "plugin.py"

features = manifest.dig("meta", "features") || []
fail!("qwenpaw plugin must not declare periodic-sync") if features.include?("periodic-sync")

assert_file(asset_dir / "plugin.yaml")
source_manifest = YAML.load_file(asset_dir / "plugin.yaml")
version = source_manifest.fetch("metadata").fetch("version")
fail!("plugin version mismatch") unless manifest["version"] == version

assert_file(asset_dir / "prompts/team/TEAMS.md")
assert_file(asset_dir / "prompts/agent/worker.md")
assert_file(asset_dir / "prompts/manager/AGENTS.md")
assert_file(asset_dir / "skills/team/communication/SKILL.md")
assert_file(asset_dir / "mcp/server.py")
assert_file(asset_dir / "mcp/message_tool.py")
assert_file(asset_dir / "mcp/roomflow_tool.py")
fail!("top-level hooks must not be packaged for qwenpaw") if (asset_dir / "hooks").exist?

stdout, stderr, status = py_compile(plugin_py)
fail!("plugin.py syntax check failed: #{stderr}#{stdout}") unless status.success?

stdout, stderr, status = py_compile(task_trace_py)
fail!("task_trace.py syntax check failed: #{stderr}#{stdout}") unless status.success?

stdout, stderr, status = py_compile(asset_dir / "mcp/server.py")
fail!("teamharness mcp syntax check failed: #{stderr}#{stdout}") unless status.success?

stdout, stderr, status = py_compile(asset_dir / "mcp/message_tool.py")
fail!("teamharness message tool syntax check failed: #{stderr}#{stdout}") unless status.success?

stdout, stderr, status = py_compile(asset_dir / "mcp/roomflow_tool.py")
fail!("teamharness roomflow tool syntax check failed: #{stderr}#{stdout}") unless status.success?

puts "ok: qwenpaw teamharness #{version}"
