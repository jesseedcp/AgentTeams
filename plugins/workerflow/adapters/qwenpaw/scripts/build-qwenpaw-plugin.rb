#!/usr/bin/env ruby
# frozen_string_literal: true

# 初学者导读：把 WorkerFlow 的 MCP、技能和 QwenPaw adapter 组装成宿主可安装包。
# 只复制 manifest 允许的第一方内容，并清除缓存/生成物，防止本地开发状态进入镜像。

require "fileutils"
require "json"
require "open3"
require "pathname"
require "tmpdir"
require "yaml"

manifest_path = Pathname.new(ARGV[0] || "plugins/workerflow/plugin.yaml").expand_path
plugin_root = manifest_path.dirname
repo_root = plugin_root.ascend.find { |path| (path / ".git").directory? } || plugin_root
adapter_root = plugin_root / "adapters/qwenpaw"
out_dir = Pathname.new(ENV["OUT_DIR"] || (repo_root / "dist/adapters/qwenpaw").to_s).expand_path

abort("missing manifest: #{manifest_path}") unless manifest_path.file?
abort("missing qwenpaw adapter: #{adapter_root}") unless adapter_root.directory?

manifest = YAML.load_file(manifest_path)
name = manifest.fetch("metadata").fetch("name")
version = manifest.fetch("metadata").fetch("version")
package_name = "#{name}-qwenpaw-#{version}"

# 逻辑说明：按 WorkerFlow manifest/adapter 的相对条目从源根复制到临时 QwenPaw 包；源缺失立即终止，所有写入都限制在 staging 根目录下。
def copy_entry(source_root, target_root, entry)
  src = source_root / entry
  abort("missing qwenpaw package source: #{src}") unless src.exist?

  dst = target_root / entry
  if src.directory?
    FileUtils.mkdir_p(dst)
    entries = Dir.glob((src / "*").to_s, File::FNM_DOTMATCH).reject do |path|
      [".", ".."].include?(File.basename(path))
    end
    FileUtils.cp_r(entries, dst)
  else
    FileUtils.mkdir_p(dst.dirname)
    FileUtils.cp(src, dst)
  end
end

# 逻辑说明：遍历临时 WorkerFlow 打包树并删除 Python 缓存与 macOS 元数据；只清理生成目录，不修改第一方源码。
def prune_generated(path)
  Dir.glob((path / "**/*").to_s, File::FNM_DOTMATCH).each do |item|
    base = File.basename(item)
    FileUtils.rm_rf(item) if base == "__pycache__" || base == ".DS_Store" || base.end_with?(".pyc")
  end
end

# 逻辑说明：覆盖同名旧 zip 后压缩本次 staging；优先使用系统 `zip`，不可用则回退 Python `zipfile`，任一路径失败都会中止打包并向 CI 返回非零状态。
def zip_dir(root, package_name, out_path)
  FileUtils.rm_f(out_path)
  if system("zip", "-v", out: File::NULL, err: File::NULL)
    Dir.chdir(root) do
      system("zip", "-qry", out_path.to_s, package_name) || abort("zip failed")
    end
    return
  end

  python = <<~PY
    import os, zipfile
    root = #{root.to_s.dump}
    package = #{package_name.dump}
    out = #{out_path.to_s.dump}
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        base = os.path.join(root, package)
        for dirpath, _, filenames in os.walk(base):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                rel = os.path.relpath(path, root)
                zf.write(path, rel)
  PY
  stdout, stderr, status = Open3.capture3("python3", "-c", python)
  abort("python zip failed: #{stderr}#{stdout}") unless status.success?
end

out_dir.mkpath
out_zip = out_dir / "#{package_name}.zip"
stable_zip = out_dir / "workerflow-qwenpaw.zip"

Dir.mktmpdir("workerflow-qwenpaw-") do |tmp|
  tmp_root = Pathname.new(tmp)
  staging = tmp_root / package_name
  asset_dir = staging / "workerflow"
  staging.mkpath
  asset_dir.mkpath

  %w[
    plugin.yaml
    prompts
    skills
    mcp
  ].each do |entry|
    copy_entry(plugin_root, asset_dir, entry)
  end

  copy_entry(adapter_root, staging, "plugin.py")

  qwenpaw_manifest = {
    "id" => "workerflow",
    "name" => "WorkerFlow",
    "version" => version,
    "type" => "general",
    "description" => "Worker-local workflow plugin for QwenPaw agents and subagents.",
    "author" => "AgentTeams",
    "entry" => {
      "backend" => "plugin.py"
    },
    "dependencies" => [],
    "min_version" => "2.0.1",
    "qwenpaw_version" => {
      "min" => "2.0.1",
      "max" => "2.1.0"
    },
    "meta" => {
      "category" => "workerflow",
      "features" => [
        "worker-internal-workflow",
        "temporary-agent-lifecycle",
        "workerflow-mcp"
      ]
    }
  }
  (staging / "plugin.json").write(
    JSON.pretty_generate(qwenpaw_manifest) + "\n",
    mode: "w",
    encoding: "UTF-8"
  )

  prune_generated(staging)
  zip_dir(tmp_root, package_name, out_zip)
  FileUtils.cp(out_zip, stable_zip)
end

puts out_zip
