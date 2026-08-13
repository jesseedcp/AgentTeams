"""Plugin package installer used by the `agentteams` CLI fallback."""

# 初学者导读：安装流程是“准备来源 → 安全解包到临时目录 → 找到 manifest 根 →
# 复制到项目插件目录 → 执行 install 生命周期 → 写入清单”。临时目录与路径穿越
# 检查避免一个恶意 tar 覆盖项目外文件；清单只在安装成功后保存，失败不会伪装成
# 已安装。update 复用同一流程，uninstall 则先运行插件自己的清理脚本。

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from agentteams_cli.config_store import ConfigStore


def _now() -> str:
    # 逻辑说明：`_now` 读取 UTC 时钟并格式化为插件清单使用的稳定时间戳；不修改安装状态，时钟异常按原语义向上传播。
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_metadata(manifest_path: Path) -> Tuple[str, str, list[str]]:
    """Load only the manifest fields the CLI needs.

    The full TeamHarness schema is validated by `plugins/scripts/validate-plugin.rb`.
    Keeping the CLI parser tiny avoids adding a PyYAML dependency for the fallback
    installer path.
    """
    # 逻辑说明：`_load_metadata` 只读取 fallback 所需字段；缺字段或 I/O 错误显式失败。
    if not manifest_path.exists():
        raise ValueError(f"missing plugin.yaml: {manifest_path}")

    metadata: Dict[str, str] = {}
    dependencies: list[str] = []
    section: Optional[str] = None

    for raw in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith(" ") and stripped.endswith(":"):
            section = stripped[:-1]
            continue
        if section == "metadata" and line.startswith("  ") and ":" in stripped:
            key, _, value = stripped.partition(":")
            metadata[key.strip()] = value.strip().strip('"').strip("'")
            continue
        if section == "dependencies" and stripped.startswith("- "):
            dependencies.append(stripped[2:].strip())

    name = metadata.get("name", "")
    version = metadata.get("version", "")
    if not name:
        raise ValueError("metadata.name is required")
    if not version:
        raise ValueError("metadata.version is required")
    return name, version, dependencies


def _safe_extract_tar(package: Path, target: Path) -> None:
    """只把归档成员解压到 ``target`` 内，拒绝 ``../`` 与绝对路径逃逸。"""
    # 逻辑说明：`_safe_extract_tar` 先验证成员路径与类型，全部通过后才产生解包副作用。
    try:
        with tarfile.open(package, "r:gz") as archive:
            root = target.resolve()
            for member in archive.getmembers():
                member_path = (target / member.name).resolve()
                if not str(member_path).startswith(str(root) + os.sep) and member_path != root:
                    raise ValueError(f"unsafe tar member: {member.name}")
                if member.name.startswith("/") or ".." in Path(member.name).parts:
                    raise ValueError(f"unsafe tar member: {member.name}")
                if not (member.isfile() or member.isdir()):
                    raise ValueError(f"unsafe tar member type: {member.name}")
            archive.extractall(target)
    except tarfile.TarError as exc:
        raise ValueError(f"invalid tar package: {exc}") from exc


def _find_plugin_root(search_root: Path) -> Path:
    # 逻辑说明：`_find_plugin_root` 接受直接根或唯一单层子目录；歧义和缺少 manifest 都拒绝继续。
    if (search_root / "plugin.yaml").is_file():
        return search_root
    candidates = [
        path
        for path in search_root.iterdir()
        if path.is_dir() and (path / "plugin.yaml").is_file()
    ]
    if len(candidates) == 1:
        return candidates[0]
    raise ValueError(f"plugin.yaml not found under {search_root}")


def _copytree(src: Path, dst: Path) -> None:
    # 逻辑说明：`_copytree` 用来源完整替换旧 content 并过滤缓存；复制失败直接终止安装。
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(
        src,
        dst,
        ignore=shutil.ignore_patterns("__pycache__", ".DS_Store", "*.pyc"),
    )


def _hash_path(path: Path) -> str:
    # 逻辑说明：`_hash_path` 按相对路径稳定计算 SHA-256；digest 记录安装内容而不用于鉴权。
    digest = hashlib.sha256()
    if path.is_file():
        digest.update(path.read_bytes())
    else:
        for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(file_path.relative_to(path)).encode("utf-8"))
            digest.update(b"\0")
            digest.update(file_path.read_bytes())
    return "sha256:" + digest.hexdigest()


def _script_env(store: ConfigStore, name: str, content_dir: Path) -> dict[str, str]:
    # 逻辑说明：`_script_env` 复制当前环境并补齐 lifecycle 变量；setdefault 保留显式设置。
    env = dict(os.environ)
    env.setdefault("AGENTTEAMS_PROJECT_DIR", str(store.project_dir))
    env.setdefault("AGENTTEAMS_PLUGIN_NAME", name)
    env.setdefault("AGENTTEAMS_PLUGIN_DIR", str(content_dir))
    env.setdefault("PILOT_DATA_DIR", str(store.root))
    env.setdefault("PILOT_LOG_DIR", str(store.root / "logs" / name))
    env.setdefault("PILOT_NODE_BIN", shutil.which("node") or "")
    env.setdefault("PILOT_NPM_BIN", shutil.which("npm") or "")
    return env


def _run_lifecycle(store: ConfigStore, name: str, content_dir: Path, script_name: str) -> bool:
    # 逻辑说明：`_run_lifecycle` 在项目目录运行 shell；捕获输出并以 bool 返回，非零不算成功。
    script = content_dir / "scripts" / script_name
    if not script.exists():
        return script_name == "uninstall.sh"
    result = subprocess.run(
        ["bash", str(script)],
        cwd=store.project_dir,
        env=_script_env(store, name, content_dir),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        print(f"ERROR: {script_name} failed for {name}: {detail}")
        return False
    return True


def _prepare_package(package: Path) -> Tuple[Path, Optional[tempfile.TemporaryDirectory[str]]]:
    # 逻辑说明：`_prepare_package` 直接解析目录或安全解包归档；临时句柄由调用方最终清理。
    if not package.exists():
        raise ValueError(f"package not found: {package}")
    if package.is_dir():
        return _find_plugin_root(package), None

    tmp = tempfile.TemporaryDirectory(prefix="agentteams-plugin-")
    tmp_root = Path(tmp.name)
    _safe_extract_tar(package, tmp_root)
    return _find_plugin_root(tmp_root), tmp


def install(
    # package（归档）与 source（目录）是互斥来源，最后都会归一化成可复制的插件根。
    store: ConfigStore,
    name: str,
    package: Optional[Path] = None,
    source: Optional[Path] = None,
) -> bool:
    # 逻辑说明：`install` 规范化来源、验证、替换并运行 lifecycle；全部成功后才持久化清单。
    # 任一步失败均返回 False，finally 始终清理临时解包目录，避免残留被误认作已安装状态。
    if not package and not source:
        print("ERROR: Use --package or --source.")
        return False

    tmp: Optional[tempfile.TemporaryDirectory[str]] = None
    try:
        plugin_root, tmp = _prepare_package(package) if package else (_find_plugin_root(source), None)  # type: ignore[arg-type]
        manifest_name, version, dependencies = _load_metadata(plugin_root / "plugin.yaml")
        if manifest_name != name:
            print(f"ERROR: Plugin source metadata.name is '{manifest_name}', not requested plugin '{name}'.")
            return False

        plugin_dir = store.plugin_dir(name)
        content_dir = store.plugin_content_dir(name)
        old_content_dir = content_dir if content_dir.exists() else None
        if old_content_dir:
            if not _run_lifecycle(store, name, old_content_dir, "uninstall.sh"):
                return False

        plugin_dir.mkdir(parents=True, exist_ok=True)
        _copytree(plugin_root, content_dir)

        if not _run_lifecycle(store, name, content_dir, "install.sh"):
            return False

        content_dir_rel = content_dir.relative_to(store.project_dir)
        manifest: Dict[str, Any] = {
            "name": name,
            "version": version,
            "installed_at": _now(),
            "content_dir": str(content_dir_rel),
            "content_hash": _hash_path(content_dir),
            "dependencies": dependencies,
        }
        if package:
            manifest["package"] = str(package)
        if source:
            manifest["source"] = str(source)
        store.save_plugin_manifest(name, manifest)
        print(f"Installed {name} v{version}.")
        return True
    except Exception as exc:
        print(f"ERROR: Failed to install {name}: {exc}")
        return False
    finally:
        if tmp:
            tmp.cleanup()


def update(
    store: ConfigStore,
    name: str,
    package: Optional[Path] = None,
    source: Optional[Path] = None,
) -> bool:
    # 逻辑说明：`update` 确认旧插件存在后复用 install 路径，并比较前后版本输出结果。
    old = store.get_plugin_manifest(name)
    if not old:
        print(f"Plugin '{name}' is not installed. Use 'install' first.")
        return False
    if install(store, name, package=package, source=source):
        new = store.get_plugin_manifest(name) or {}
        print(f"Updated {name}: {old.get('version', '?')} -> {new.get('version', '?')}")
        return True
    return False


def uninstall(store: ConfigStore, name: str) -> bool:
    # 逻辑说明：`uninstall` 先运行 lifecycle，成功后才删除项目状态；失败时保留清单便于恢复。
    manifest = store.get_plugin_manifest(name)
    if not manifest:
        print(f"Plugin '{name}' is not installed.")
        return False
    content_dir = store.project_dir / manifest.get("content_dir", store.plugin_content_dir(name))
    if not _run_lifecycle(store, name, content_dir, "uninstall.sh"):
        return False
    store.remove_plugin(name)
    print(f"Uninstalled {name}.")
    return True


def list_plugins(store: ConfigStore) -> list[Dict[str, Any]]:
    # 逻辑说明：`list_plugins` 返回 ConfigStore 的稳定清单，不触发 lifecycle 或文件写入。
    return store.list_plugins()
