"""Local `.agentteams/` state store."""

# 初学者导读：插件内容和安装清单保存在当前项目的 ``.agentteams`` 下，不写入
# 全局用户目录。清单记录版本、来源与 digest，插件管理器才能判断当前安装内容；
# 它不是 Agent 的长期记忆，也不应保存 Matrix token 或模型 key。

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional


class ConfigStore:
    """Read and write project-local AgentTeams plugin state.

    即读取和写入项目级插件目录及其安装清单。
    """

    def __init__(self, project_dir: Optional[Path] = None) -> None:
        # 逻辑说明：`__init__` 接收可选项目目录，固定插件状态根目录；只更新当前 ConfigStore 实例。
        self.project_dir = Path(project_dir) if project_dir else Path.cwd()
        self.root = self.project_dir / ".agentteams"

    def _ensure_parent(self, path: Path) -> Path:
        # 逻辑说明：`_ensure_parent` 在写文件前递归创建父目录并返回原路径；目录创建失败直接向上传播。
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _read_json(self, path: Path) -> Dict[str, Any]:
        # 逻辑说明：`_read_json` 读取项目级 JSON；文件不存在返回空映射，格式或 I/O 错误不伪装成空配置。
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, data: Dict[str, Any]) -> None:
        # 逻辑说明：`_write_json` 先确保父目录存在，再以 UTF-8 写入稳定格式 JSON；写入会改变磁盘状态。
        self._ensure_parent(path)
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def plugin_dir(self, name: str) -> Path:
        return self.root / "plugins" / name

    def plugin_content_dir(self, name: str) -> Path:
        return self.plugin_dir(name) / "content"

    def list_plugins(self) -> List[Dict[str, Any]]:
        # 逻辑说明：`list_plugins` 按目录名稳定排序并读取已存在的 manifest；忽略没有清单的残缺目录。
        plugins_dir = self.root / "plugins"
        if not plugins_dir.exists():
            return []
        result: List[Dict[str, Any]] = []
        for path in sorted(plugins_dir.iterdir()):
            manifest = path / "manifest.json"
            if manifest.exists():
                result.append(self._read_json(manifest))
        return result

    def get_plugin_manifest(self, name: str) -> Optional[Dict[str, Any]]:
        # 逻辑说明：`get_plugin_manifest` 把插件名解析为项目内 manifest 路径；不存在返回 None，其余读取错误向上传播。
        path = self.plugin_dir(name) / "manifest.json"
        if not path.exists():
            return None
        return self._read_json(path)

    def save_plugin_manifest(self, name: str, manifest: Dict[str, Any]) -> None:
        # 逻辑说明：`save_plugin_manifest` 把安装结果持久化到对应插件清单；只有生命周期成功后才应调用。
        self._write_json(self.plugin_dir(name) / "manifest.json", manifest)

    def remove_plugin(self, name: str) -> None:
        # 逻辑说明：`remove_plugin` 删除项目内该插件的完整状态目录；不存在时保持幂等，删除失败显式报错。
        plugin_dir = self.plugin_dir(name)
        if plugin_dir.exists():
            shutil.rmtree(plugin_dir)
