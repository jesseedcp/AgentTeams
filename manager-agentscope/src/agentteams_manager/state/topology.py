"""Materialized Controller/Matrix room topology.

物化 Controller 资源与 Matrix 房间、用户之间的绑定拓扑。

policy resolver 需要快速回答“这个 room 属于哪个 Worker/Team/Project、发送者是谁”。
heartbeat 从 Controller 和 Matrix 的权威事实刷新本表；稳定 alias 若迁移到新 room，也要
替换旧绑定，避免消息进入历史房间。物化表是授权查询缓存，不应凭聊天内容自行写入。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    HumanResource,
    RoomKind,
    TopologySnapshot,
)

from .database import Database


@dataclass(frozen=True, slots=True)
class TopologyBinding:
    resource_type: str
    resource_name: str
    room_kind: RoomKind
    room_id: str
    matrix_user_id: str | None
    payload: dict[str, object]
    refreshed_at: datetime


class ActorKind(StrEnum):
    ADMIN = "admin"
    WORKER = "worker"
    TEAM_LEADER = "team_leader"
    TEAM_WORKER = "team_worker"
    HUMAN = "human"
    TRUSTED_CONTACT = "trusted_contact"


@dataclass(frozen=True, slots=True)
class Actor:
    matrix_user_id: str
    kind: ActorKind
    resource_name: str | None = None
    team_name: str | None = None
    payload: dict[str, object] | None = None


def _binding_from_row(row: sqlite3.Row) -> TopologyBinding:
    # 逻辑说明：解析 room kind、payload JSON 和刷新时间，将物化表行恢复为授权查询绑定；损坏缓存显式失败以触发重新同步。
    return TopologyBinding(
        resource_type=row["resource_type"],
        resource_name=row["resource_name"],
        room_kind=RoomKind(row["room_kind"]),
        room_id=row["room_id"],
        matrix_user_id=row["matrix_user_id"],
        payload=json.loads(row["payload_json"]),
        refreshed_at=datetime.fromisoformat(row["refreshed_at"]),
    )


class TopologyRepository:
    def __init__(
        self,
        database: Database,
        *,
        admin_user_id: str | None = None,
    ) -> None:
        # 逻辑说明：绑定拓扑数据库和可选管理员身份；实际房间、成员与 actor 状态只在后续事务中写入。
        self._database = database
        self._admin_user_id = admin_user_id

    async def replace_snapshot(self, snapshot: TopologySnapshot) -> None:
        # 逻辑说明：先在事务外把 Controller snapshot 规范成 room、human、actor 三组行并检查一房多 kind 冲突，再在单事务整体替换并推进 revision。
        rows: list[tuple[str, str, str, str, str | None, str, str]] = []
        human_rows: list[tuple[str, str, int, str, str, str]] = []
        actor_rows: list[
            tuple[str, str, str | None, str | None, str, str]
        ] = []
        kinds_by_room: dict[str, RoomKind] = {}
        teams_by_leader = {team.leader: team for team in snapshot.teams}
        team_worker_names = {
            worker_name
            for team in snapshot.teams
            for worker_name in team.workers
        }

        def add(
            *,
            resource_type: str,
            resource_name: str,
            room_kind: RoomKind,
            room_id: str | None,
            matrix_user_id: str | None,
            payload: str,
        ) -> None:
            # 逻辑说明：忽略无 room 资源，拒绝同一 room 映射不同 RoomKind，并把规范行加入待写列表；此阶段不修改数据库。
            if not room_id:
                return
            previous = kinds_by_room.get(room_id)
            if previous is not None and previous is not room_kind:
                raise ConflictError(
                    f"{room_id} has multiple room kinds: "
                    f"{previous.value}, {room_kind.value}",
                )
            kinds_by_room[room_id] = room_kind
            rows.append(
                (
                    resource_type,
                    resource_name,
                    room_kind.value,
                    room_id,
                    matrix_user_id,
                    payload,
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        workers_by_name = {worker.name: worker for worker in snapshot.workers}
        teams_by_worker = {
            worker_name: team
            for team in snapshot.teams
            for worker_name in team.workers
        }
        for worker in snapshot.workers:
            team = teams_by_leader.get(worker.name)
            actor_kind = ActorKind.WORKER
            actor_team_name: str | None = None
            if team is not None:
                actor_kind = ActorKind.TEAM_LEADER
                actor_team_name = team.name
            elif worker.name in teams_by_worker:
                actor_kind = ActorKind.TEAM_WORKER
                actor_team_name = teams_by_worker[worker.name].name
            if worker.matrix_user_id:
                actor_rows.append(
                    (
                        worker.matrix_user_id,
                        actor_kind.value,
                        worker.name,
                        actor_team_name,
                        worker.model_dump_json(),
                        snapshot.refreshed_at.isoformat(),
                    ),
                )
            if worker.name in teams_by_leader or worker.name in team_worker_names:
                continue
            add(
                resource_type="worker",
                resource_name=worker.name,
                room_kind=RoomKind.WORKER_ROOM,
                room_id=worker.room_id,
                matrix_user_id=worker.matrix_user_id,
                payload=worker.model_dump_json(),
            )
        for team in snapshot.teams:
            leader = workers_by_name.get(team.leader)
            add(
                resource_type="team",
                resource_name=team.name,
                room_kind=RoomKind.LEADER_ROOM,
                room_id=team.room_id,
                matrix_user_id=(
                    leader.matrix_user_id if leader is not None else None
                ),
                payload=team.model_dump_json(),
            )
            team_room_id = team.spec.get("teamRoomID")
            add(
                resource_type="team",
                resource_name=team.name,
                room_kind=RoomKind.TEAM_ROOM,
                room_id=(
                    str(team_room_id)
                    if isinstance(team_room_id, str)
                    else None
                ),
                matrix_user_id=None,
                payload=team.model_dump_json(),
            )
        for human in snapshot.humans:
            human_rows.append(
                (
                    human.name,
                    human.matrix_user_id,
                    human.permission_level,
                    json.dumps(list(human.allowed_rooms)),
                    human.model_dump_json(),
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：删除旧 Controller 派生绑定后批量插入新 topology/human/actor，并在同一事务更新 revision；任一步失败保留完整旧快照。
            connection.execute(
                "DELETE FROM topology "
                "WHERE resource_type IN ('worker', 'team')",
            )
            connection.execute("DELETE FROM human_access")
            connection.execute(
                "DELETE FROM topology_actors "
                "WHERE actor_kind IN "
                "('worker', 'team_leader', 'team_worker')",
            )
            connection.executemany(
                """
                INSERT INTO topology(
                    resource_type, resource_name, room_kind, room_id,
                    matrix_user_id, payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            connection.executemany(
                """
                INSERT INTO human_access(
                    name, matrix_user_id, permission_level,
                    allowed_rooms_json, payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                human_rows,
            )
            connection.executemany(
                """
                INSERT INTO topology_actors(
                    matrix_user_id, actor_kind, resource_name, team_name,
                    payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(matrix_user_id) DO UPDATE SET
                    actor_kind=excluded.actor_kind,
                    resource_name=excluded.resource_name,
                    team_name=excluded.team_name,
                    payload_json=excluded.payload_json,
                    refreshed_at=excluded.refreshed_at
                """,
                actor_rows,
            )
            connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES ('topology_revision', ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (
                    str(snapshot.revision),
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def actor_for_sender(
        self,
        matrix_user_id: str,
    ) -> Actor | None:
        # 逻辑说明：先用配置的管理员 ID 快速判定 Admin，否则在单一读快照依次解析 Worker/Team actor、Human 和 trusted contact；都未命中返回 None。
        if self._admin_user_id and matrix_user_id == self._admin_user_id:
            return Actor(
                matrix_user_id=matrix_user_id,
                kind=ActorKind.ADMIN,
            )

        def read(connection: sqlite3.Connection) -> Actor | None:
            # 逻辑说明：按信任优先级查询物化身份并解析 payload；只依据权威同步表，不从聊天内容推断角色。
            row = connection.execute(
                """
                SELECT actor_kind, resource_name, team_name, payload_json
                  FROM topology_actors
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if row is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind(row["actor_kind"]),
                    resource_name=row["resource_name"],
                    team_name=row["team_name"],
                    payload=json.loads(row["payload_json"]),
                )

            human = connection.execute(
                """
                SELECT name, payload_json FROM human_access
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if human is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind.HUMAN,
                    resource_name=human["name"],
                    payload=json.loads(human["payload_json"]),
                )

            trusted = connection.execute(
                """
                SELECT 1 FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND (owner_user_id=? OR peer_user_id=?)
                 LIMIT 1
                """,
                (matrix_user_id, matrix_user_id),
            ).fetchone()
            if trusted is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind.TRUSTED_CONTACT,
                )
            return None

        return await self._database.read(read)

    async def revision(self) -> int:
        # 逻辑说明：读取最后成功替换的 topology revision，未同步时返回 0；不修改快照。
        def read(connection: sqlite3.Connection) -> int:
            # 逻辑说明：查询通用 key/value 游标并转换整数。
            row = connection.execute(
                "SELECT value FROM key_values WHERE key='topology_revision'",
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def room_binding(self, room_id: str) -> TopologyBinding | None:
        # 逻辑说明：按 Matrix room ID 返回物化资源绑定，不存在返回 None，供 policy resolver 选择最小权限。
        def read(connection: sqlite3.Connection) -> TopologyBinding | None:
            # 逻辑说明：单行查询并统一转换 payload 和枚举。
            row = connection.execute(
                "SELECT * FROM topology WHERE room_id=?",
                (room_id,),
            ).fetchone()
            return _binding_from_row(row) if row else None

        return await self._database.read(read)

    async def human_for_sender(
        self,
        matrix_user_id: str,
    ) -> HumanResource | None:
        """Resolve Controller-declared Human access by Matrix identity."""
        # 逻辑说明：按 Matrix sender 查 Controller 声明的 Human payload，未命中返回 None；不根据房间成员关系自动授予身份。
        def read(connection: sqlite3.Connection) -> HumanResource | None:
            # 逻辑说明：读取 JSON payload 并用领域模型完整校验权限字段。
            row = connection.execute(
                """
                SELECT payload_json FROM human_access
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if row is None:
                return None
            return HumanResource.model_validate_json(row["payload_json"])

        return await self._database.read(read)

    async def all_bindings(self) -> tuple[TopologyBinding, ...]:
        # 逻辑说明：读取全部物化绑定并按资源种类/名称稳定返回，用于管理诊断和策略缓存刷新。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[TopologyBinding, ...]:
            # 逻辑说明：在同一读快照批量转换所有 binding。
            rows = connection.execute(
                """
                SELECT * FROM topology
                 ORDER BY resource_type, resource_name
                """,
            ).fetchall()
            return tuple(_binding_from_row(row) for row in rows)

        return await self._database.read(read)

    async def upsert_project(
        self,
        *,
        project_id: str,
        room_id: str,
        payload: dict[str, object],
        refreshed_at: datetime,
    ) -> None:
        # 逻辑说明：验证 project 与 room 键，在事务中按 project identity upsert Project Room 绑定和稳定 JSON payload；不会替换 Worker/Team snapshot。
        _require_channel_key(project_id, "project_id")
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：单条 upsert 原子更新 room、payload 与刷新时间，alias 迁移时旧 room 不再被视为当前绑定。
            connection.execute(
                """
                INSERT INTO topology(
                    resource_type, resource_name, room_kind, room_id,
                    matrix_user_id, payload_json, refreshed_at
                ) VALUES ('project', ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(resource_type, resource_name, room_kind)
                DO UPDATE SET
                    room_id=excluded.room_id,
                    payload_json=excluded.payload_json,
                    refreshed_at=excluded.refreshed_at
                """,
                (
                    project_id,
                    RoomKind.PROJECT_ROOM.value,
                    room_id,
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    refreshed_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def set_primary_channel(
        self,
        user_id: str,
        room_id: str,
    ) -> None:
        # 逻辑说明：验证 user/room 后按用户 upsert 唯一 primary channel；重复设置更新房间与时间，不增加多条主通道。
        _require_channel_key(user_id, "user_id")
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：用 relationship 唯一键原子创建或替换 primary 关系。
            connection.execute(
                """
                INSERT INTO channel_relationships(
                    relationship_kind, owner_user_id, peer_user_id,
                    room_id, updated_at
                ) VALUES ('primary', ?, '', ?, ?)
                ON CONFLICT(
                    relationship_kind, owner_user_id, peer_user_id
                ) DO UPDATE SET
                    room_id=excluded.room_id,
                    updated_at=excluded.updated_at
                """,
                (user_id, room_id, datetime.now(UTC).isoformat()),
            )

        await self._database.write(write)

    async def clear_primary_channel(self, user_id: str) -> None:
        # 逻辑说明：验证用户 ID 后删除其 primary 关系；不存在时幂等成功，不影响 trusted 关系。
        _require_channel_key(user_id, "user_id")

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：条件删除指定用户的 primary 行，事务失败则保留旧路由。
            connection.execute(
                """
                DELETE FROM channel_relationships
                 WHERE relationship_kind='primary'
                   AND owner_user_id=?
                """,
                (user_id,),
            )

        await self._database.write(write)

    async def primary_channel(self, user_id: str) -> str | None:
        # 逻辑说明：验证用户 ID 并读取唯一 primary room；未配置返回 None，不回退到任意加入房间。
        _require_channel_key(user_id, "user_id")

        def read(connection: sqlite3.Connection) -> str | None:
            # 逻辑说明：执行单行关系查询并规范返回字符串。
            row = connection.execute(
                """
                SELECT room_id FROM channel_relationships
                 WHERE relationship_kind='primary'
                   AND owner_user_id=?
                """,
                (user_id,),
            ).fetchone()
            return str(row["room_id"]) if row is not None else None

        return await self._database.read(read)

    async def set_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
        room_id: str,
    ) -> None:
        # 逻辑说明：先把两个不同用户规范为无方向有序对并验证 room，再 upsert trusted 关系；反向重复调用命中同一记录。
        first, second = _trusted_pair(first_user_id, second_user_id)
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：按 relationship kind 与有序用户对原子创建或更新可信 room。
            connection.execute(
                """
                INSERT INTO channel_relationships(
                    relationship_kind, owner_user_id, peer_user_id,
                    room_id, updated_at
                ) VALUES ('trusted', ?, ?, ?, ?)
                ON CONFLICT(
                    relationship_kind, owner_user_id, peer_user_id
                ) DO UPDATE SET
                    room_id=excluded.room_id,
                    updated_at=excluded.updated_at
                """,
                (
                    first,
                    second,
                    room_id,
                    datetime.now(UTC).isoformat(),
                ),
            )

        await self._database.write(write)

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]:
        # 逻辑说明：验证用户并查询其作为任一端点的全部 trusted rooms，排序后返回不可变元组。
        _require_channel_key(user_id, "user_id")

        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
            # 逻辑说明：在一致快照读取关联 room，不改变信任关系。
            rows = connection.execute(
                """
                SELECT room_id FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND (owner_user_id=? OR peer_user_id=?)
                 ORDER BY room_id
                """,
                (user_id, user_id),
            ).fetchall()
            return tuple(str(row["room_id"]) for row in rows)

        return await self._database.read(read)

    async def remove_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
    ) -> None:
        # 逻辑说明：规范两个用户为同一有序身份后删除对应 trusted 关系；重复删除幂等且不影响 primary channel。
        first, second = _trusted_pair(first_user_id, second_user_id)

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：按完整复合键条件删除，避免移除该用户与其他联系人的信任。
            connection.execute(
                """
                DELETE FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND owner_user_id=?
                   AND peer_user_id=?
                """,
                (first, second),
            )

        await self._database.write(write)


def _require_channel_key(value: str, label: str) -> None:
    # 逻辑说明：拒绝空的房间/用户关系键并在错误中指出字段名，防止不同缺失关系坍缩为同一数据库主键。
    if not value:
        raise ValueError(f"{label} cannot be empty")


def _trusted_pair(first: str, second: str) -> tuple[str, str]:
    # 逻辑说明：校验两个非空且不同的用户 ID，并稳定排序为无方向关系键，使 A-B 与 B-A 共享同一数据库记录。
    _require_channel_key(first, "first_user_id")
    _require_channel_key(second, "second_user_id")
    if first == second:
        raise ValueError("trusted channel users must be distinct")
    ordered = tuple(sorted((first, second)))
    return ordered[0], ordered[1]
