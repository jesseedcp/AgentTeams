package service

import (
	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
)

const roomMetaEventType = "room.meta"

func roomMetaState(content map[string]interface{}) []matrix.StateEvent {
	return []matrix.StateEvent{{
		Type:     roomMetaEventType,
		StateKey: "",
		Content:  content,
	}}
}

func teamRoomMeta(req TeamRoomRequest, teamAdminID, leaderMatrixID string, userIDForName func(string) string) map[string]interface{} {
	// 逻辑说明：teamRoomMeta 接收 req(TeamRoomRequest)、teamAdminID/leaderMatrixID(string)、userIDForName(func(string) string)，依次借助 baseRoomMeta、namedUserMeta、workerUserMeta、humanMemberMeta处理Team的期望结果。
	// 返回/状态：返回 map[string]interface{}；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	meta := baseRoomMeta("team_room")
	if req.TeamName != "" {
		meta["teamName"] = req.TeamName
	}
	if req.AdminSpec != nil && teamAdminID != "" {
		meta["teamAdmin"] = namedUserMeta(teamAdminID, req.AdminSpec.Name)
	}
	if req.LeaderName != "" && leaderMatrixID != "" {
		meta["leaderWorker"] = workerUserMeta(leaderMatrixID, req.LeaderName)
	}
	if members := humanMemberMeta(req.HumanMembers, userIDForName); len(members) > 0 {
		meta["humanMembers"] = members
	}
	return meta
}

func leaderDMRoomMeta(req TeamRoomRequest, teamAdminID, leaderMatrixID string) map[string]interface{} {
	// 逻辑说明：leaderDMRoomMeta 接收 req(TeamRoomRequest)、teamAdminID/leaderMatrixID(string)，依次借助 baseRoomMeta、namedUserMeta、workerUserMeta处理Controller 状态的期望结果。
	// 返回/状态：返回 map[string]interface{}；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	meta := baseRoomMeta("direct_room")
	if req.TeamName != "" {
		meta["teamName"] = req.TeamName
	}
	if req.AdminSpec != nil && teamAdminID != "" {
		meta["teamAdmin"] = namedUserMeta(teamAdminID, req.AdminSpec.Name)
	}
	if req.LeaderName != "" && leaderMatrixID != "" {
		meta["leaderWorker"] = workerUserMeta(leaderMatrixID, req.LeaderName)
	}
	return meta
}

func workerRoomMeta(req WorkerProvisionRequest, workerMatrixID, leaderMatrixID string) map[string]interface{} {
	// 逻辑说明：workerRoomMeta 接收 req(WorkerProvisionRequest)、workerMatrixID/leaderMatrixID(string)，依次借助 baseRoomMeta、workerUserMeta处理Worker 成员的期望结果。
	// 返回/状态：返回 map[string]interface{}；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	meta := baseRoomMeta("worker_room")
	if req.TeamName != "" {
		meta["teamName"] = req.TeamName
	}
	if req.Name != "" {
		meta["workerName"] = req.Name
	}
	if req.Role == "team_leader" && workerMatrixID != "" {
		meta["leaderWorker"] = workerUserMeta(workerMatrixID, req.Name)
	} else if req.TeamLeaderName != "" && leaderMatrixID != "" {
		meta["leaderWorker"] = workerUserMeta(leaderMatrixID, req.TeamLeaderName)
	}
	return meta
}

func managerDMRoomMeta(managerName, managerMatrixID, adminMatrixID, adminName string) map[string]interface{} {
	// 逻辑说明：managerDMRoomMeta 接收 managerName/managerMatrixID/adminMatrixID/adminName(string)，依次借助 baseRoomMeta、namedUserMeta处理Manager的期望结果。
	// 返回/状态：返回 map[string]interface{}；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	meta := baseRoomMeta("direct_room")
	if managerName != "" {
		meta["managerName"] = managerName
	}
	if managerMatrixID != "" {
		meta["manager"] = namedUserMeta(managerMatrixID, "manager")
	}
	if adminMatrixID != "" {
		meta["admin"] = namedUserMeta(adminMatrixID, adminName)
	}
	return meta
}

func baseRoomMeta(kind string) map[string]interface{} {
	return map[string]interface{}{
		"schemaVersion": 1,
		"roomKind":      kind,
		"lifecycle":     "persistent",
		"createdBy":     "agentteams",
	}
}

func namedUserMeta(userID, name string) map[string]interface{} {
	// 逻辑说明：namedUserMeta 接收 userID/name(string)，按本函数中的条件与转换步骤处理Controller 状态的期望结果。
	// 返回/状态：返回 map[string]interface{}；会更新 Controller 状态的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	out := map[string]interface{}{"userId": userID}
	if name != "" {
		out["name"] = name
	}
	return out
}

func workerUserMeta(userID, workerName string) map[string]interface{} {
	// 逻辑说明：workerUserMeta 接收 userID/workerName(string)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 map[string]interface{}；会更新 Worker 成员的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	out := map[string]interface{}{"userId": userID}
	if workerName != "" {
		out["workerName"] = workerName
	}
	return out
}

func humanMemberMeta(members []v1beta1.TeamMemberSpec, userIDForName func(string) string) []map[string]interface{} {
	// 逻辑说明：humanMemberMeta 接收 members([]v1beta1.TeamMemberSpec)、userIDForName(func(string) string)，依次借助 userIDForName、namedUserMeta处理Human的期望结果。
	// 返回/状态：返回 []map[string]interface{}；会更新 Human的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	out := make([]map[string]interface{}, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		userID := member.MatrixUserID
		if userID == "" && member.Name != "" && userIDForName != nil {
			userID = userIDForName(member.Name)
		}
		if userID == "" {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		out = append(out, namedUserMeta(userID, member.Name))
	}
	return out
}
