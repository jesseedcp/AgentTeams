package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const roleTeamWorker = "worker"

// AppserviceHandler handles Matrix Application Service transaction pushes
// from the homeserver. Events arrive via HTTP push (PUT /transactions)
// instead of the controller polling /sync.
//
// Security: transactions are authenticated by verifying the hs_token
// supplied at registration time. No K8s auth middleware is involved.
type AppserviceHandler struct {
	hsToken   string        // expected hs_token from homeserver
	client    client.Client // K8s client
	namespace string
	now       func() time.Time

	mu   sync.Mutex
	seen map[string]struct{} // event dedup: "roomID/eventID/userID"
}

// NewAppserviceHandler creates a handler for Matrix appservice transaction pushes.
func NewAppserviceHandler(hsToken string, c client.Client, namespace string) *AppserviceHandler {
	// 逻辑说明：保存 homeserver token、Kubernetes client 和作用域，并初始化可替换时钟与事件去重表；构造时不访问 Matrix 或 CR。
	return &AppserviceHandler{
		hsToken:   hsToken,
		client:    c,
		namespace: namespace,
		now:       time.Now,
		seen:      make(map[string]struct{}),
	}
}

// --- Transaction push endpoint ---

// matrixEvent mirrors the subset of a Matrix event we care about.
type matrixEvent struct {
	Type    string `json:"type"`
	RoomID  string `json:"room_id"`
	EventID string `json:"event_id"`
	Sender  string `json:"sender"`
	Content struct {
		Mentions *struct {
			UserIDs []string `json:"user_ids"`
		} `json:"m.mentions"`
	} `json:"content"`
}

type transactionBody struct {
	Events []matrixEvent `json:"events"`
}

// HandleTransactions handles PUT /_matrix/app/v1/transactions/{txnId}.
// The homeserver pushes batches of events here; we filter for m.room.message
// events with m.mentions, then wake matching sleeping workers.
func (h *AppserviceHandler) HandleTransactions(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：先验证 homeserver token，再解析批量事件并筛选带 m.mentions 的消息；每个被提及用户独立尝试唤醒，单项失败记日志但整笔 transaction 始终 200，避免 homeserver 无限重放。
	logger := log.FromContext(r.Context()).WithName("appservice")
	txnID := txnIDFromPath(r.URL.Path)

	// Authenticate: verify hs_token from Authorization header.
	if !h.verifyHSToken(r) {
		// Surface as Info (not Error): a 403 here usually means the HS
		// reached us with a stale or wrong hs_token after a re-register;
		// it's the canonical signal for "network ok but token mismatch".
		logger.Info("appservice transaction rejected: invalid hs_token",
			"txnID", txnID, "remoteAddr", r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"errcode":"M_FORBIDDEN","error":"invalid hs_token"}`)
		return
	}

	var body transactionBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		logger.Error(err, "failed to decode transaction body", "txnID", txnID)
		// Return 200 anyway to avoid infinite retries from homeserver.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "{}")
		return
	}

	// V(1) entry log so operators enabling verbose logs can confirm
	// Tuwunel → controller push is reaching us at all (the most common
	// failure mode is the request never arriving, e.g. wrong appservice
	// URL or cross-cluster network not routable).
	logger.V(1).Info("appservice transaction received",
		"txnID", txnID, "totalEvents", len(body.Events))

	mentionCount := 0
	for _, event := range body.Events {
		if event.Type != "m.room.message" {
			continue
		}
		if event.Content.Mentions == nil || len(event.Content.Mentions.UserIDs) == 0 {
			continue
		}
		mentionCount++
		for _, userID := range event.Content.Mentions.UserIDs {
			if err := h.handleMention(r.Context(), event.RoomID, event.EventID, event.Sender, userID); err != nil {
				logger.Error(err, "handle mention event",
					"txnID", txnID, "roomID", event.RoomID, "eventID", event.EventID,
					"sender", event.Sender, "mentionedUser", userID)
			}
		}
	}

	if mentionCount > 0 {
		logger.Info("appservice transaction processed",
			"txnID", txnID,
			"totalEvents", len(body.Events), "mentionEvents", mentionCount)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{}")
}

// txnIDFromPath extracts the trailing path segment from
// /_matrix/app/v1/transactions/{txnId}. Best-effort: returns "" on miss.
func txnIDFromPath(p string) string {
	// 逻辑说明：提取最后一个非空路径段供日志关联；路径无尾段时返回空串，因为 transaction 处理不依赖该辅助值完成认证。
	if i := strings.LastIndex(p, "/"); i >= 0 && i < len(p)-1 {
		return p[i+1:]
	}
	return ""
}

// HandleUserQuery handles GET /_matrix/app/v1/users/{userId}.
// We don't manage virtual users; always return empty object.
func (h *AppserviceHandler) HandleUserQuery(w http.ResponseWriter, _ *http.Request) {
	// 逻辑说明：本 AppService 不声明自动创建虚拟用户，因此按 Matrix 协议返回成功空对象；真实受管用户由 Controller 的显式供应流程创建。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{}")
}

// HandleRoomQuery handles GET /_matrix/app/v1/rooms/{roomAlias}.
// We don't manage room aliases; always return empty object.
func (h *AppserviceHandler) HandleRoomQuery(w http.ResponseWriter, _ *http.Request) {
	// 逻辑说明：本 AppService 不声明自动创建 room alias，查询固定返回成功空对象；房间与 alias 仍由显式 Matrix 客户端操作管理。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{}")
}

// --- Internal logic ---

func (h *AppserviceHandler) verifyHSToken(r *http.Request) bool {
	// 逻辑说明：优先按 Bearer header 比对注册时 hs_token；对不发送该 header 的 homeserver 兼容 access_token query，但两种入口都必须与同一秘密精确匹配。
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		// Also check query param (some homeserver implementations).
		return r.URL.Query().Get("access_token") == h.hsToken
	}
	return strings.TrimPrefix(auth, "Bearer ") == h.hsToken
}

func (h *AppserviceHandler) handleMention(ctx context.Context, roomID, eventID, sender, userID string) error {
	// 逻辑说明：以 room/event/user 组合在互斥锁下去重 Matrix 重投事件，再依次检查独立 Worker 与 Team Worker；任一 CR 更新失败返回，使上层记录具体 mention 故障。
	logger := log.FromContext(ctx).WithName("appservice")

	// Dedup by roomID/eventID/userID.
	if eventID != "" {
		key := fmt.Sprintf("%s/%s/%s", roomID, eventID, userID)
		h.mu.Lock()
		if _, ok := h.seen[key]; ok {
			h.mu.Unlock()
			logger.V(1).Info("mention skipped: duplicate event",
				"roomID", roomID, "eventID", eventID, "mentionedUser", userID)
			return nil
		}
		h.seen[key] = struct{}{}
		h.mu.Unlock()
	}

	logger.V(1).Info("mention dispatch",
		"roomID", roomID, "eventID", eventID,
		"sender", sender, "mentionedUser", userID)

	if err := h.wakeStandaloneWorker(ctx, roomID, userID); err != nil {
		return fmt.Errorf("wake standalone worker: %w", err)
	}
	if err := h.wakeTeamWorker(ctx, roomID, userID); err != nil {
		return fmt.Errorf("wake team worker: %w", err)
	}
	return nil
}

// wakeStandaloneWorker wakes a standalone Worker whose MatrixUserID matches
// the mentioned user, provided the mention occurred in the worker's own room.
func (h *AppserviceHandler) wakeStandaloneWorker(ctx context.Context, roomID, userID string) error {
	// 逻辑说明：列出命名空间内 Worker，只选择 Matrix user 匹配、期望状态 Sleeping 且消息来自其自身房间者；符合时用当前 UTC 时间切到 Running，其余原因分别计数用于诊断越权或无效 mention。
	logger := log.FromContext(ctx).WithName("appservice")

	var workers v1beta1.WorkerList
	if err := h.client.List(ctx, &workers, client.InNamespace(h.namespace)); err != nil {
		logger.Error(err, "list standalone Worker CRs failed",
			"namespace", h.namespace, "mentionedUser", userID)
		return err
	}
	var (
		matchedUser  int // CRs whose Status.MatrixUserID equals the mentioned user
		notSleeping  int // matched but already Running/Stopped
		roomMismatch int // matched + Sleeping but mention came from foreign room
		wokenUp      int
	)
	for _, worker := range workers.Items {
		if worker.Status.MatrixUserID != userID {
			continue
		}
		matchedUser++
		if worker.Spec.DesiredState() != "Sleeping" {
			notSleeping++
			logger.V(1).Info("mention skipped: standalone worker not Sleeping",
				"worker", worker.Name, "desiredState", worker.Spec.DesiredState())
			continue
		}
		// Permission: mention must come from worker's own room.
		if worker.Status.RoomID != "" && roomID != "" && worker.Status.RoomID != roomID {
			roomMismatch++
			logger.V(1).Info("mention rejected: not worker own room",
				"worker", worker.Name, "workerRoom", worker.Status.RoomID,
				"mentionRoom", roomID)
			continue
		}
		now := h.now().UTC().Format(time.RFC3339)
		if err := h.setStandaloneWorkerRunning(ctx, worker.Name, now); err != nil {
			logger.Error(err, "set standalone worker Running failed",
				"worker", worker.Name, "room", roomID)
			return err
		}
		wokenUp++
		logger.Info("worker woken by mention",
			"worker", worker.Name, "room", roomID, "type", "standalone")
	}
	if matchedUser == 0 {
		// No standalone Worker advertises this Matrix user. Could be a
		// team worker (handled by wakeTeamWorker next) or a stale mention
		// for a deleted CR — verbose only to avoid log spam.
		logger.V(1).Info("no standalone worker matched mention",
			"mentionedUser", userID, "scanned", len(workers.Items))
	} else if wokenUp == 0 {
		logger.V(1).Info("standalone mention had no effect",
			"mentionedUser", userID,
			"matched", matchedUser, "notSleeping", notSleeping, "roomMismatch", roomMismatch)
	}
	return nil
}

// wakeTeamWorker wakes a team worker whose MatrixUserID matches the mentioned
// user. Permission boundary: the mention must occur in either the worker's
// own DM room OR the team's shared room. Mentions from other rooms (e.g.
// another team's room) are rejected — this prevents cross-team wake.
func (h *AppserviceHandler) wakeTeamWorker(ctx context.Context, roomID, userID string) error {
	// 逻辑说明：扫描 Team status 成员并匹配 worker 身份；仅允许来自成员私聊房间或本 Team 共享房间的 mention，随后确认 Spec 引用和 Worker CR 仍存在且 Sleeping，再更新状态以阻止跨团队唤醒。
	logger := log.FromContext(ctx).WithName("appservice")

	var teams v1beta1.TeamList
	if err := h.client.List(ctx, &teams, client.InNamespace(h.namespace)); err != nil {
		logger.Error(err, "list Team CRs failed",
			"namespace", h.namespace, "mentionedUser", userID)
		return err
	}
	var (
		matchedUser  int
		notSleeping  int
		roomMismatch int
		noSpec       int
		wokenUp      int
	)
	for _, team := range teams.Items {
		teamRoomID := team.Status.TeamRoomID
		for _, member := range team.Status.Members {
			if member.Role != roleTeamWorker || member.MatrixUserID != userID {
				continue
			}
			matchedUser++

			// === Permission boundary ===
			// Mention must come from one of:
			//   1. member.RoomID   — worker's own DM room
			//   2. teamRoomID      — team's shared room
			// Anything else (e.g. another team's room) is rejected.
			if roomID != "" {
				allowed := false
				if member.RoomID != "" && member.RoomID == roomID {
					allowed = true
				}
				if teamRoomID != "" && teamRoomID == roomID {
					allowed = true
				}
				// When both are empty, allow as fallback (bootstrapping).
				if member.RoomID == "" && teamRoomID == "" {
					allowed = true
				}
				if !allowed {
					roomMismatch++
					logger.V(1).Info("mention rejected: room not in allowed set",
						"roomID", roomID, "worker", member.Name,
						"team", team.Name, "workerRoom", member.RoomID,
						"teamRoom", teamRoomID)
					continue
				}
			}

			if !isTeamWorkerRef(&team, member.Name) {
				noSpec++
				logger.V(1).Info("mention skipped: team worker ref not found",
					"worker", member.Name, "team", team.Name)
				continue
			}
			var worker v1beta1.Worker
			if err := h.client.Get(ctx, types.NamespacedName{Name: member.Name, Namespace: h.namespace}, &worker); err != nil {
				noSpec++
				logger.V(1).Info("mention skipped: worker CR not found",
					"worker", member.Name, "team", team.Name)
				continue
			}
			if worker.Spec.DesiredState() != "Sleeping" {
				notSleeping++
				logger.V(1).Info("mention skipped: team worker not Sleeping",
					"worker", member.Name, "team", team.Name,
					"desiredState", worker.Spec.DesiredState())
				continue
			}
			now := h.now().UTC().Format(time.RFC3339)
			if err := h.setStandaloneWorkerRunning(ctx, member.Name, now); err != nil {
				logger.Error(err, "set team worker Running failed",
					"team", team.Name, "worker", member.Name, "room", roomID)
				return err
			}
			wokenUp++
			logger.Info("worker woken by mention",
				"worker", member.Name, "team", team.Name,
				"room", roomID, "type", "team")
		}
	}
	if matchedUser == 0 {
		logger.V(1).Info("no team worker matched mention",
			"mentionedUser", userID, "scannedTeams", len(teams.Items))
	} else if wokenUp == 0 {
		logger.V(1).Info("team mention had no effect",
			"mentionedUser", userID,
			"matched", matchedUser, "notSleeping", notSleeping,
			"roomMismatch", roomMismatch, "noSpec", noSpec)
	}
	return nil
}

// --- CR mutation helpers (mirrored from mention_watcher.go) ---

func (h *AppserviceHandler) setStandaloneWorkerRunning(ctx context.Context, name, lastActiveAt string) error {
	// 逻辑说明：先用 RetryOnConflict 重新读取并仅在仍为 Sleeping 时更新 Spec.State，随后另一次冲突重试单独更新较新的 Status.LastActiveAt；分开 spec/status 写入符合 Kubernetes 子资源版本语义。
	logger := log.FromContext(ctx).WithName("appservice")
	running := "Running"
	specPatched := false
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var worker v1beta1.Worker
		if err := h.client.Get(ctx, types.NamespacedName{Name: name, Namespace: h.namespace}, &worker); err != nil {
			return client.IgnoreNotFound(err)
		}
		if worker.Spec.DesiredState() != "Sleeping" {
			return nil
		}
		worker.Spec.State = &running
		if err := h.client.Update(ctx, &worker); err != nil {
			return err
		}
		specPatched = true
		return nil
	}); err != nil {
		return err
	}
	if specPatched {
		logger.Info("standalone worker spec.state patched to Running by mention",
			"worker", name)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var worker v1beta1.Worker
		if err := h.client.Get(ctx, types.NamespacedName{Name: name, Namespace: h.namespace}, &worker); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !isLastActiveNewer(lastActiveAt, worker.Status.LastActiveAt) {
			return nil
		}
		worker.Status.LastActiveAt = lastActiveAt
		return h.client.Status().Update(ctx, &worker)
	}); err != nil {
		logger.Error(err, "update standalone worker status.lastActiveAt failed (non-fatal)",
			"worker", name)
		return err
	}
	return nil
}

// --- Appservice-local helpers ---

func isTeamWorkerRef(team *v1beta1.Team, name string) bool {
	// 逻辑说明：在期望 WorkerMembers 中确认名称存在且角色为空或 worker；仅出现在 Status 的过期成员不能据此获得唤醒权限。
	for _, ref := range team.Spec.WorkerMembers {
		if ref.Name == name {
			return ref.Role == "" || ref.Role == roleTeamWorker
		}
	}
	return false
}

func isLastActiveNewer(next, current string) bool {
	// 逻辑说明：空的新时间绝不覆盖，当前为空则接受；两者都有值时按 RFC3339 比较，拒绝无效 next，而无效 current 可由有效 next 修复。
	if next == "" {
		return false
	}
	if current == "" {
		return true
	}
	nextTime, err := time.Parse(time.RFC3339, next)
	if err != nil {
		return false
	}
	currentTime, err := time.Parse(time.RFC3339, current)
	if err != nil {
		return true
	}
	return nextTime.After(currentTime)
}
