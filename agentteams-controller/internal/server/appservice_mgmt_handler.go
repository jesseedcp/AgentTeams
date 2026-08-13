package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
)

// AppServiceHandler handles AppService management endpoints.
type AppServiceHandler struct {
	matrixCfg matrix.Config
}

// NewAppServiceHandler creates an AppServiceHandler.
func NewAppServiceHandler(cfg matrix.Config) *AppServiceHandler {
	// 逻辑说明：保存当前 Matrix 配置作为 token 轮换基线；真正的注册、验证和外部副作用只在 RotateToken 请求中发生。
	return &AppServiceHandler{matrixCfg: cfg}
}

// rotateTokenRequest is the JSON body for POST /api/v1/appservice/rotate-token.
type rotateTokenRequest struct {
	ASToken string `json:"as_token"`
	HSToken string `json:"hs_token"`
}

// RotateToken rotates the Matrix AppService as_token (and optionally hs_token).
// It creates a temporary TuwunelClient with the new token, unregisters the old
// registration (via admin command, which does not require the old as_token),
// registers with the new token, and verifies with a smoke test.
//
// NOTE: This only updates the homeserver registration. The caller must also
// update the controller env var / Secret and restart for the new token to
// take effect permanently.
func (h *AppServiceHandler) RotateToken(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析并校验新 as_token，复制现有配置并可选替换 hs_token；用临时客户端重注册后执行 smoke test，两步都成功才返回提示，持久化 Secret/重启明确留给调用方。
	var req rotateTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	if req.ASToken == "" {
		http.Error(w, "as_token is required", http.StatusBadRequest)
		return
	}

	// Build a new config with the rotated tokens.
	newCfg := h.matrixCfg
	newCfg.AppServiceToken = req.ASToken
	if req.HSToken != "" {
		newCfg.AppServiceHSToken = req.HSToken
	}

	// Create a temporary client with the new token.
	client := matrix.NewTuwunelClient(newCfg, nil)

	ctx := r.Context()

	// Build registration with new tokens and register (includes unregister fallback).
	reg := matrix.RenderAppServiceRegistration(newCfg)
	if err := client.RegisterAppService(ctx, reg); err != nil {
		http.Error(w, fmt.Sprintf("appservice registration failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Verify the new token works.
	if err := client.AppServiceSmokeTest(ctx); err != nil {
		http.Error(w, fmt.Sprintf("smoke test failed after rotation: %v", err), http.StatusInternalServerError)
		return
	}

	resp := map[string]string{
		"message": "appservice token rotated successfully; update your env file / Secret and restart the controller",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
