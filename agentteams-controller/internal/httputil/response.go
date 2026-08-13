package httputil

import (
	"encoding/json"
	"log"
	"net/http"
)

// ErrorResponse is the standard JSON error response.
type ErrorResponse struct {
	Message string `json:"message"`
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	// 逻辑说明：先固定 JSON Content-Type 和 HTTP 状态，再编码响应对象；头部写出后编码失败已无法改状态码，因此只记录警告供服务端排障。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[WARN] failed to write JSON response: %v", err)
	}
}

// WriteError writes a JSON error response.
func WriteError(w http.ResponseWriter, status int, message string) {
	// 逻辑说明：把错误文本包成统一 JSON 结构并写入指定 HTTP 状态码；编码或底层写入失败由 WriteJSON 的既定响应策略处理。
	WriteJSON(w, status, ErrorResponse{Message: message})
}
