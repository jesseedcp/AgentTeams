package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/metrics"
)

func withControllerHTTPMetrics(next http.Handler) http.Handler {
	// 逻辑说明：为每个请求包装 status recorder 并记录开始时间，handler 完成后用 mux 路由模板而非原始 URL 上报状态和耗时，避免资源名造成高基数指标。
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		metrics.ObserveControllerHTTP(r.Method, routePattern(r), start, rec.statusCode)
	})
}

func routePattern(r *http.Request) string {
	// 逻辑说明：读取 Go ServeMux 匹配后的 Pattern，未匹配归入 unmatched；匹配时去掉方法前缀，只保留稳定路径模板作为 metric 标签。
	pattern := r.Pattern
	if pattern == "" {
		return "unmatched"
	}
	prefix := r.Method + " "
	if strings.HasPrefix(pattern, prefix) {
		return strings.TrimPrefix(pattern, prefix)
	}
	return pattern
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	// 逻辑说明：在把 header 写给真实 ResponseWriter 前保存最终显式状态码；未调用本方法的 handler 由构造时默认 200 覆盖。
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
