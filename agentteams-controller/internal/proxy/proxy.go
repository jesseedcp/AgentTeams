package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"regexp"
)

var (
	// URL patterns for POST/DELETE allowlist
	containerAction = regexp.MustCompile(`^(/v[\d.]+)?/containers/[a-zA-Z0-9_.-]+/(start|stop|kill|restart|wait|resize|attach|logs)$`)
	containerExec   = regexp.MustCompile(`^(/v[\d.]+)?/containers/[a-zA-Z0-9_.-]+/exec$`)
	containerCreate = regexp.MustCompile(`^(/v[\d.]+)?/containers/create$`)
	containerDelete = regexp.MustCompile(`^(/v[\d.]+)?/containers/[a-zA-Z0-9_.-]+$`)
	execStart       = regexp.MustCompile(`^(/v[\d.]+)?/exec/[a-zA-Z0-9]+/(start|resize|json)$`)
	imageCreate     = regexp.MustCompile(`^(/v[\d.]+)?/images/create$`)
)

// Handler is a Docker API reverse proxy with security validation.
type Handler struct {
	proxy     *httputil.ReverseProxy
	validator *SecurityValidator
}

// NewHandler creates a Docker API proxy handler that forwards to the given socket.
func NewHandler(socketPath string, validator *SecurityValidator) *Handler {
	// 逻辑说明：创建只通过指定 Unix socket 连接 Docker daemon 的 transport，并让反向代理重写上游 scheme/host；validator 保留在外层，先做授权再转发。
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		},
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = "localhost"
		},
		Transport: transport,
	}

	return &Handler{
		proxy:     proxy,
		validator: validator,
	}
}

// ServeHTTP handles Docker API requests with security filtering.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：GET/HEAD 作为只读请求直接代理；写请求必须同时匹配方法与固定 Docker API 路径白名单，容器创建还进入载荷校验，其余请求记录并返回 403。
	path := r.URL.Path

	// GET/HEAD requests are read-only, always allow
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		h.proxy.ServeHTTP(w, r)
		return
	}

	// POST/DELETE allowlist
	switch {
	case r.Method == http.MethodPost && containerCreate.MatchString(path):
		h.handleContainerCreate(w, r)
		return

	case r.Method == http.MethodPost && containerAction.MatchString(path):
		// start/stop/kill/restart/wait/resize/attach/logs — allow
	case r.Method == http.MethodPost && containerExec.MatchString(path):
		// exec create — allow
	case r.Method == http.MethodPost && execStart.MatchString(path):
		// exec start — allow
	case r.Method == http.MethodPost && imageCreate.MatchString(path):
		// image pull — allow
	case r.Method == http.MethodDelete && containerDelete.MatchString(path):
		// container remove — allow

	default:
		log.Printf("[DENIED] %s %s", r.Method, r.URL.String())
		http.Error(w, fmt.Sprintf(`{"message":"agentteams-controller: %s %s is not allowed"}`, r.Method, path), http.StatusForbidden)
		return
	}

	h.proxy.ServeHTTP(w, r)
}

func (h *Handler) handleContainerCreate(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：完整读取并解析创建载荷，连同 query 中容器名交给安全策略校验；拒绝时返回不含原始敏感载荷的 JSON，允许时重建已消费的 Body 后再代理给 Docker。
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, `{"message":"agentteams-controller: failed to read request body"}`, http.StatusBadRequest)
		return
	}

	containerName := r.URL.Query().Get("name")

	var req ContainerCreateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"message":"agentteams-controller: invalid JSON in request body"}`, http.StatusBadRequest)
		return
	}

	if err := h.validator.ValidateContainerCreate(req, containerName); err != nil {
		log.Printf("[BLOCKED] POST /containers/create name=%s: %s", containerName, err)
		msg, _ := json.Marshal(map[string]string{"message": fmt.Sprintf("agentteams-controller: %s", err)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		w.Write(msg)
		return
	}

	log.Printf("[ALLOWED] POST /containers/create name=%s image=%s", containerName, req.Image)

	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	h.proxy.ServeHTTP(w, r)
}
