package server

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
)

// PackageHandler handles ZIP package uploads to OSS.
type PackageHandler struct {
	oss oss.StorageClient
}

func NewPackageHandler(ossClient oss.StorageClient) *PackageHandler {
	// 逻辑说明：注入对象存储接口而不立即访问外部服务；Upload 会在请求时验证 client 是否配置并处理实际上传。
	return &PackageHandler{oss: ossClient}
}

// Upload handles POST /api/v1/packages.
// Accepts multipart/form-data with fields:
//   - file: ZIP binary
//   - name: resource name (used in the storage key)
//
// Returns {"packageUri": "oss://agentteams-config/packages/{name}-{hash}.zip"}
func (h *PackageHandler) Upload(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：要求 OSS 可用并以 64 MiB 上限解析 multipart，校验 name/file 后完整读取内容；用 SHA-256 前缀生成内容寻址键并上传，只有存储成功才返回 oss:// URI。
	if h.oss == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "OSS client not configured")
		return
	}

	const maxUpload = 64 << 20 // 64 MB
	if err := r.ParseMultipartForm(maxUpload); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "parse multipart form: "+err.Error())
		return
	}

	name := r.FormValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name field is required")
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "file field is required: "+err.Error())
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "read uploaded file: "+err.Error())
		return
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(data))[:16]
	ossKey := fmt.Sprintf("agentteams-config/packages/%s-%s.zip", name, hash)

	if err := h.oss.PutObject(r.Context(), ossKey, data); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "upload to OSS: "+err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"packageUri": "oss://" + ossKey,
	})
}
