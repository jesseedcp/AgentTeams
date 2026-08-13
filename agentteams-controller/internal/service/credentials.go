package service

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/auth"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// WorkerCredentials holds persisted credentials for a worker.
// These are generated once on first creation and reused across retries.
type WorkerCredentials struct {
	MatrixPassword string
	MinIOPassword  string
	GatewayKey     string
	// MatrixToken is the access token returned by the most recent matrix.Login.
	// Persisted so that subsequent RefreshManagerCredentials calls can reuse
	// the cached token instead of issuing a fresh login on every controller
	// reconcile. Without this, every reconcile would rotate the runtime
	// credential and tear down the Manager's Matrix sync session. May be empty
	// on first boot or when the old token has been invalidated; callers must
	// re-login in that case.
	MatrixToken string
}

// CredentialStore manages worker credential persistence.
type CredentialStore interface {
	Load(ctx context.Context, workerName string) (*WorkerCredentials, error)
	Save(ctx context.Context, workerName string, creds *WorkerCredentials) error
	Delete(ctx context.Context, workerName string) error
	// List returns the names of all workers/managers with stored credentials.
	List(ctx context.Context) ([]string, error)
}

// FileCredentialStore persists credentials as env files (embedded mode).
// Compatible with the existing /data/worker-creds/{name}.env format.
type FileCredentialStore struct {
	Dir string // e.g. /data/worker-creds
}

func (s *FileCredentialStore) Load(_ context.Context, workerName string) (*WorkerCredentials, error) {
	// 逻辑说明：Load 接收 _(context.Context)、workerName(string)，依次借助 Join、Open、IsNotExist、Close读取凭据记录的期望结果。
	// 返回/状态：返回 *WorkerCredentials、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	path := filepath.Join(s.Dir, workerName+".env")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open credentials file: %w", err)
	}
	defer f.Close()

	creds := &WorkerCredentials{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v := parseEnvLine(line)
		switch k {
		case "WORKER_PASSWORD":
			creds.MatrixPassword = v
		case "WORKER_MINIO_PASSWORD":
			creds.MinIOPassword = v
		case "WORKER_GATEWAY_KEY":
			creds.GatewayKey = v
		case "WORKER_MATRIX_TOKEN":
			creds.MatrixToken = v
		}
	}
	return creds, scanner.Err()
}

func (s *FileCredentialStore) Save(_ context.Context, workerName string, creds *WorkerCredentials) error {
	// 逻辑说明：Save 接收 _(context.Context)、workerName(string)、creds(*WorkerCredentials)，依次借助 MkdirAll、Join、WriteFile保存凭据记录的期望结果。
	// 返回/状态：返回 error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	if err := os.MkdirAll(s.Dir, 0755); err != nil {
		return fmt.Errorf("create credentials dir: %w", err)
	}
	path := filepath.Join(s.Dir, workerName+".env")
	content := fmt.Sprintf(
		"WORKER_PASSWORD=%q\nWORKER_MINIO_PASSWORD=%q\nWORKER_GATEWAY_KEY=%q\nWORKER_MATRIX_TOKEN=%q\n",
		creds.MatrixPassword, creds.MinIOPassword, creds.GatewayKey, creds.MatrixToken,
	)
	return os.WriteFile(path, []byte(content), 0600)
}

func (s *FileCredentialStore) Delete(_ context.Context, workerName string) error {
	// 逻辑说明：Delete 接收 _(context.Context)、workerName(string)，依次借助 Join、Remove、IsNotExist删除凭据记录的期望结果。
	// 返回/状态：返回 error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	path := filepath.Join(s.Dir, workerName+".env")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *FileCredentialStore) List(_ context.Context) ([]string, error) {
	// 逻辑说明：List 接收 _(context.Context)，依次借助 ReadDir、IsNotExist、Name、IsDir列出凭据记录的期望结果。
	// 返回/状态：返回 []string、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read creds dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && strings.HasSuffix(name, ".env") {
			names = append(names, strings.TrimSuffix(name, ".env"))
		}
	}
	return names, nil
}

func parseEnvLine(line string) (string, string) {
	// 逻辑说明：parseEnvLine 接收 line(string)，依次借助 IndexByte、Trim解析凭据记录的期望结果。
	// 返回/状态：返回 string、string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	idx := strings.IndexByte(line, '=')
	if idx < 0 {
		return line, ""
	}
	k := line[:idx]
	v := line[idx+1:]
	v = strings.Trim(v, `"'`)
	return k, v
}

// GenerateCredentials creates a fresh set of worker credentials.
func GenerateCredentials() (*WorkerCredentials, error) {
	// 逻辑说明：GenerateCredentials 接收 无，依次借助 generateRandomHex生成凭据记录的期望结果。
	// 返回/状态：返回 *WorkerCredentials、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	matrixPw, err := generateRandomHex(16)
	if err != nil {
		return nil, fmt.Errorf("generate matrix password: %w", err)
	}
	minioPw, err := generateRandomHex(24)
	if err != nil {
		return nil, fmt.Errorf("generate minio password: %w", err)
	}
	gwKey, err := generateRandomHex(32)
	if err != nil {
		return nil, fmt.Errorf("generate gateway key: %w", err)
	}
	return &WorkerCredentials{
		MatrixPassword: matrixPw,
		MinIOPassword:  minioPw,
		GatewayKey:     gwKey,
	}, nil
}

func generateRandomHex(n int) (string, error) {
	// 逻辑说明：generateRandomHex 接收 n(int)，依次借助 Read、EncodeToString生成凭据记录的期望结果。
	// 返回/状态：返回 string、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// SecretCredentialStore persists credentials as K8s Secrets (incluster mode).
// Secret name: agentteams-creds-{workerName}
type SecretCredentialStore struct {
	Client    kubernetes.Interface
	Namespace string
	// ControllerName identifies this controller instance. Stamped on the
	// credential Secret via agentteams.io/controller so multi-instance
	// deployments sharing a namespace can filter by owner.
	ControllerName string
	// ResourcePrefix is the tenant prefix used to derive the decorative
	// "app" label on the Secret (via WorkerAppLabel()). Empty falls back
	// to auth.DefaultResourcePrefix — keeps the Secret's "app" value
	// aligned with the Pod and ServiceAccount created for the same worker.
	ResourcePrefix auth.ResourcePrefix
}

func (s *SecretCredentialStore) secretName(workerName string) string {
	return "agentteams-creds-" + workerName
}

func (s *SecretCredentialStore) Load(ctx context.Context, workerName string) (*WorkerCredentials, error) {
	// 逻辑说明：Load 接收 ctx(context.Context)、workerName(string)，依次借助 Get、Secrets、CoreV1、secretName读取凭据记录的期望结果。
	// 返回/状态：返回 *WorkerCredentials、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	secret, err := s.Client.CoreV1().Secrets(s.Namespace).Get(ctx, s.secretName(workerName), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get credentials secret: %w", err)
	}
	return &WorkerCredentials{
		MatrixPassword: string(secret.Data["WORKER_PASSWORD"]),
		MinIOPassword:  string(secret.Data["WORKER_MINIO_PASSWORD"]),
		GatewayKey:     string(secret.Data["WORKER_GATEWAY_KEY"]),
		MatrixToken:    string(secret.Data["WORKER_MATRIX_TOKEN"]),
	}, nil
}

func (s *SecretCredentialStore) Save(ctx context.Context, workerName string, creds *WorkerCredentials) error {
	// 逻辑说明：Save 接收 ctx(context.Context)、workerName(string)、creds(*WorkerCredentials)，依次借助 secretName、WorkerAppLabel、Get、Secrets保存凭据记录的期望结果。
	// 返回/状态：返回 error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.secretName(workerName),
			Namespace: s.Namespace,
			Labels: map[string]string{
				"app":                   s.ResourcePrefix.WorkerAppLabel(),
				"agentteams.io/worker":  workerName,
				v1beta1.LabelController: s.ControllerName,
			},
		},
		Data: map[string][]byte{
			"WORKER_PASSWORD":       []byte(creds.MatrixPassword),
			"WORKER_MINIO_PASSWORD": []byte(creds.MinIOPassword),
			"WORKER_GATEWAY_KEY":    []byte(creds.GatewayKey),
			"WORKER_MATRIX_TOKEN":   []byte(creds.MatrixToken),
		},
	}

	existing, err := s.Client.CoreV1().Secrets(s.Namespace).Get(ctx, s.secretName(workerName), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = s.Client.CoreV1().Secrets(s.Namespace).Create(ctx, secret, metav1.CreateOptions{})
			return err
		}
		return fmt.Errorf("get credentials secret: %w", err)
	}
	existing.Data = secret.Data
	existing.Labels = secret.Labels
	_, err = s.Client.CoreV1().Secrets(s.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (s *SecretCredentialStore) Delete(ctx context.Context, workerName string) error {
	// 逻辑说明：Delete 接收 ctx(context.Context)、workerName(string)，依次借助 Delete、Secrets、CoreV1、secretName删除凭据记录的期望结果。
	// 返回/状态：返回 error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	err := s.Client.CoreV1().Secrets(s.Namespace).Delete(ctx, s.secretName(workerName), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (s *SecretCredentialStore) List(ctx context.Context) ([]string, error) {
	// 逻辑说明：List 接收 ctx(context.Context)，依次借助 List、Secrets、CoreV1列出凭据记录的期望结果。
	// 返回/状态：返回 []string、error；可能读写凭据文件或 Kubernetes Secret，敏感内容只经返回值传递，禁止写日志。
	// 失败/重试：随机源、文件系统或 Secret API 失败会返回错误；调用方重新读取现状后可安全重试。
	secrets, err := s.Client.CoreV1().Secrets(s.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: v1beta1.LabelController + "=" + s.ControllerName,
	})
	if err != nil {
		return nil, fmt.Errorf("list credential secrets: %w", err)
	}
	var names []string
	for _, sec := range secrets.Items {
		if name, ok := sec.Labels["agentteams.io/worker"]; ok && name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}
