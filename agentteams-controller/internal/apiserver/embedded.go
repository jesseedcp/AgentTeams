package apiserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

// Config holds embedded kube-apiserver configuration.
type Config struct {
	DataDir    string // directory for certs, tokens, etc.
	EtcdURL    string // kine endpoint (default: http://127.0.0.1:2379)
	BindAddr   string // apiserver listen address (default: 127.0.0.1)
	SecurePort string // apiserver port (default: 6443)
	CRDDir     string // directory containing CRD YAML files
}

// EmbeddedAPIServer manages a kube-apiserver subprocess.
type EmbeddedAPIServer struct {
	config  Config
	cmd     *exec.Cmd
	certDir string
}

// Start launches kube-apiserver as a subprocess and waits for it to be ready.
// Returns a rest.Config that can be used by controller-runtime.
func Start(ctx context.Context, cfg Config) (*rest.Config, error) {
	// 逻辑说明：补齐嵌入式默认值，持久化证书/管理员 token，启动 kube-apiserver，等健康后注册 CRD 并返回认证配置。
	logger := log.FromContext(ctx).WithName("embedded-apiserver")

	if cfg.EtcdURL == "" {
		cfg.EtcdURL = "http://127.0.0.1:2379"
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1"
	}
	if cfg.SecurePort == "" {
		cfg.SecurePort = "6443"
	}
	if cfg.CRDDir == "" {
		cfg.CRDDir = "/opt/agentteams/config/crd"
	}

	certDir := filepath.Join(cfg.DataDir, "pki")
	if err := os.MkdirAll(certDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cert dir: %w", err)
	}

	// Generate self-signed certs if not present
	caCertFile := filepath.Join(certDir, "ca.crt")
	servingCertFile := filepath.Join(certDir, "apiserver.crt")
	servingKeyFile := filepath.Join(certDir, "apiserver.key")
	saKeyFile := filepath.Join(certDir, "sa.key")
	saPubFile := filepath.Join(certDir, "sa.pub")

	if _, err := os.Stat(caCertFile); os.IsNotExist(err) {
		logger.Info("generating self-signed certificates")
		if err := generateCerts(certDir, cfg.BindAddr); err != nil {
			return nil, fmt.Errorf("failed to generate certs: %w", err)
		}
	}

	// Create static token file for authentication.
	// Token is randomly generated on first start and persisted to dataDir for reuse across restarts.
	tokenFile := filepath.Join(certDir, "token.csv")
	adminToken, err := generateOrLoadAdminToken(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to generate admin token: %w", err)
	}
	if _, err := os.Stat(tokenFile); os.IsNotExist(err) {
		// Format: token,user,uid,"group1,group2"
		if err := os.WriteFile(tokenFile, []byte(adminToken+",admin,admin,\"system:masters\"\n"), 0600); err != nil {
			return nil, fmt.Errorf("failed to create token file: %w", err)
		}
	}

	// Start kube-apiserver
	args := []string{
		"--etcd-servers=" + cfg.EtcdURL,
		"--bind-address=" + cfg.BindAddr,
		"--secure-port=" + cfg.SecurePort,
		"--tls-cert-file=" + servingCertFile,
		"--tls-private-key-file=" + servingKeyFile,
		"--client-ca-file=" + caCertFile,
		"--service-account-key-file=" + saPubFile,
		"--service-account-signing-key-file=" + saKeyFile,
		"--service-account-issuer=https://agentteams.local",
		"--token-auth-file=" + tokenFile,
		"--authorization-mode=AlwaysAllow",
		"--anonymous-auth=true",
		"--disable-admission-plugins=ServiceAccount",
		"--etcd-prefix=/registry",
		"--v=0",
	}

	cmd := exec.CommandContext(ctx, "kube-apiserver", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Info("starting kube-apiserver", "etcd", cfg.EtcdURL, "port", cfg.SecurePort)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start kube-apiserver: %w", err)
	}

	// Build rest.Config with token auth
	restCfg := &rest.Config{
		Host:        fmt.Sprintf("https://%s:%s", cfg.BindAddr, cfg.SecurePort),
		BearerToken: adminToken,
		TLSClientConfig: rest.TLSClientConfig{
			CAFile: caCertFile,
		},
	}

	// Wait for apiserver to be ready
	logger.Info("waiting for kube-apiserver to be ready...")
	if err := waitForReady(ctx, restCfg); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("kube-apiserver failed to become ready: %w", err)
	}
	logger.Info("kube-apiserver is ready")

	// Register CRDs
	if cfg.CRDDir != "" {
		if err := registerCRDs(ctx, restCfg, cfg.CRDDir); err != nil {
			logger.Error(err, "failed to register CRDs (non-fatal, will retry)")
		}
	}

	return restCfg, nil
}

// waitForReady polls the apiserver until it responds to HTTP requests.
// Any HTTP response (including 401) means the server is up and ready.
func waitForReady(ctx context.Context, cfg *rest.Config) error {
	// 逻辑说明：用生成的 CA 构建受信 HTTP client，在 60 秒内轮询 /healthz；任何 HTTP 响应表示进程已监听。
	// Build a simple HTTP client with the CA cert
	caCert, err := os.ReadFile(cfg.TLSClientConfig.CAFile)
	if err != nil {
		return fmt.Errorf("failed to read CA cert: %w", err)
	}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
		Timeout: 5 * time.Second,
	}

	healthURL := cfg.Host + "/healthz"

	return wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := httpClient.Get(healthURL)
		if err != nil {
			return false, nil // connection refused, not ready yet
		}
		defer resp.Body.Close()
		// Any HTTP response means the server is up (even 401 from anonymous auth)
		return true, nil
	})
}

// registerCRDs reads CRD YAML files from a directory and applies them.
func registerCRDs(ctx context.Context, cfg *rest.Config, crdDir string) error {
	// 逻辑说明：遍历 YAML CRD，存在则保留 resourceVersion 更新，否则创建；单文件失败记录后继续，最后等待全部 Established。
	logger := log.FromContext(ctx).WithName("crd-register")

	client, err := apiextensionsclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to create apiextensions client: %w", err)
	}

	entries, err := os.ReadDir(crdDir)
	if err != nil {
		return fmt.Errorf("failed to read CRD dir %s: %w", crdDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".yaml" && filepath.Ext(name) != ".yml" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(crdDir, name))
		if err != nil {
			logger.Error(err, "failed to read CRD file", "file", name)
			continue
		}

		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			logger.Error(err, "failed to parse CRD", "file", name)
			continue
		}

		existing, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err == nil {
			// Update existing CRD
			crd.ResourceVersion = existing.ResourceVersion
			if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, &crd, metav1.UpdateOptions{}); err != nil {
				logger.Error(err, "failed to update CRD", "name", crd.Name)
			} else {
				logger.Info("CRD updated", "name", crd.Name)
			}
		} else {
			// Create new CRD
			if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &crd, metav1.CreateOptions{}); err != nil {
				logger.Error(err, "failed to create CRD", "name", crd.Name)
			} else {
				logger.Info("CRD registered", "name", crd.Name)
			}
		}
	}

	// Wait for CRDs to be established
	return waitForCRDsEstablished(ctx, client, []string{
		"workers.agentteams.io",
		"teams.agentteams.io",
		"humans.agentteams.io",
		"managers.agentteams.io",
	})
}

func waitForCRDsEstablished(ctx context.Context, client apiextensionsclient.Interface, names []string) error {
	// 逻辑说明：在限定时间内逐个读取目标 CRD，只有每个都有 Established=True 才返回成功，瞬时缺失继续等待。
	return wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, name := range names {
			crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			established := false
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
					established = true
					break
				}
			}
			if !established {
				return false, nil
			}
		}
		return true, nil
	})
}

// generateCerts creates a self-signed CA and serving cert for the embedded apiserver.
func generateCerts(certDir, bindAddr string) error {
	// 逻辑说明：生成自签 CA、服务端 ECDSA 证书和 SA 签名密钥，并以受限权限分别写入指定 PKI 目录。
	// Generate CA key
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "agentteams-ca",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return err
	}

	// Generate serving key
	servingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	servingTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "kube-apiserver",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP(bindAddr)},
		DNSNames:    []string{"localhost", "kube-apiserver"},
	}

	servingCertDER, err := x509.CreateCertificate(rand.Reader, servingTemplate, caCert, &servingKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Generate service account key pair
	saKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Write files
	if err := writePEM(filepath.Join(certDir, "ca.crt"), "CERTIFICATE", caCertDER); err != nil {
		return err
	}
	if err := writeECKey(filepath.Join(certDir, "ca.key"), caKey); err != nil {
		return err
	}
	if err := writePEM(filepath.Join(certDir, "apiserver.crt"), "CERTIFICATE", servingCertDER); err != nil {
		return err
	}
	if err := writeECKey(filepath.Join(certDir, "apiserver.key"), servingKey); err != nil {
		return err
	}
	if err := writeECKey(filepath.Join(certDir, "sa.key"), saKey); err != nil {
		return err
	}
	if err := writeECPubKey(filepath.Join(certDir, "sa.pub"), &saKey.PublicKey); err != nil {
		return err
	}

	return nil
}

func writePEM(path, pemType string, data []byte) error {
	// 逻辑说明：以 0600 创建或截断目标文件，编码单个 PEM block，并通过 defer 保证描述符关闭。
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: pemType, Bytes: data})
}

func writeECKey(path string, key *ecdsa.PrivateKey) error {
	// 逻辑说明：先按 SEC1 编码 EC 私钥，再复用受限 PEM 写入；编码失败不会留下目标内容。
	data, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return writePEM(path, "EC PRIVATE KEY", data)
}

func writeECPubKey(path string, key *ecdsa.PublicKey) error {
	// 逻辑说明：将公钥编码为 PKIX 格式后复用 PEM 写入，供 kube-apiserver 验证 SA token。
	data, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return err
	}
	return writePEM(path, "PUBLIC KEY", data)
}

// generateOrLoadAdminToken returns a per-instance admin token.
// On first invocation it generates a cryptographically random 64-char hex token
// and persists it to dataDir/admin-token. Subsequent calls (including after
// container restart) reuse the persisted value.
func generateOrLoadAdminToken(dataDir string) (string, error) {
	// 逻辑说明：优先复用非空持久 token；否则用 crypto/rand 生成 256 位随机值，并以 0600 原子边界持久化供重启复用。
	tokenPath := filepath.Join(dataDir, "admin-token")
	if data, err := os.ReadFile(tokenPath); err == nil {
		token := strings.TrimSpace(string(data))
		if len(token) > 0 {
			return token, nil
		}
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	token := hex.EncodeToString(b)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dataDir, err)
	}
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0600); err != nil {
		return "", fmt.Errorf("write %s: %w", tokenPath, err)
	}
	return token, nil
}
