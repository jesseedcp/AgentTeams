package backend

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	authenticationv1client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
)

const defaultK8sNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// K8sConfig holds Kubernetes backend configuration.
type K8sConfig struct {
	Namespace          string
	ManagerImage       string
	ManagerDataClaim   string
	ManagerHostPath    string
	WorkerImage        string
	CopawWorkerImage   string
	HermesWorkerImage  string
	QwenPawWorkerImage string
	WorkerCPU          string
	WorkerMemory       string

	// ControllerName identifies this controller instance. The agent
	// PodTemplateSpec overlay (see LoadAgentPodTemplate) is looked up as the
	// ConfigMap named exactly ControllerName in the controller's own
	// Namespace, with key "pod-template.yaml". Empty ControllerName, a
	// missing ConfigMap, or any API / parse error all collapse to "no
	// overlay" (Pod creation proceeds unchanged).
	ControllerName string

	// ResourcePrefix is the tenant prefix used to derive worker "app" label
	// values and default SA names. Empty falls back to "agentteams-" for tests
	// and out-of-cluster callers. See internal/auth.ResourcePrefix for
	// semantics.
	ResourcePrefix string
}

// K8sBackend manages worker lifecycle via Kubernetes Pods.
type K8sBackend struct {
	client          K8sCoreClient
	config          K8sConfig
	containerPrefix string

	// scheme is used to resolve GVK for CreateRequest.Owner when stamping
	// the child Pod's controller OwnerReference via
	// controllerutil.SetControllerReference. A nil scheme means "callers
	// never supply Owner" — typical for unit tests that don't exercise
	// ownerRef behaviour.
	scheme *runtime.Scheme

	// namespace is a convenience alias for config.Namespace used by
	// resolveClient to return the local namespace.
	namespace string
}

// K8sServiceAccountClient is the minimal ServiceAccount client surface needed.
type K8sServiceAccountClient interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.ServiceAccount, error)
	Create(ctx context.Context, sa *corev1.ServiceAccount, opts metav1.CreateOptions) (*corev1.ServiceAccount, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
}

// K8sTokenReviewClient is the minimal TokenReview client surface needed for authentication.
type K8sTokenReviewClient interface {
	Create(ctx context.Context, review *authenticationv1.TokenReview, opts metav1.CreateOptions) (*authenticationv1.TokenReview, error)
}

// K8sCoreClient is the minimal CoreV1 client surface needed by the backend.
type K8sCoreClient interface {
	Pods(namespace string) K8sPodClient
	ConfigMaps(namespace string) K8sConfigMapClient
	Services(namespace string) K8sServiceClient
	Namespaces() K8sNamespaceClient
	ServiceAccounts(namespace string) K8sServiceAccountClient
	TokenReviews() K8sTokenReviewClient
}

// K8sPodClient is the minimal Pod client surface needed by the backend.
type K8sPodClient interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error)
	Create(ctx context.Context, pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
}

// K8sConfigMapClient is the minimal ConfigMap client surface needed by the
// backend. Only Get is exposed — ConfigMaps are consumed read-only for the
// agent pod template.
type K8sConfigMapClient interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.ConfigMap, error)
}

// k8sCoreClientWrapper adapts *corev1client.CoreV1Client to K8sCoreClient.
type k8sCoreClientWrapper struct {
	client     *corev1client.CoreV1Client
	authClient *authenticationv1client.AuthenticationV1Client
}

func (w *k8sCoreClientWrapper) Pods(namespace string) K8sPodClient {
	return w.client.Pods(namespace)
}

func (w *k8sCoreClientWrapper) ConfigMaps(namespace string) K8sConfigMapClient {
	return w.client.ConfigMaps(namespace)
}

func (w *k8sCoreClientWrapper) Services(namespace string) K8sServiceClient {
	return w.client.Services(namespace)
}

func (w *k8sCoreClientWrapper) Namespaces() K8sNamespaceClient {
	return w.client.Namespaces()
}

func (w *k8sCoreClientWrapper) ServiceAccounts(namespace string) K8sServiceAccountClient {
	return w.client.ServiceAccounts(namespace)
}

func (w *k8sCoreClientWrapper) TokenReviews() K8sTokenReviewClient {
	return w.authClient.TokenReviews()
}

// NewK8sBackend creates a Kubernetes backend using in-cluster config or kubeconfig.
// scheme is used by Create to stamp CR-to-Pod controller OwnerReferences
// (see CreateRequest.Owner); it must have all CR kinds that might appear as
// Owner registered.
func NewK8sBackend(config K8sConfig, containerPrefix string, scheme *runtime.Scheme) (*K8sBackend, error) {
	return NewK8sBackendWithCache(config, containerPrefix, scheme, nil)
}

// NewK8sBackendWithCache creates a Kubernetes backend using in-cluster config
// or kubeconfig. The remoteCache argument is retained only for call-site
// compatibility; OSS controllers no longer route backend operations to target
// clusters.
func NewK8sBackendWithCache(config K8sConfig, containerPrefix string, scheme *runtime.Scheme, remoteCache RemoteClientProvider) (*K8sBackend, error) {
	// 逻辑说明：依次加载集群连接配置、构造 CoreV1 与 AuthenticationV1 客户端，再交给可注入客户端的构造器补默认值；任何客户端创建失败都带阶段返回，保留的 remoteCache 参数不参与开源版路由。
	restConfig, err := loadK8sRESTConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := corev1client.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	authClient, err := authenticationv1client.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create authentication client: %w", err)
	}
	return NewK8sBackendWithClient(&k8sCoreClientWrapper{client: clientset, authClient: authClient}, config, containerPrefix, scheme), nil
}

// NewK8sBackendWithClient creates a Kubernetes backend with a custom client.
// scheme may be nil in tests that don't set CreateRequest.Owner.
func NewK8sBackendWithClient(client K8sCoreClient, config K8sConfig, containerPrefix string, scheme *runtime.Scheme) *K8sBackend {
	// 逻辑说明：为未设置的 namespace、CPU 与内存补部署安全默认值，并保存调用方注入的客户端和 scheme；只构造内存对象，便于测试使用 fake client。
	if config.Namespace == "" {
		config.Namespace = detectK8sNamespace()
	}
	if config.WorkerCPU == "" {
		config.WorkerCPU = "1000m"
	}
	if config.WorkerMemory == "" {
		config.WorkerMemory = "2Gi"
	}
	return &K8sBackend{
		client:          client,
		config:          config,
		containerPrefix: containerPrefix,
		scheme:          scheme,
		namespace:       config.Namespace,
	}
}

// WithPrefix returns a shallow copy of the backend with a different container name prefix.
// The returned backend shares the same client (safe — K8sCoreClient is stateless).
// Use WithPrefix("") to disable prefix for containers that already have full names
// (e.g. Manager containers named "agentteams-manager" rather than "agentteams-worker-X").
func (k *K8sBackend) WithPrefix(prefix string) *K8sBackend {
	// 逻辑说明：浅拷贝 backend 并替换 Pod 名前缀，继续共享无状态 Kubernetes client；原实例保持不变，Manager 可借此使用完整固定名称。
	cp := *k
	cp.containerPrefix = prefix
	return &cp
}

func (k *K8sBackend) resolveClient(ctx context.Context) (K8sCoreClient, string, error) {
	return k.client, k.namespace, nil
}

// ServiceClient implements ServiceBackend.
func (k *K8sBackend) ServiceClient(ctx context.Context) (K8sServiceClient, string, error) {
	// 逻辑说明：通过统一解析入口取得当前 Core client 与 namespace，再返回该 namespace 的 Service 子客户端；解析失败不返回半有效客户端。
	client, ns, err := k.resolveClient(ctx)
	if err != nil {
		return nil, "", err
	}
	return client.Services(ns), ns, nil
}

func (k *K8sBackend) Name() string                   { return "k8s" }
func (k *K8sBackend) DeploymentMode() string         { return DeployCloud }
func (k *K8sBackend) NeedsCredentialInjection() bool { return true }

func (k *K8sBackend) Available(_ context.Context) bool {
	return k.client != nil && k.config.Namespace != ""
}

// Create 根据已解析的 CreateRequest 构造并提交 Agent Pod。
//
// Pod 通过 controller OwnerReference 连到 Worker/Team/Manager CR。OwnerReference
// 是 Kubernetes 的级联回收关系：所有者最终删除后，garbage collector
// 会删除子 Pod。它不能取代 finalizer，因为 Matrix 房间和 Higress 路由
// 不是 Kubernetes 对象，仍需 Controller 主动清理。
//
// 方法先 Get 稳定 pod name 防止重复创建。这不会消除所有竞态，因为
// 两个调用可能同时看到 NotFound；Kubernetes 的名称唯一性作为最终保护，
// 上层在 AlreadyExists/超时后应通过 Status 查询现状。
func (k *K8sBackend) Create(ctx context.Context, req CreateRequest) (*WorkerResult, error) {
	// 逻辑说明：解析 runtime/命名/镜像与目标 namespace，拒绝重名 Pod，合并模板、资源、认证 token、HostPath 和 worker-deps 后设置 OwnerReference 并创建；冲突映射领域错误，成功先返回 Starting 等待后续状态收敛。
	// Resolve effective runtime once: explicit > caller fallback > openclaw.
	// See ResolveRuntime godoc — the Worker / Manager CRDs intentionally have
	// no schema-level default, so the only place the operator-side env var can
	// take effect is here, via the caller-provided RuntimeFallback (which the
	// reconciler picks per-resource: AGENTTEAMS_MANAGER_RUNTIME for managers,
	// AGENTTEAMS_DEFAULT_WORKER_RUNTIME for workers).
	req.Runtime = ResolveRuntime(req.Runtime, req.RuntimeFallback)

	targetClient, targetNS, err := k.resolveClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve client for create: %w", err)
	}

	podName := req.ContainerName
	if podName == "" {
		podName = k.podName(req.NamePrefix, req.Name)
	}
	if _, err := targetClient.Pods(targetNS).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		return nil, fmt.Errorf("%w: pod %q", ErrConflict, podName)
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("kubernetes get pod %s: %w", podName, err)
	}

	if req.Env == nil {
		req.Env = make(map[string]string)
	}
	mergeOSSRegionFromProcessEnv(req.Env)
	if rt := firstNonEmptyTrimmed(os.Getenv("AGENTTEAMS_RUNTIME")); rt != "" {
		req.Env["AGENTTEAMS_RUNTIME"] = rt
	} else {
		req.Env["AGENTTEAMS_RUNTIME"] = "k8s"
	}
	if req.ControllerURL != "" {
		req.Env["AGENTTEAMS_CONTROLLER_URL"] = req.ControllerURL
	}
	// SA token is mounted via projected volume; tell the worker where to read it.
	req.Env["AGENTTEAMS_AUTH_TOKEN_FILE"] = "/var/run/secrets/agentteams/token"

	image := req.Image
	if image == "" {
		switch {
		case req.Runtime == RuntimeAgentScope && k.config.ManagerImage != "":
			image = k.config.ManagerImage
		case req.Runtime == RuntimeCopaw && k.config.CopawWorkerImage != "":
			image = k.config.CopawWorkerImage
		case req.Runtime == RuntimeHermes && k.config.HermesWorkerImage != "":
			image = k.config.HermesWorkerImage
		case req.Runtime == RuntimeQwenPaw && k.config.QwenPawWorkerImage != "":
			image = k.config.QwenPawWorkerImage
		case k.config.WorkerImage != "":
			image = k.config.WorkerImage
		}
	}
	if image == "" {
		return nil, fmt.Errorf(
			"no image configured for %s runtime on kubernetes backend",
			req.Runtime,
		)
	}

	if req.WorkingDir == "" {
		switch {
		case req.Runtime == RuntimeCopaw:
			req.WorkingDir = fmt.Sprintf("/root/agentteams-fs/agents/%s", req.Name)
			if req.Env == nil {
				req.Env = map[string]string{}
			}
			req.Env["HOME"] = req.WorkingDir
		default:
			// Both openclaw and hermes use the same workspace layout:
			// HOME == WorkingDir == /root/agentteams-fs/agents/<name> (== MinIO
			// mirror root). The hermes entrypoint anchors its install_dir to
			// the same location so workspace_dir == HOME and HERMES_HOME ==
			// $HOME/.hermes.
			if home := req.Env["HOME"]; home != "" {
				req.WorkingDir = home
			} else {
				req.WorkingDir = fmt.Sprintf("/root/agentteams-fs/agents/%s", req.Name)
				req.Env["HOME"] = req.WorkingDir
			}
		}
	}

	defaultResources := buildDefaultResources(k.config.WorkerCPU, k.config.WorkerMemory)
	var resourcesOverride *corev1.ResourceRequirements
	if req.Resources != nil {
		merged := mergeResourceOverrides(defaultResources, req.Resources)
		resourcesOverride = &merged
	}

	agentContainer := corev1.Container{
		Name:            "worker",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env:             buildK8sEnvVars(req.Env),
		WorkingDir:      req.WorkingDir,
	}

	tokenAudience := req.AuthAudience
	if tokenAudience == "" {
		tokenAudience = "agentteams-controller"
	}
	tokenExpSeconds := NormalizeAuthTokenExpirationSeconds(req.AuthExpirationSeconds)
	tokenVolume := corev1.Volume{
		Name: "agentteams-token",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Audience:          tokenAudience,
						ExpirationSeconds: &tokenExpSeconds,
						Path:              "token",
					},
				}},
			},
		},
	}
	tokenVolumeMount := corev1.VolumeMount{
		Name:      "agentteams-token",
		MountPath: "/var/run/secrets/agentteams",
		ReadOnly:  true,
	}
	extraVolumes, extraVolumeMounts := podWorkerDepsVolumes(req.WorkersDeps)
	hostVolumes, hostVolumeMounts, err := podHostPathVolumes(req.Volumes)
	if err != nil {
		return nil, err
	}
	extraVolumes = append(extraVolumes, hostVolumes...)
	extraVolumeMounts = append(extraVolumeMounts, hostVolumeMounts...)
	if req.Runtime == RuntimeAgentScope {
		dataVolume := corev1.Volume{
			Name: "manager-data",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}
		if k.config.ManagerDataClaim != "" {
			dataVolume.VolumeSource = corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: k.config.ManagerDataClaim,
				},
			}
		}
		extraVolumes = append(extraVolumes, dataVolume)
		extraVolumeMounts = append(extraVolumeMounts, corev1.VolumeMount{
			Name:      "manager-data",
			MountPath: "/var/lib/agentteams-manager",
		})
		if k.config.ManagerHostPath != "" {
			hostPathType := corev1.HostPathDirectory
			extraVolumes = append(extraVolumes, corev1.Volume{
				Name: "manager-host-share",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: k.config.ManagerHostPath,
						Type: &hostPathType,
					},
				},
			})
			extraVolumeMounts = append(
				extraVolumeMounts,
				corev1.VolumeMount{
					Name:      "manager-host-share",
					MountPath: "/host-share",
				},
			)
		}
	}

	saName := req.ServiceAccountName
	if saName == "" {
		saName = k.workerNamePrefix() + req.Name
	}

	// Callers own the full label set except agentteams.io/runtime, which the
	// backend stamps because it knows the resolved runtime value (after
	// CRD spec + operator-default fallback).
	podLabels := map[string]string{
		v1beta1.LabelRuntime: defaultRuntime(req.Runtime),
	}
	for k, v := range req.Labels {
		podLabels[k] = v
	}

	tmpl := LoadAgentPodTemplate(ctx, k.client, k.config.Namespace, k.config.ControllerName, req.DeployMode)

	pod := ApplyPodTemplate(tmpl, PodOverlay{
		Name:               podName,
		Namespace:          targetNS,
		Labels:             podLabels,
		Annotations:        nil,
		ServiceAccountName: saName,
		Container:          agentContainer,
		ResourcesOverride:  resourcesOverride,
		DefaultResources:   defaultResources,
		TokenVolume:        tokenVolume,
		TokenVolumeMount:   tokenVolumeMount,
		ExtraVolumes:       extraVolumes,
		ExtraVolumeMounts:  extraVolumeMounts,
		HostAliases:        buildHostAliases(req.ExtraHosts),
	})
	if req.Runtime == RuntimeAgentScope {
		configureAgentScopeManagerHealth(&pod.Spec.Containers[0])
	}

	if req.Owner != nil {
		if k.scheme == nil {
			return nil, fmt.Errorf("kubernetes backend: scheme is required when CreateRequest.Owner is set")
		}
		if err := controllerutil.SetControllerReference(req.Owner, pod, k.scheme); err != nil {
			return nil, fmt.Errorf("set owner reference on pod %s: %w", podName, err)
		}
	}

	created, err := targetClient.Pods(targetNS).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("%w: pod %q", ErrConflict, podName)
		}
		return nil, fmt.Errorf("kubernetes create pod %s: %w", podName, err)
	}

	return &WorkerResult{
		Name:      req.Name,
		Backend:   "k8s",
		Status:    StatusStarting,
		RawStatus: rawK8sPhase(created.Status.Phase),
	}, nil
}

func configureAgentScopeManagerHealth(container *corev1.Container) {
	// 逻辑说明：移除与 Manager 健康端口同名或同端口的旧声明后追加唯一 18799 端口，并强制设置 `/healthz` 与 `/readyz` 探针，避免模板中的冲突端口导致 Pod 无法创建。
	const (
		portName = "manager-health"
		port     = 18799
	)

	ports := make([]corev1.ContainerPort, 0, len(container.Ports)+1)
	for _, existing := range container.Ports {
		if existing.Name == portName || existing.ContainerPort == port {
			continue
		}
		ports = append(ports, existing)
	}
	container.Ports = append(ports, corev1.ContainerPort{
		Name:          portName,
		ContainerPort: port,
	})
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromString(portName),
			},
		},
	}
	container.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intstr.FromString(portName),
			},
		},
	}
}

func (k *K8sBackend) Delete(ctx context.Context, name string) error {
	// 逻辑说明：解析 client/namespace 后删除精确 Worker Pod；NotFound 作为幂等成功，其他 API 错误带 Pod 名返回，不触碰同标签的其他资源。
	targetClient, targetNS, err := k.resolveClient(ctx)
	if err != nil {
		return fmt.Errorf("resolve client for delete: %w", err)
	}
	podName := k.workerPodName(name)
	err = targetClient.Pods(targetNS).Delete(ctx, podName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("kubernetes delete pod %s: %w", podName, err)
	}
	return nil
}

func (k *K8sBackend) Start(ctx context.Context, name string) error {
	// 逻辑说明：读取目标 Pod 并检查 phase；Running/Pending 视为已启动或正在启动，缺失映射 ErrNotFound，终止态不能原地重启而要求上层重建。
	targetClient, targetNS, err := k.resolveClient(ctx)
	if err != nil {
		return fmt.Errorf("resolve client for start: %w", err)
	}
	pod, err := targetClient.Pods(targetNS).Get(ctx, k.workerPodName(name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: worker %q", ErrNotFound, name)
	}
	if err != nil {
		return fmt.Errorf("kubernetes get pod %s: %w", k.workerPodName(name), err)
	}

	switch pod.Status.Phase {
	case corev1.PodRunning, corev1.PodPending:
		return nil
	default:
		return fmt.Errorf("kubernetes worker %q cannot be started from phase %q; recreate it instead", name, pod.Status.Phase)
	}
}

func (k *K8sBackend) Stop(ctx context.Context, name string) error {
	// 逻辑说明：Kubernetes 没有独立的“停止但保留”语义，因此把停止请求委托给删除流程，并原样返回资源清理失败。
	return k.Delete(ctx, name)
}

func podHostPathVolumes(
	mounts []VolumeMount,
) ([]corev1.Volume, []corev1.VolumeMount, error) {
	// 逻辑说明：逐项验证宿主与容器路径均为绝对路径，再按索引生成稳定 HostPath volume/mount；任一非法项立即失败且不返回部分列表。
	volumes := make([]corev1.Volume, 0, len(mounts))
	volumeMounts := make([]corev1.VolumeMount, 0, len(mounts))
	for index, mount := range mounts {
		if !strings.HasPrefix(mount.HostPath, "/") {
			return nil, nil, fmt.Errorf(
				"kubernetes host volume %d host path must be absolute",
				index,
			)
		}
		if !strings.HasPrefix(mount.ContainerPath, "/") {
			return nil, nil, fmt.Errorf(
				"kubernetes host volume %d container path must be absolute",
				index,
			)
		}
		name := fmt.Sprintf("agentteams-host-%d", index)
		hostPathType := corev1.HostPathDirectory
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: mount.HostPath,
					Type: &hostPathType,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      name,
			MountPath: mount.ContainerPath,
			ReadOnly:  mount.ReadOnly,
		})
	}
	return volumes, volumeMounts, nil
}

func (k *K8sBackend) Status(ctx context.Context, name string) (*WorkerResult, error) {
	// 逻辑说明：读取 Pod 后先映射 phase，再优先用 init/主容器等待或退出原因识别真实失败，最后结合 Ready 条件区分预热与故障；NotFound 返回状态对象，API 错误才返回 error。
	targetClient, targetNS, err := k.resolveClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve client for status: %w", err)
	}
	pod, err := targetClient.Pods(targetNS).Get(ctx, k.workerPodName(name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &WorkerResult{Name: name, Backend: "k8s", Status: StatusNotFound}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("kubernetes get pod %s: %w", k.workerPodName(name), err)
	}
	status := normalizeK8sPodPhase(pod.Status.Phase)
	var message string
	rawStatus := rawK8sPhase(pod.Status.Phase)

	// Container waiting/terminated states carry the real failure reason for
	// cases such as ImagePullBackOff while the Pod phase is still Pending.
	if containerStatus, containerMessage, containerRaw, ok := podContainerFailureStatus(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses); ok {
		status = containerStatus
		message = containerMessage
		rawStatus = containerRaw
	} else if status == StatusRunning {
		// When phase maps to Running, additionally check the Ready condition.
		// A pod can have phase Running but Ready=False (e.g. CrashLoopBackOff).
		if msg, ready := podReadyCondition(pod.Status.Conditions); !ready {
			message = msg
			if msg != "" && !podReadinessStillStarting(
				pod.Status.Conditions,
			) {
				// Ready=False + message: container has an actual error.
				status = StatusFailed
			} else {
				// Kubernetes attaches a non-empty ContainersNotReady message
				// while readiness probes are still warming up. Container
				// waiting/termination failures were classified above.
				status = StatusStarting
			}
		}
	}

	return &WorkerResult{
		Name:           name,
		Backend:        "k8s",
		DeploymentMode: DeployCloud,
		Status:         status,
		Message:        message,
		RawStatus:      rawStatus,
	}, nil
}

func podContainerFailureStatus(statusGroups ...[]corev1.ContainerStatus) (WorkerStatus, string, string, bool) {
	// 逻辑说明：按调用方给定顺序扫描 init 与主容器状态；已知 waiting 故障或非零 terminated 首次命中即返回统一 Failed、可读消息和原始原因，没有硬故障返回 false。
	for _, statuses := range statusGroups {
		for i := range statuses {
			cs := statuses[i]
			if waiting := cs.State.Waiting; waiting != nil {
				reason := strings.TrimSpace(waiting.Reason)
				if isK8sContainerFailureReason(reason) {
					return StatusFailed, formatK8sContainerStateMessage(cs.Name, reason, waiting.Message), reason, true
				}
			}
			if terminated := cs.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
				reason := strings.TrimSpace(terminated.Reason)
				if reason == "" {
					reason = fmt.Sprintf("ExitCode%d", terminated.ExitCode)
				}
				return StatusFailed, formatK8sContainerStateMessage(cs.Name, reason, terminated.Message), reason, true
			}
		}
	}
	return "", "", "", false
}

func isK8sContainerFailureReason(reason string) bool {
	// 逻辑说明：只把镜像、容器配置、创建与崩溃循环等确定性 waiting 原因判为失败；普通 ContainerCreating 等启动原因返回 false，避免过早标红。
	switch reason {
	case "CrashLoopBackOff",
		"CreateContainerConfigError",
		"CreateContainerError",
		"ErrImageNeverPull",
		"ErrImagePull",
		"ImageInspectError",
		"ImagePullBackOff",
		"InvalidImageName",
		"RegistryUnavailable",
		"RunContainerError":
		return true
	default:
		return false
	}
}

func formatK8sContainerStateMessage(containerName, reason, message string) string {
	// 逻辑说明：清理 reason/message 空白并按“容器名: 原因: 详情”组合诊断文本；缺 reason 使用稳定兜底，缺详情时不附多余分隔符。
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "container failed"
	}
	if containerName != "" {
		reason = fmt.Sprintf("container %s: %s", containerName, reason)
	}
	if msg := strings.TrimSpace(message); msg != "" {
		return reason + ": " + msg
	}
	return reason
}

// podReadyCondition finds the Ready condition and returns (message, ready).
//   - No Ready condition found → ("", true) — conditions not yet populated.
//   - Ready.Status == True    → ("", true) — container is healthy.
//   - Ready.Status != True    → (Ready.Message, false) — container not ready;
//     message may be empty (still starting) or non-empty (actual error).
func podReadyCondition(conditions []corev1.PodCondition) (string, bool) {
	// 逻辑说明：查找 PodReady 条件并返回其消息与布尔就绪值；条件尚未出现按历史兼容视为 true，明确非 True 才返回 false。
	for i := range conditions {
		if conditions[i].Type == corev1.PodReady {
			if conditions[i].Status == corev1.ConditionTrue {
				return "", true
			}
			return conditions[i].Message, false
		}
	}
	// No Ready condition yet — treat as healthy (backward compat).
	return "", true
}

func podReadinessStillStarting(conditions []corev1.PodCondition) bool {
	// 逻辑说明：定位非 True 的 PodReady 条件，仅当 reason 为 `ContainersNotReady` 时判定仍在探针预热；其他非就绪原因交给上层视为失败。
	for i := range conditions {
		condition := conditions[i]
		if condition.Type == corev1.PodReady &&
			condition.Status != corev1.ConditionTrue {
			return condition.Reason == "ContainersNotReady"
		}
	}
	return false
}

func (k *K8sBackend) podName(prefix, name string) string {
	// 逻辑说明：请求级前缀非空时优先拼接，否则使用 backend 默认前缀；返回稳定 Pod 名，不访问集群。
	if prefix != "" {
		return prefix + name
	}
	return k.containerPrefix + name
}

func (k *K8sBackend) workerPodName(name string) string {
	return k.containerPrefix + name
}

// workerNamePrefix returns the default worker SA name prefix, e.g.
// "agentteams-worker-". Used only when a CreateRequest arrives without an
// explicit ServiceAccountName (production callers always set one).
func (k *K8sBackend) workerNamePrefix() string {
	// 逻辑说明：从租户 ResourcePrefix 派生默认 Worker ServiceAccount 前缀；未配置时使用兼容值 `agentteams-worker-`，只参与命名。
	if k.config.ResourcePrefix == "" {
		return "agentteams-worker-"
	}
	return k.config.ResourcePrefix + "worker-"
}

// buildDefaultResources constructs the backend-level default ResourceRequirements
// that apply when neither the CreateRequest nor the agent pod template
// specifies resources. Request side is fixed at "100m" / "256Mi" to match
// historical behavior; limits come from K8sConfig.WorkerCPU / WorkerMemory.
func buildDefaultResources(workerCPU, workerMemory string) corev1.ResourceRequirements {
	// 逻辑说明：为空的 limit 补 1 CPU/2Gi，再构造固定 100m/256Mi requests 的默认资源对象；数量解析失败由 MustParse 暴露为配置错误。
	if workerCPU == "" {
		workerCPU = "1000m"
	}
	if workerMemory == "" {
		workerMemory = "2Gi"
	}
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(workerCPU),
			corev1.ResourceMemory: resource.MustParse(workerMemory),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// mergeResourceOverrides layers a ResourceRequirements override (from
// CreateRequest.Resources) on top of defaults, field by field.
func mergeResourceOverrides(defaults corev1.ResourceRequirements, override *ResourceRequirements) corev1.ResourceRequirements {
	// 逻辑说明：深拷贝默认资源，再仅用非空请求字段逐项覆盖 CPU/内存 request/limit；nil 覆盖返回独立副本，避免修改共享默认 map。
	out := *defaults.DeepCopy()
	if override == nil {
		return out
	}
	if override.CPULimit != "" {
		out.Limits[corev1.ResourceCPU] = resource.MustParse(override.CPULimit)
	}
	if override.MemoryLimit != "" {
		out.Limits[corev1.ResourceMemory] = resource.MustParse(override.MemoryLimit)
	}
	if override.CPURequest != "" {
		out.Requests[corev1.ResourceCPU] = resource.MustParse(override.CPURequest)
	}
	if override.MemoryRequest != "" {
		out.Requests[corev1.ResourceMemory] = resource.MustParse(override.MemoryRequest)
	}
	return out
}

// mergeOSSRegionFromProcessEnv sets AGENTTEAMS_FS_BUCKET and AGENTTEAMS_REGION when the client
// omitted them; the controller process should already have these from the same Secret as Manager (envFrom).
func mergeOSSRegionFromProcessEnv(env map[string]string) {
	// 逻辑说明：仅当请求环境缺少文件桶或 region 时，从 Controller 进程环境补入已去空白的值；显式请求值优先，nil map 不创建新状态。
	if env == nil {
		return
	}
	bucket := firstNonEmptyTrimmed(
		env["AGENTTEAMS_FS_BUCKET"],
		os.Getenv("AGENTTEAMS_FS_BUCKET"),
	)
	if bucket != "" && strings.TrimSpace(env["AGENTTEAMS_FS_BUCKET"]) == "" {
		env["AGENTTEAMS_FS_BUCKET"] = bucket
	}
	if v := firstNonEmptyTrimmed(os.Getenv("AGENTTEAMS_REGION")); v != "" && strings.TrimSpace(env["AGENTTEAMS_REGION"]) == "" {
		env["AGENTTEAMS_REGION"] = v
	}
}

func firstNonEmptyTrimmed(values ...string) string {
	// 逻辑说明：按参数顺序返回第一个去空白后非空的字符串，全部为空返回空值；用于表达显式配置优先级。
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func buildK8sEnvVars(env map[string]string) []corev1.EnvVar {
	// 逻辑说明：过滤空值、排序键后生成稳定 EnvVar 列表；确定顺序让 Pod spec hash 与测试不受 Go map 随机迭代影响。
	keys := make([]string, 0, len(env))
	for k := range env {
		if env[k] != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var out []corev1.EnvVar
	for _, k := range keys {
		out = append(out, corev1.EnvVar{Name: k, Value: env[k]})
	}
	return out
}

func podWorkerDepsVolumes(deps *WorkerDepsSpec) ([]corev1.Volume, []corev1.VolumeMount) {
	// 逻辑说明：当 worker-deps 声明完整且有挂载时，生成一个 PVC volume 和保持顺序的多个 mount；缺依赖/卷/挂载返回 nil，避免创建无用 PVC 引用。
	if deps == nil || deps.PodVolume == nil || len(deps.PodVolume.Mounts) == 0 {
		return nil, nil
	}
	vol := corev1.Volume{
		Name: deps.PodVolume.Name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: deps.PodVolume.ClaimName,
			},
		},
	}
	mounts := make([]corev1.VolumeMount, 0, len(deps.PodVolume.Mounts))
	for _, mount := range deps.PodVolume.Mounts {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      deps.PodVolume.Name,
			MountPath: mount.MountPath,
			SubPath:   mount.SubPath,
			ReadOnly:  mount.ReadOnly,
		})
	}
	return []corev1.Volume{vol}, mounts
}

func buildHostAliases(extraHosts []string) []corev1.HostAlias {
	// 逻辑说明：解析 `hostname:ip` 条目、丢弃格式不全项，并按 IP 分组；IP 与组内主机均排序后输出，保证语义相同输入生成稳定 Pod spec。
	byIP := map[string][]string{}
	for _, entry := range extraHosts {
		host, ip, ok := strings.Cut(strings.TrimSpace(entry), ":")
		if !ok || host == "" || ip == "" {
			continue
		}
		byIP[ip] = append(byIP[ip], host)
	}
	if len(byIP) == 0 {
		return nil
	}

	ips := make([]string, 0, len(byIP))
	for ip := range byIP {
		ips = append(ips, ip)
	}
	sort.Strings(ips)

	aliases := make([]corev1.HostAlias, 0, len(ips))
	for _, ip := range ips {
		hosts := byIP[ip]
		sort.Strings(hosts)
		aliases = append(aliases, corev1.HostAlias{
			IP:        ip,
			Hostnames: hosts,
		})
	}
	return aliases
}

func normalizeK8sPodPhase(phase corev1.PodPhase) WorkerStatus {
	// 逻辑说明：把 Kubernetes Running/Pending/终止 phase 映射成跨后端 Running/Starting/Stopped；空值及未知 phase 返回 Unknown，容器级失败由 Status 另行细化。
	switch phase {
	case corev1.PodRunning:
		return StatusRunning
	case corev1.PodPending:
		return StatusStarting
	case corev1.PodSucceeded, corev1.PodFailed:
		return StatusStopped
	default:
		return StatusUnknown
	}
}

func rawK8sPhase(phase corev1.PodPhase) string {
	// 逻辑说明：为尚未写入 phase 的新 Pod 返回可读 `Pending`，其他值保留 Kubernetes 原始字符串供 API 诊断。
	if phase == "" {
		return "Pending"
	}
	return string(phase)
}

func defaultRuntime(runtime string) string {
	// 逻辑说明：只允许已知 Manager/Worker runtime 原样通过，未知或空值回退历史 OpenClaw；这是内部防御，外层仍负责校验用户输入。
	switch runtime {
	case RuntimeAgentScope,
		RuntimeOpenClaw,
		RuntimeCopaw,
		RuntimeHermes,
		RuntimeQwenPaw:
		return runtime
	default:
		return RuntimeOpenClaw
	}
}

func loadK8sRESTConfig() (*rest.Config, error) {
	// 逻辑说明：优先加载 Pod 内 ServiceAccount 配置，失败后使用 `KUBECONFIG` 或用户默认文件；文件缺失和解析错误带路径返回，不构造空 client。
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = clientcmd.RecommendedHomeFile
	}
	if _, err := os.Stat(kubeconfig); err != nil {
		return nil, fmt.Errorf("load kubernetes config: no in-cluster config and kubeconfig %q not found", kubeconfig)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load kubernetes kubeconfig %q: %w", kubeconfig, err)
	}
	return cfg, nil
}

func detectK8sNamespace() string {
	// 逻辑说明：优先读取显式 namespace 环境变量，否则读取 ServiceAccount 挂载文件并去空白；两者都不可用返回空值，让构造/Available 明确失败。
	if ns := strings.TrimSpace(os.Getenv("AGENTTEAMS_K8S_NAMESPACE")); ns != "" {
		return ns
	}
	if data, err := os.ReadFile(defaultK8sNamespaceFile); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	return ""
}

func boolPtr(v bool) *bool {
	return &v
}
