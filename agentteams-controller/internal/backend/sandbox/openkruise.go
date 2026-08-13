package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var sandboxGVR = schema.GroupVersionResource{
	Group:    "agents.kruise.io",
	Version:  "v1alpha1",
	Resource: "sandboxes",
}

var sandboxClaimGVR = schema.GroupVersionResource{
	Group:    "agents.kruise.io",
	Version:  "v1alpha1",
	Resource: "sandboxclaims",
}

// OpenKruisePlugin implements SandboxPlugin for the OpenKruise Agent Sandbox CRD.
// It uses the Kubernetes dynamic client to operate on agents.kruise.io/v1alpha1
// Sandbox resources, which is compatible with both open-source openkruise/agents
// and Alibaba Cloud ACS.
type OpenKruisePlugin struct {
	dynamicClient dynamic.Interface
}

// NewOpenKruisePlugin creates a new OpenKruise sandbox plugin.
func NewOpenKruisePlugin(dynamicClient dynamic.Interface) *OpenKruisePlugin {
	return &OpenKruisePlugin{dynamicClient: dynamicClient}
}

func (p *OpenKruisePlugin) Type() string { return "openkruise" }

// MaxCapabilities returns the theoretical maximum capabilities of the OpenKruise
// plugin. Actual capabilities are min(Max, config.Capabilities).
func (p *OpenKruisePlugin) MaxCapabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Hibernate: true,
		Pool:      true,
	}
}

func (p *OpenKruisePlugin) Capabilities(config ProviderConfig) ProviderCapabilities {
	// 逻辑说明：把插件理论最大能力与部署配置逐项取交集，确保未显式启用的休眠/池能力不会被调用。
	max := p.MaxCapabilities()
	cfg := config.Capabilities
	return ProviderCapabilities{
		Hibernate: max.Hibernate && cfg.Hibernate,
		Pool:      max.Pool && cfg.Pool,
	}
}

// resolveClient returns the dynamic client to use for an operation.
// It prefers config.DynamicClient (set by SandboxBackend.resolveProviderConfig
// for remote-mode operations) and falls back to the plugin's own client
// (local cluster) when config.DynamicClient is nil.
func (p *OpenKruisePlugin) resolveClient(config ProviderConfig) dynamic.Interface {
	// 逻辑说明：操作级 ProviderConfig 带动态客户端时优先使用，否则回退插件本地集群客户端；只选择引用，不建立新连接。
	if config.DynamicClient != nil {
		return config.DynamicClient
	}
	return p.dynamicClient
}

func (p *OpenKruisePlugin) CreateSandboxClaim(ctx context.Context, spec SandboxClaimSpec, config ProviderConfig) (SandboxHandle, error) {
	// 逻辑说明：确定 namespace，组装 metadata/spec/ownerRef 后创建 SandboxClaim；AlreadyExists 映射类型化冲突，成功按多个兼容 status 字段解析实际 SandboxID，最后才回退 claim 名。
	ns := config.Namespace
	if ns == "" {
		ns = spec.Namespace
	}

	metadata := map[string]interface{}{
		"name":      spec.Name,
		"namespace": ns,
	}
	if len(spec.Labels) > 0 {
		metadata["labels"] = toStringInterfaceMap(spec.Labels)
	}
	if len(spec.Annotations) > 0 {
		metadata["annotations"] = toStringInterfaceMap(spec.Annotations)
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "agents.kruise.io/v1alpha1",
			"kind":       "SandboxClaim",
			"metadata":   metadata,
			"spec":       p.buildSandboxClaimSpec(spec),
		},
	}

	if spec.OwnerRef != nil {
		obj.SetOwnerReferences([]metav1.OwnerReference{*spec.OwnerRef})
	}

	created, err := p.resolveClient(config).Resource(sandboxClaimGVR).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return SandboxHandle{}, fmt.Errorf("%w: %s/%s: %v", ErrAlreadyExists, ns, spec.Name, err)
		}
		return SandboxHandle{}, fmt.Errorf("openkruise create SandboxClaim %s/%s: %w", ns, spec.Name, err)
	}

	sandboxID, _, _ := unstructured.NestedString(created.Object, "status", "sandboxName")
	if sandboxID == "" {
		sandboxID, _, _ = unstructured.NestedString(created.Object, "status", "sandboxRef", "name")
	}
	if sandboxID == "" {
		sandboxID = created.GetName()
	}
	return SandboxHandle{
		SandboxID: sandboxID,
	}, nil
}

func (p *OpenKruisePlugin) DeleteSandboxClaim(ctx context.Context, claimID string, config ProviderConfig) error {
	// 逻辑说明：删除精确 namespace/name 的 SandboxClaim；Kubernetes NotFound 视为幂等成功，其他 API 错误带资源定位返回。
	ns := config.Namespace
	err := p.resolveClient(config).Resource(sandboxClaimGVR).Namespace(ns).Delete(ctx, claimID, metav1.DeleteOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("openkruise delete SandboxClaim %s/%s: %w", ns, claimID, err)
	}
	return nil
}

func (p *OpenKruisePlugin) DeleteSandbox(ctx context.Context, sandboxID string, config ProviderConfig) error {
	// 逻辑说明：删除精确实际 Sandbox CR；已不存在不报错，权限、连接或 admission 错误原样包装，避免误认为资源已清理。
	ns := config.Namespace
	err := p.resolveClient(config).Resource(sandboxGVR).Namespace(ns).Delete(ctx, sandboxID, metav1.DeleteOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("openkruise delete sandbox %s/%s: %w", ns, sandboxID, err)
	}
	return nil
}

func (p *OpenKruisePlugin) HibernateSandbox(ctx context.Context, sandboxID string, config ProviderConfig) error {
	// 逻辑说明：先执行 capability 门禁，再用同一 MergePatch 原子写入 paused=true 与最后暂停时间；序列化或 API patch 失败都不报告休眠成功。
	caps := p.Capabilities(config)
	if !caps.Hibernate {
		return ErrCapabilityNotSupported
	}

	ns := config.Namespace
	// Patch spec.paused=true and stamp last-paused-time in the same MergePatch.
	// Co-locating the two writes guarantees the bookkeeping annotation is
	// updated iff the hibernate intent is recorded server-side, so retries
	// after a partial failure cannot drift the timestamp from reality.
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				AnnotationLastPausedTime: time.Now().UTC().Format(time.RFC3339),
			},
		},
		"spec": map[string]interface{}{
			"paused": true,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("openkruise hibernate: marshal patch: %w", err)
	}

	_, err = p.resolveClient(config).Resource(sandboxGVR).Namespace(ns).Patch(
		ctx, sandboxID, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("openkruise hibernate sandbox %s/%s: %w", ns, sandboxID, err)
	}
	return nil
}

func (p *OpenKruisePlugin) ResumeSandbox(ctx context.Context, sandboxID string, config ProviderConfig) error {
	// 逻辑说明：用幂等 MergePatch 把 paused 改为 false；已运行实例不会受破坏，patch 构造或 API 错误携带 Sandbox 定位返回。
	// Resume is an idempotent MergePatch of spec.paused=false. It is a
	// no-op against an already-running CR and has no destructive side
	// effects, so no capability gate is needed. Only Hibernate (which
	// actively stops the workload) keeps the opt-in gate.
	ns := config.Namespace
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"paused": false,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("openkruise resume: marshal patch: %w", err)
	}

	_, err = p.resolveClient(config).Resource(sandboxGVR).Namespace(ns).Patch(
		ctx, sandboxID, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("openkruise resume sandbox %s/%s: %w", ns, sandboxID, err)
	}
	return nil
}

func (p *OpenKruisePlugin) GetSandboxStatus(ctx context.Context, sandboxID string, config ProviderConfig) (SandboxStatus, error) {
	// 逻辑说明：读取实际 Sandbox，区分真实缺失与查询故障；删除中返回 Terminating，paused=true 立即覆盖为 Hibernated，再提取 status 与 Ready 条件生成统一快照。
	ns := config.Namespace
	obj, err := p.resolveClient(config).Resource(sandboxGVR).Namespace(ns).Get(ctx, sandboxID, metav1.GetOptions{})
	if err != nil {
		if isNotFound(err) {
			// CR really does not exist. Surface a typed sentinel rather
			// than a synthesized Phase so the backend layer cannot
			// accidentally conflate "gone" with "Terminated".
			return SandboxStatus{}, ErrNotFound
		}
		return SandboxStatus{}, fmt.Errorf("openkruise get sandbox %s/%s: %w", ns, sandboxID, err)
	}

	// CR still exists but is already being deleted (finalizer in progress).
	// Report a synthetic Terminating phase so the reconciler waits instead
	// of trying to Create on top of a terminating object and triggering
	// "object is being deleted" AlreadyExists errors.
	if ts := obj.GetDeletionTimestamp(); ts != nil && !ts.IsZero() {
		return SandboxStatus{Phase: PhaseTerminating}, nil
	}

	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")

	// .spec.paused is the operator's authoritative intent and is set
	// synchronously by HibernateSandbox, while .status.phase is reconciled
	// by the provider asynchronously. When paused=true, override the phase
	// so the upper layer sees Hibernated immediately and does not race the
	// provider into a Delete+Create cycle. Single-direction only:
	// paused=false does NOT override, because resume has legitimate
	// intermediate phases (Starting/Resuming) the provider reports more
	// accurately.
	if paused, ok, _ := unstructured.NestedBool(obj.Object, "spec", "paused"); ok && paused {
		phase = PhaseHibernated
	}

	var raw map[string]any
	if statusMap, ok, _ := unstructured.NestedMap(obj.Object, "status"); ok {
		raw = statusMap
	}

	// Check the "Ready" condition specifically — only this condition type
	// determines container health. Other conditions (e.g. InplaceUpdate) are
	// informational and do not affect the running/not-running determination.
	readyStatus := true
	var readyMessage string
	if conditions, ok, _ := unstructured.NestedSlice(obj.Object, "status", "conditions"); ok {
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(cond, "type")
			if condType != "Ready" {
				continue
			}
			// Found the Ready condition.
			s, _, _ := unstructured.NestedString(cond, "status")
			if s != "True" {
				readyStatus = false
				readyMessage, _, _ = unstructured.NestedString(cond, "message")
			}
			break
		}
	}

	return SandboxStatus{
		SandboxID:             sandboxID,
		Phase:                 phase,
		Message:               message,
		Raw:                   raw,
		ReadyConditionStatus:  readyStatus,
		ReadyConditionMessage: readyMessage,
	}, nil
}

func (p *OpenKruisePlugin) ListSandboxes(ctx context.Context, matchLabels map[string]string, config ProviderConfig) ([]SandboxStatus, error) {
	// 逻辑说明：把精确标签转 selector 列出 Sandbox，再逐个复用 GetSandboxStatus 补齐语义；列表期间消失的对象跳过，其他单项错误终止以免返回不可信集合。
	ns := config.Namespace
	opts := metav1.ListOptions{}
	if len(matchLabels) > 0 {
		opts.LabelSelector = labels.SelectorFromSet(labels.Set(matchLabels)).String()
	}
	list, err := p.resolveClient(config).Resource(sandboxGVR).Namespace(ns).List(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("openkruise list sandboxes %s selector %q: %w", ns, opts.LabelSelector, err)
	}
	out := make([]SandboxStatus, 0, len(list.Items))
	for i := range list.Items {
		status, err := p.GetSandboxStatus(ctx, list.Items[i].GetName(), config)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, status)
	}
	return out, nil
}

func (p *OpenKruisePlugin) GetSandboxClaimStatus(ctx context.Context, claimID string, config ProviderConfig) (SandboxStatus, error) {
	// 逻辑说明：读取 SandboxClaim 并解析 phase/message、期望/已领取副本和 Ready 条件；NotFound 类型化返回，删除中用 Terminating，缺副本字段通过 nil 保留“未知”语义。
	ns := config.Namespace
	obj, err := p.resolveClient(config).Resource(sandboxClaimGVR).Namespace(ns).Get(ctx, claimID, metav1.GetOptions{})
	if err != nil {
		if isNotFound(err) {
			return SandboxStatus{}, ErrNotFound
		}
		return SandboxStatus{}, fmt.Errorf("openkruise get SandboxClaim %s/%s: %w", ns, claimID, err)
	}
	if ts := obj.GetDeletionTimestamp(); ts != nil && !ts.IsZero() {
		return SandboxStatus{Phase: PhaseTerminating}, nil
	}

	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")
	desiredReplicas, desiredOK, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
	claimedReplicas, claimedOK, _ := unstructured.NestedInt64(obj.Object, "status", "claimedReplicas")
	var raw map[string]any
	if statusMap, ok, _ := unstructured.NestedMap(obj.Object, "status"); ok {
		raw = statusMap
	}
	var desiredReplicasPtr *int64
	if desiredOK {
		desiredReplicasPtr = &desiredReplicas
	}
	var claimedReplicasPtr *int64
	if claimedOK {
		claimedReplicasPtr = &claimedReplicas
	}
	readyStatus, readyMessage := readyConditionFromObject(obj)
	return SandboxStatus{
		Phase:                 phase,
		Message:               message,
		Raw:                   raw,
		ReadyConditionStatus:  readyStatus,
		ReadyConditionMessage: readyMessage,
		DesiredReplicas:       desiredReplicasPtr,
		ClaimedReplicas:       claimedReplicasPtr,
	}, nil
}

func (p *OpenKruisePlugin) Validate(config ProviderConfig) error {
	// 逻辑说明：确认插件至少具备本地 dynamic client；不在启动期探测 CRD/RBAC，避免为只执行特定操作的账号额外要求 list 权限。
	if p.dynamicClient == nil {
		return fmt.Errorf("%w: dynamic client is nil", ErrInvalidConfig)
	}
	// CRD existence is validated at operation time
	// to avoid requiring list permissions on the CRD during startup.
	return nil
}

func (p *OpenKruisePlugin) HealthCheck(ctx context.Context, config ProviderConfig) error {
	// 逻辑说明：确认客户端存在后对目标 namespace 执行 Limit=1 的 Sandbox 列表，验证 API/CRD/RBAC；任何失败统一包装 provider unavailable 供上层降级。
	if p.dynamicClient == nil {
		return fmt.Errorf("%w: dynamic client is nil", ErrProviderUnavailable)
	}
	_, err := p.resolveClient(config).Resource(sandboxGVR).Namespace(config.Namespace).List(
		ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProviderUnavailable, err)
	}
	return nil
}

func (p *OpenKruisePlugin) buildSandboxClaimSpec(spec SandboxClaimSpec) map[string]interface{} {
	// 逻辑说明：把类型化 claim 请求纯转换成 CRD spec，固定单副本与超时/TTL，并按需加入标签、注解、原地镜像和动态卷；不修改输入集合。
	out := map[string]interface{}{
		"templateName":      spec.SandboxSetName,
		"replicas":          int64(1),
		"claimTimeout":      "5m",
		"waitReadyTimeout":  "2m",
		"ttlAfterCompleted": "15m",
	}
	if len(spec.Labels) > 0 {
		out["labels"] = toStringInterfaceMap(spec.Labels)
	}
	if len(spec.Annotations) > 0 {
		out["annotations"] = toStringInterfaceMap(spec.Annotations)
	}
	if spec.InplaceUpdate != nil && spec.InplaceUpdate.Image != "" {
		out["inplaceUpdate"] = map[string]interface{}{
			"image": spec.InplaceUpdate.Image,
		}
	}
	if len(spec.DynamicVolumesMount) > 0 {
		mounts := make([]interface{}, 0, len(spec.DynamicVolumesMount))
		for _, mount := range spec.DynamicVolumesMount {
			item := map[string]interface{}{
				"pvName":    mount.PVName,
				"mountPath": mount.MountPath,
				"subPath":   mount.SubPath,
				"readOnly":  mount.ReadOnly,
			}
			if len(mount.Attributes) > 0 {
				item["attributes"] = toStringInterfaceMap(mount.Attributes)
			}
			mounts = append(mounts, item)
		}
		out["dynamicVolumesMount"] = mounts
	}
	return out
}

func readyConditionFromObject(obj *unstructured.Unstructured) (bool, string) {
	// 逻辑说明：扫描 unstructured status.conditions 中第一个 `Ready` 条件；缺条件默认 true，明确非 True 返回 false 与消息，忽略其他条件类型。
	readyStatus := true
	var readyMessage string
	if conditions, ok, _ := unstructured.NestedSlice(obj.Object, "status", "conditions"); ok {
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(cond, "type")
			if condType != "Ready" {
				continue
			}
			s, _, _ := unstructured.NestedString(cond, "status")
			if s != "True" {
				readyStatus = false
				readyMessage, _, _ = unstructured.NestedString(cond, "message")
			}
			break
		}
	}
	return readyStatus, readyMessage
}

// toStringInterfaceMap converts map[string]string to map[string]interface{} for unstructured.
func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	// 逻辑说明：nil 保持 nil，否则新建 interface map 并复制所有字符串键值，满足 unstructured 序列化同时隔离输入可变状态。
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// isNotFound checks if the error is a Kubernetes NotFound error.
func isNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}
