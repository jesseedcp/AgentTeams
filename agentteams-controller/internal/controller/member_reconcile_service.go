package controller

import (
	"context"
	"fmt"
	"reflect"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
)

// ReconcileMemberService ensures a ClusterIP Service exists (or is removed)
// based on ServiceEnabled. This phase runs after ReconcileMemberContainer so
// the Pod selector labels are guaranteed to be present on the target pod.
// Returns the Service name on success (empty string when the Service is
// disabled, deleted or skipped).
// ReconcileMemberService 根据 ServiceEnabled 确保 ClusterIP Service 存在或已删除。
// 它在 Container 阶段后执行，因为 Service selector 要指向已经带稳定
// label 的 Pod。Service 属于 Kubernetes 子资源，调和时不只检查“名字
// 存不存在”，还比较 selector 和 ports，这样 spec 变化才能被幂等更新。
func ReconcileMemberService(ctx context.Context, mc *MemberContext, deps *MemberDeps) (string, error) {
	// 逻辑说明：ReconcileMemberService 接收 ctx(context.Context)、mc(*MemberContext)、deps(*MemberDeps)，依次借助 ensureServiceDeleted、ensureServiceExists调谐Worker Service的期望结果。
	// 返回/状态：返回 string、error；可能读取或修改 Kubernetes API 中的账户、令牌、服务或存储对象。
	// 失败/重试：API 冲突或暂时不可用会返回错误；上层重新读取 resourceVersion 与实际对象后幂等重试。
	if !mc.ServiceEnabled {
		return "", ensureServiceDeleted(ctx, mc, deps)
	}
	return ensureServiceExists(ctx, mc, deps)
}

// ensureServiceExists creates or updates a ClusterIP Service for the member pod.
// Returns the Service name when the Service exists or has just been created;
// returns an empty string when creation is skipped (no ports declared).
func ensureServiceExists(ctx context.Context, mc *MemberContext, deps *MemberDeps) (string, error) {
	// 逻辑说明：ensureServiceExists 接收 ctx(context.Context)、mc(*MemberContext)、deps(*MemberDeps)，依次借助 V、resolveServiceClient、serviceName、serviceSelector确保Worker Service的期望结果。
	// 返回/状态：返回 string、error；可能读取或修改 Kubernetes API 中的账户、令牌、服务或存储对象。
	// 失败/重试：API 冲突或暂时不可用会返回错误；上层重新读取 resourceVersion 与实际对象后幂等重试。
	logger := log.FromContext(ctx)

	// A Service without ports is useless; skip with a log.
	if len(mc.Spec.Expose) == 0 {
		logger.V(1).Info("serviceEnabled is true but no ports declared in spec.expose, skipping Service creation", "name", mc.Name)
		return "", nil
	}

	svcClient, ns, err := resolveServiceClient(ctx, mc, deps)
	if err != nil {
		return "", err
	}

	svcName := serviceName(deps, mc)
	selector := serviceSelector(mc)
	desiredPorts := buildServicePorts(mc)

	existing, err := svcClient.Get(ctx, svcName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("get service %s/%s: %w", ns, svcName, err)
	}

	if apierrors.IsNotFound(err) {
		// Create
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: ns,
				Labels: map[string]string{
					v1beta1.LabelWorker: mc.Name,
					"app":               deps.ResourcePrefix.WorkerAppLabel(),
				},
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selector,
				Ports:    desiredPorts,
			},
		}
		if _, err := svcClient.Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Lost the race; will reconcile on next pass.
				return svcName, nil
			}
			return "", fmt.Errorf("create service %s/%s: %w", ns, svcName, err)
		}
		logger.Info("created ClusterIP Service for member", "name", mc.Name, "service", svcName, "namespace", ns)
		return svcName, nil
	}

	// Update if selector or ports differ.
	needsUpdate := !reflect.DeepEqual(existing.Spec.Selector, selector) ||
		!reflect.DeepEqual(existing.Spec.Ports, desiredPorts)
	if !needsUpdate {
		return svcName, nil
	}

	existing.Spec.Selector = selector
	existing.Spec.Ports = desiredPorts
	if _, err := svcClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return "", fmt.Errorf("update service %s/%s: %w", ns, svcName, err)
	}
	logger.Info("updated ClusterIP Service for member", "name", mc.Name, "service", svcName, "namespace", ns)
	return svcName, nil
}

// ensureServiceDeleted removes every ClusterIP Service tagged with this
// member via the worker identity label. Using a label selector
// (instead of name-based delete) guarantees both current and legacy
// naming conventions are cleaned up in a single pass.
func ensureServiceDeleted(ctx context.Context, mc *MemberContext, deps *MemberDeps) error {
	// 逻辑说明：ensureServiceDeleted 接收 ctx(context.Context)、mc(*MemberContext)、deps(*MemberDeps)，依次借助 resolveServiceClient、List、Delete、IsNotFound确保Worker Service的期望结果。
	// 返回/状态：返回 error；可能读取或修改 Kubernetes API 中的账户、令牌、服务或存储对象。
	// 失败/重试：API 冲突或暂时不可用会返回错误；上层重新读取 resourceVersion 与实际对象后幂等重试。
	svcClient, ns, err := resolveServiceClient(ctx, mc, deps)
	if err != nil {
		// If the backend doesn't support services (e.g. Docker), nothing to delete.
		return nil
	}
	selector := fmt.Sprintf("%s=%s", v1beta1.LabelWorker, mc.Name)
	list, err := svcClient.List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list services for worker %s in %s: %w", mc.Name, ns, err)
	}
	logger := log.FromContext(ctx)
	for i := range list.Items {
		svc := &list.Items[i]
		if err := svcClient.Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("delete service %s/%s: %w", ns, svc.Name, err)
		}
		logger.Info("deleted Service for worker", "worker", mc.Name, "service", svc.Name, "namespace", ns)
	}
	return nil
}

// resolveServiceClient obtains a K8sServiceClient by searching for a backend
// that implements ServiceBackend. Returns an error if none qualifies.
func resolveServiceClient(ctx context.Context, mc *MemberContext, deps *MemberDeps) (backend.K8sServiceClient, string, error) {
	// 逻辑说明：resolveServiceClient 接收 ctx(context.Context)、mc(*MemberContext)、deps(*MemberDeps)，依次借助 FindServiceBackend、ServiceClient解析Worker Service的期望结果。
	// 返回/状态：返回 backend.K8sServiceClient、string、error；可能读取或修改 Kubernetes API 中的账户、令牌、服务或存储对象。
	// 失败/重试：API 冲突或暂时不可用会返回错误；上层重新读取 resourceVersion 与实际对象后幂等重试。
	if deps.Backend == nil {
		return nil, "", fmt.Errorf("no backend registry available")
	}
	sb := deps.Backend.FindServiceBackend(ctx)
	if sb == nil {
		return nil, "", fmt.Errorf("no backend supports Service management")
	}
	return sb.ServiceClient(ctx)
}

// serviceName returns the K8s Service name for the member. The Service name
// matches the Pod name which is the bare Worker CR name (mc.Name).
func serviceName(_ *MemberDeps, mc *MemberContext) string {
	return mc.Name
}

// serviceSelector builds the label selector that matches the member Pod.
// Uses the identity label stamped by createMemberContainer.
func serviceSelector(mc *MemberContext) map[string]string {
	return map[string]string{
		v1beta1.LabelWorker: mc.Name,
	}
}

// buildServicePorts converts spec.Expose entries into Kubernetes ServicePorts.
func buildServicePorts(mc *MemberContext) []corev1.ServicePort {
	// 逻辑说明：buildServicePorts 接收 mc(*MemberContext)，依次借助 int32、FromInt构造Worker Service的期望结果。
	// 返回/状态：返回 []corev1.ServicePort；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	ports := make([]corev1.ServicePort, 0, len(mc.Spec.Expose))
	for _, ep := range mc.Spec.Expose {
		proto := corev1.ProtocolTCP
		name := fmt.Sprintf("port-%d", ep.Port)
		ports = append(ports, corev1.ServicePort{
			Name:       name,
			Port:       int32(ep.Port),
			TargetPort: intstr.FromInt(ep.Port),
			Protocol:   proto,
		})
	}
	return ports
}
