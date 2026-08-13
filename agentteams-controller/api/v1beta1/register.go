package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
)

// Resource 把资源复数名与 AgentTeams API group 组合成 Kubernetes
// GroupResource，主要用于构造带正确 group/resource 的 API 错误。
func Resource(resource string) schema.GroupResource {
	// 逻辑说明：把调用方给出的资源复数名绑定到当前 API group/version，供错误对象携带一致资源身份。
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

func addKnownTypes(scheme *runtime.Scheme) error {
	// 逻辑说明：把四类 CR 及列表类型注册进共享 Scheme，并登记 metav1 类型，序列化器才能识别它们。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Worker{},
		&WorkerList{},
		&Team{},
		&TeamList{},
		&Human{},
		&HumanList{},
		&Manager{},
		&ManagerList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
