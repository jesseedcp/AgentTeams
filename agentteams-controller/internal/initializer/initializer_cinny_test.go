package initializer

import (
	"context"
	"testing"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
)

type cinnyGatewayRecorder struct {
	serviceSourceName string
	serviceDomain     string
	servicePort       int
	routeName         string
	routeService      string
	routePort         int
	routePath         string
	deletedRoutes     []string
	serviceSources    []string
	routes            []string
}

func (g *cinnyGatewayRecorder) EnsureConsumer(
	context.Context,
	gateway.ConsumerRequest,
) (*gateway.ConsumerResult, error) {
	return nil, nil
}

func (g *cinnyGatewayRecorder) DeleteConsumer(context.Context, string) error {
	return nil
}

func (g *cinnyGatewayRecorder) AuthorizeAIRoutes(context.Context, string, string) error {
	return nil
}

func (g *cinnyGatewayRecorder) DeauthorizeAIRoutes(context.Context, string, string) error {
	return nil
}

func (g *cinnyGatewayRecorder) ExposePort(context.Context, gateway.PortExposeRequest) error {
	return nil
}

func (g *cinnyGatewayRecorder) UnexposePort(context.Context, gateway.PortExposeRequest) error {
	return nil
}

func (g *cinnyGatewayRecorder) EnsureServiceSource(
	_ context.Context,
	name, domain string,
	port int,
	_ string,
) error {
	g.serviceSourceName = name
	g.serviceDomain = domain
	g.servicePort = port
	g.serviceSources = append(g.serviceSources, name+"|"+domain)
	return nil
}

func (g *cinnyGatewayRecorder) EnsureStaticServiceSource(
	context.Context,
	string,
	string,
	int,
) error {
	return nil
}

func (g *cinnyGatewayRecorder) EnsureRoute(
	_ context.Context,
	name string,
	_ []string,
	serviceName string,
	port int,
	pathPrefix string,
) error {
	g.routeName = name
	g.routeService = serviceName
	g.routePort = port
	g.routePath = pathPrefix
	g.routes = append(g.routes, name+"|"+serviceName+"|"+pathPrefix)
	return nil
}

func TestInitGatewayRoutesRegistersManagerAdminUnderDedicatedPrefix(t *testing.T) {
	recorder := &cinnyGatewayRecorder{}
	init := &Initializer{
		Gateway: recorder,
		Config: Config{
			GatewayProvider: "higress",
			ManagerAdminURL: "http://agentteams-manager.agentteams.svc.cluster.local:18799",
		},
	}

	if err := init.initGatewayRoutes(context.Background()); err != nil {
		t.Fatalf("initGatewayRoutes: %v", err)
	}

	if len(recorder.serviceSources) != 1 ||
		recorder.serviceSources[0] != "manager-admin|agentteams-manager.agentteams.svc.cluster.local" {
		t.Fatalf("service sources = %#v", recorder.serviceSources)
	}
	if len(recorder.routes) != 1 ||
		recorder.routes[0] != "manager-admin|manager-admin.dns|/manager-admin" {
		t.Fatalf("routes = %#v", recorder.routes)
	}
	if recorder.routePort != 18799 {
		t.Fatalf("route port = %d, want 18799", recorder.routePort)
	}
}

func (g *cinnyGatewayRecorder) DeleteRoute(_ context.Context, name string) error {
	g.deletedRoutes = append(g.deletedRoutes, name)
	return nil
}

func (g *cinnyGatewayRecorder) EnsureAIProvider(
	context.Context,
	gateway.AIProviderRequest,
) error {
	return nil
}

func (g *cinnyGatewayRecorder) EnsureStreamIdleTimeout(context.Context, int) error {
	return nil
}

func (g *cinnyGatewayRecorder) EnsureAIRoute(context.Context, gateway.AIRouteRequest) error {
	return nil
}

func (g *cinnyGatewayRecorder) ResolveModelProvider(
	context.Context,
	string,
) (*gateway.ModelProviderInfo, error) {
	return nil, nil
}

func (g *cinnyGatewayRecorder) Healthy(context.Context) error {
	return nil
}

func TestInitGatewayRoutesRegistersCinnyAsRootClient(t *testing.T) {
	recorder := &cinnyGatewayRecorder{}
	init := &Initializer{
		Gateway: recorder,
		Config: Config{
			GatewayProvider: "higress",
			CinnyURL:        "http://agentteams-cinny.agentteams.svc.cluster.local:8080",
		},
	}

	if err := init.initGatewayRoutes(context.Background()); err != nil {
		t.Fatalf("initGatewayRoutes: %v", err)
	}

	if recorder.serviceSourceName != "cinny" {
		t.Fatalf("service source = %q, want cinny", recorder.serviceSourceName)
	}
	if recorder.serviceDomain != "agentteams-cinny.agentteams.svc.cluster.local" {
		t.Fatalf("service domain = %q", recorder.serviceDomain)
	}
	if recorder.servicePort != 8080 {
		t.Fatalf("service port = %d", recorder.servicePort)
	}
	if recorder.routeName != "cinny" {
		t.Fatalf("route name = %q, want cinny", recorder.routeName)
	}
	if len(recorder.deletedRoutes) != 2 ||
		recorder.deletedRoutes[0] != "element-web" ||
		recorder.deletedRoutes[1] != "default" {
		t.Fatalf("deleted routes = %#v, want element-web then default", recorder.deletedRoutes)
	}
	if recorder.routeService != "cinny.dns" {
		t.Fatalf("route service = %q, want cinny.dns", recorder.routeService)
	}
	if recorder.routePort != 8080 || recorder.routePath != "/" {
		t.Fatalf("route = port %d path %q", recorder.routePort, recorder.routePath)
	}
}
