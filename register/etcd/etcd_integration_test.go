package etcd_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/jrmarcco/xgrpc/register"
	"github.com/jrmarcco/xgrpc/register/etcd"
)

// TestRegistryIntegration 演示完整链路：
// Build -> Subscribe -> Register -> ListServices -> Unregister。
//
// 运行方式:
// XGRPC_ETCD_ENDPOINTS=127.0.0.1:2379 go test ./register/etcd -run TestRegistryIntegration -v
func TestRegistryIntegration(t *testing.T) {
	endpointsEnv := strings.TrimSpace(os.Getenv("XGRPC_ETCD_ENDPOINTS"))
	if endpointsEnv == "" {
		t.Skip("skip integration test: XGRPC_ETCD_ENDPOINTS is empty")
	}
	endpoints := splitAndTrim(endpointsEnv)
	if len(endpoints) == 0 {
		t.Skip("skip integration test: no valid endpoints in XGRPC_ETCD_ENDPOINTS")
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd client: %v", err)
	}
	defer etcdClient.Close()

	reg, err := etcd.NewBuilder(etcdClient).
		KeyPrefix("xgrpc-it").
		LeaseTTL(10).
		Build()
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	defer reg.Close()

	serviceName := fmt.Sprintf("svc-it-%d", time.Now().UnixNano())
	si := register.ServiceInstance{
		Name: serviceName,
		Addr: fmt.Sprintf("127.0.0.1:%d", time.Now().UnixNano()%40000+20000),
	}

	// 先订阅再注册，尽可能让变更事件可观测。
	ctx := t.Context()
	watchCh := reg.SubscribeWithContext(ctx, serviceName)
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := reg.Register(ctx, si); err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = reg.Unregister(c, si)
	}()

	// Watch 事件可能因时序合并或连接抖动被错过，这里只做示例性观测，不作为强断言。
	select {
	case <-watchCh:
		t.Log("received subscribe notification after register")
	case <-time.After(2 * time.Second):
		t.Log("no subscribe notification observed within timeout")
	}

	instances, err := waitUntilListed(ctx, reg, serviceName, si.Addr)
	if err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(instances) == 0 {
		t.Fatalf("expected at least one instance for service=%s", serviceName)
	}

	unregisterCtx, unregisterCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer unregisterCancel()
	if err := reg.Unregister(unregisterCtx, si); err != nil {
		t.Fatalf("unregister: %v", err)
	}
}

func waitUntilListed(ctx context.Context, reg *etcd.Registry, serviceName, addr string) ([]register.ServiceInstance, error) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		instances, err := reg.ListServices(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		for _, inst := range instances {
			if inst.Addr == addr {
				return instances, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		res = append(res, p)
	}
	return res
}
