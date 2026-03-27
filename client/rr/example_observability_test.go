package rr_test

import (
	"context"
	"log"
	"time"

	"github.com/jrmarcco/xgrpc/client/rr"
	"github.com/jrmarcco/xgrpc/register"
)

type noopRegistry struct{}

func (noopRegistry) Register(context.Context, register.ServiceInstance) error { return nil }

func (noopRegistry) Unregister(context.Context, register.ServiceInstance) error { return nil }

func (noopRegistry) ListServices(context.Context, string) ([]register.ServiceInstance, error) {
	return nil, nil
}

func (noopRegistry) Subscribe(string) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (noopRegistry) Close() error { return nil }

// ExampleResolverBuilder_observabilityCallbacks 展示如何接入 resolver 可选观测回调。
//
// 这里不调用 Build，只演示 Builder 配置方式。
func ExampleResolverBuilder_observabilityCallbacks() {
	var registry register.Registry = noopRegistry{}

	builder, _ := rr.NewResolverBuilder(registry, 2*time.Second)
	_ = builder.
		OnResolveError(func(serviceName string, err error) {
			log.Printf("resolve error service=%s err=%v", serviceName, err)
		}).
		OnResolveUpdated(func(serviceName string, instanceCount int) {
			log.Printf("resolve updated service=%s instances=%d", serviceName, instanceCount)
		})

	// Output:
}
