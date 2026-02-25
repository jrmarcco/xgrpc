package rr_test

import (
	"log"
	"time"

	"github.com/jrmarcco/xgrpc/client/rr"
	"github.com/jrmarcco/xgrpc/register"
)

// ExampleResolverBuilder_observabilityCallbacks 展示如何接入 resolver 可选观测回调。
//
// 这里不调用 Build，只演示 Builder 配置方式。
func ExampleResolverBuilder_observabilityCallbacks() {
	var registry register.Registry

	_ = rr.NewResolverBuilder(registry, 2*time.Second).
		OnResolveError(func(serviceName string, err error) {
			log.Printf("resolve error service=%s err=%v", serviceName, err)
		}).
		OnResolveUpdated(func(serviceName string, instanceCount int) {
			log.Printf("resolve updated service=%s instances=%d", serviceName, instanceCount)
		})

	// Output:
}
