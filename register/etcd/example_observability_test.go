package etcd_test

import (
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/jrmarcco/xgrpc/register/etcd"
)

// ExampleBuilder_observabilityCallbacks 展示如何接入可选观测回调。
//
// 这里不调用 Build，只演示 Builder 配置方式，因此不需要真实 etcd 连接。
func ExampleBuilder_observabilityCallbacks() {
	var etcdClient *clientv3.Client

	_ = etcd.NewBuilder(etcdClient).
		KeyPrefix("xgrpc-prod").
		LeaseTTL(15).
		OnWatchError(func(serviceName string, err error) {
			log.Printf("watch error service=%s err=%v", serviceName, err)
		}).
		OnWatchNotifyDrop(func(serviceName string) {
			log.Printf("watch notify dropped service=%s", serviceName)
		}).
		OnListDecodeError(func(serviceName, key string, err error) {
			log.Printf("decode error service=%s key=%s err=%v", serviceName, key, err)
		})

	// Output:
}
