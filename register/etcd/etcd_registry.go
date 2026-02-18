package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/jrmarcco/xgrpc/internal/errs"
	"github.com/jrmarcco/xgrpc/register"
)

type Builder struct {
	etcdClient *clientv3.Client

	keyPrefix string
	// 租约 ttl ( 默认 30s )。
	//
	// | cluster scale | recommended lease TTL ( second ) |
	// |    < 50       |             30                   |
	// |    50 ~ 200   |             15                   |
	// |    > 200      |             10                   |
	//
	// 集群规模越大续约越频繁的原因：
	//
	// 	1. 服务变更感知时效性要求更高。
	// 	2. 减少“僵尸”服务的影响。
	// 	3. 防止单点故障影响扩大。
	// 	4. 更快的健康检查与剔除。
	// 	5. 分摊注册中心压力。
	//
	// 虽然续约频繁会增加 etcd 的 QPS，但大集群本身对注册中心的压力主要来自服务变更和查询。
	// 通过合理缩短 TTL 和加快续约，可以让 etcd 更及时地维护服务列表，避免因过期服务过多导致的查询不准确。
	leaseTTL int
}

func NewBuilder(etcdClient *clientv3.Client) *Builder {
	return &Builder{
		etcdClient: etcdClient,
		keyPrefix:  "xgrpc",
		leaseTTL:   30,
	}
}

// LeaseTTL 设置租约 ttl ( 单位为秒 )。
func (b *Builder) LeaseTTL(ttl int) *Builder {
	b.leaseTTL = ttl
	return b
}

// KeyPrefix 设置注册服务 key 前缀。
func (b *Builder) KeyPrefix(keyPrefix string) *Builder {
	b.keyPrefix = keyPrefix
	return b
}

// Build 构建注册器。
func (b *Builder) Build() (*Registry, error) {
	if b.leaseTTL <= 0 {
		return nil, errs.ErrInvalidEtcdLeaseTTL
	}

	session, err := concurrency.NewSession(b.etcdClient, concurrency.WithTTL(b.leaseTTL))
	if err != nil {
		return nil, err
	}
	return &Registry{
		keyPrefix:   b.keyPrefix,
		etcdClient:  b.etcdClient,
		etcdSession: session,
	}, nil
}

var _ register.Registry = (*Registry)(nil)

type Registry struct {
	mu sync.Mutex

	keyPrefix string

	etcdClient  *clientv3.Client
	etcdSession *concurrency.Session

	watchCancel []context.CancelFunc
}

func (r *Registry) Register(ctx context.Context, si register.ServiceInstance) error {
	val, err := json.Marshal(si)
	if err != nil {
		return err
	}
	_, err = r.etcdClient.Put(ctx, r.instanceKey(si), string(val), clientv3.WithLease(r.etcdSession.Lease()))
	return err
}

func (r *Registry) Unregister(ctx context.Context, si register.ServiceInstance) error {
	_, err := r.etcdClient.Delete(ctx, r.instanceKey(si))
	return err
}

func (r *Registry) ListServices(ctx context.Context, serviceName string) ([]register.ServiceInstance, error) {
	resp, err := r.etcdClient.Get(ctx, r.serviceKey(serviceName), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]register.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var si register.ServiceInstance
		if err := json.Unmarshal(kv.Value, &si); err != nil {
			return nil, err
		}
		res = append(res, si)
	}
	return res, nil
}

func (r *Registry) Subscribe(serviceName string) <-chan struct{} {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = clientv3.WithRequireLeader(ctx)

	r.mu.Lock()
	r.watchCancel = append(r.watchCancel, cancel)
	r.mu.Unlock()

	watchChan := r.etcdClient.Watch(ctx, r.serviceKey(serviceName), clientv3.WithPrefix())
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp := <-watchChan:
				if resp.Err() != nil {
					continue
				}
				if resp.Canceled {
					return
				}

				ch <- struct{}{}
			}
		}
	}()
	return ch
}

func (r *Registry) serviceKey(serviceName string) string {
	return fmt.Sprintf("/%s/%s", r.keyPrefix, serviceName)
}

func (r *Registry) instanceKey(si register.ServiceInstance) string {
	return fmt.Sprintf("/%s/%s/%s", r.keyPrefix, si.Name, si.Addr)
}

func (r *Registry) Close() error {
	r.mu.Lock()
	for _, cancel := range r.watchCancel {
		cancel()
	}
	r.mu.Unlock()
	return r.etcdSession.Close()
}
