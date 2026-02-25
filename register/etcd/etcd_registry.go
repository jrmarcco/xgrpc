package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/jrmarcco/xgrpc/internal/errs"
	"github.com/jrmarcco/xgrpc/register"
)

type Builder struct {
	etcdClient *clientv3.Client

	// keyPrefix 是 etcd 中服务注册目录的根前缀。
	//
	// 服务目录： /{keyPrefix}/{serviceName}
	// 服务实例： /{keyPrefix}/{serviceName}/{instanceAddr}
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
	// 虽然续约频繁会增加 etcd 的 QPS，
	// 但大集群本身对注册中心的压力主要来自服务变更和查询。
	// 通过合理缩短 TTL 和加快续约可以让 etcd 更及时地维护服务列表，
	// 避免因过期服务过多导致的查询不准确。
	leaseTTL int

	// 可选观测回调 ( 默认 nil，不影响核心流程 )。
	onWatchError      func(serviceName string, err error)
	onWatchNotifyDrop func(serviceName string)
	onListDecodeError func(serviceName, key string, err error)
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

// OnWatchError 设置 watch 异常回调。
func (b *Builder) OnWatchError(fn func(serviceName string, err error)) *Builder {
	b.onWatchError = fn
	return b
}

// OnWatchNotifyDrop 设置通知被合并 ( 丢弃 ) 回调。
func (b *Builder) OnWatchNotifyDrop(fn func(serviceName string)) *Builder {
	b.onWatchNotifyDrop = fn
	return b
}

// OnListDecodeError 设置服务实例解码失败回调。
func (b *Builder) OnListDecodeError(fn func(serviceName, key string, err error)) *Builder {
	b.onListDecodeError = fn
	return b
}

// Build 构建注册器。
func (b *Builder) Build() (*Registry, error) {
	if b.leaseTTL <= 0 {
		return nil, errs.ErrInvalidEtcdLeaseTTL
	}

	keyPrefix := strings.Trim(strings.TrimSpace(b.keyPrefix), "/")
	if keyPrefix == "" {
		return nil, errs.ErrInvalidEtcdKeyPrefix
	}

	session, err := concurrency.NewSession(b.etcdClient, concurrency.WithTTL(b.leaseTTL))
	if err != nil {
		return nil, err
	}

	return &Registry{
		keyPrefix: keyPrefix,

		etcdClient:  b.etcdClient,
		etcdSession: session,

		watchCancel: make(map[uint64]context.CancelFunc),

		onWatchError:         b.onWatchError,
		onWatchNotifyDrop:    b.onWatchNotifyDrop,
		onServiceDecodeError: b.onListDecodeError,
	}, nil
}

var _ register.Registry = (*Registry)(nil)

type Registry struct {
	// mu 配合 nextWatchID 并发安全地管理订阅生命周期。
	mu sync.Mutex

	// 标准化后的 key 前缀 ( 已去掉首尾 "/" 与空格 )。
	keyPrefix string

	etcdClient *clientv3.Client
	// etcdSession 承载租约续约。
	// Register 方法写入的实例 key 绑定在该 lease 上。
	etcdSession *concurrency.Session

	nextWatchID uint64
	// watchCancel 记录所有订阅取消函数，Close 时会统一回收。
	watchCancel map[uint64]context.CancelFunc

	// closed 标记 registry 是否已关闭，避免 Close 后继续新增订阅。
	closed bool

	onWatchError         func(serviceName string, err error)
	onWatchNotifyDrop    func(serviceName string)
	onServiceDecodeError func(serviceName, key string, err error)
}

// Register 注册服务实例。
func (r *Registry) Register(ctx context.Context, si register.ServiceInstance) error {
	if !isValidServiceName(si.Name) {
		return errs.ErrInvalidServiceName
	}

	val, err := json.Marshal(si)
	if err != nil {
		return err
	}

	// 将服务实例信息写入 etcd。
	// 使用租约续约机制，确保服务实例信息不会过期。
	_, err = r.etcdClient.Put(ctx, r.instanceKey(si), string(val), clientv3.WithLease(r.etcdSession.Lease()))
	return err
}

// Unregister 注销服务实例。
func (r *Registry) Unregister(ctx context.Context, si register.ServiceInstance) error {
	if !isValidServiceName(si.Name) {
		return errs.ErrInvalidServiceName
	}

	_, err := r.etcdClient.Delete(ctx, r.instanceKey(si))
	return err
}

func (r *Registry) ListServices(ctx context.Context, serviceName string) ([]register.ServiceInstance, error) {
	if !isValidServiceName(serviceName) {
		return nil, errs.ErrInvalidServiceName
	}

	// 获取服务目录下所有实例 key。
	resp, err := r.etcdClient.Get(ctx, r.serviceKey(serviceName), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]register.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var si register.ServiceInstance
		if err := json.Unmarshal(kv.Value, &si); err != nil {
			if r.onServiceDecodeError != nil {
				// 回调用于观测脏数据来源 ( 具体 key )。
				// 主流程仍按原语义返回错误。
				r.onServiceDecodeError(serviceName, string(kv.Key), err)
			}
			return nil, err
		}
		res = append(res, si)
	}
	return res, nil
}

// Subscribe 订阅服务实例变更。
func (r *Registry) Subscribe(serviceName string) <-chan struct{} {
	return r.SubscribeWithContext(context.Background(), serviceName)
}

// SubscribeWithContext 允许调用方通过 ctx 控制订阅生命周期。
func (r *Registry) SubscribeWithContext(ctx context.Context, serviceName string) <-chan struct{} {
	if !isValidServiceName(serviceName) {
		// 因为接口 Subscribe 不返回 error，
		// 这里直接返回已关闭的 channel 快速失败。
		// 调用方可以通过 range/receive 立即感知订阅不可用。
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	// 使用 WithRequireLeader 确保 watch 操作在 leader 上执行。
	baseCtx := clientv3.WithRequireLeader(ctx)
	watchCtx, cancel := context.WithCancel(baseCtx)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	watchID := r.nextWatchID
	r.nextWatchID++
	r.watchCancel[watchID] = cancel
	r.mu.Unlock()

	// 监听服务目录下所有实例 key 的变更事件。
	watchChan := r.etcdClient.Watch(watchCtx, r.serviceKey(serviceName), clientv3.WithPrefix())

	// 使用带缓冲的 channel 避免慢消费者阻塞 watch goroutine。
	ch := make(chan struct{}, 1)
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.watchCancel, watchID)
			r.mu.Unlock()
			close(ch)
		}()

		for {
			select {
			case <-watchCtx.Done():
				return
			case resp, ok := <-watchChan:
				if !ok {
					return
				}
				if resp.Err() != nil {
					if r.onWatchError != nil {
						// watch 可能因 leader 变化、网络抖动临时失败。
						// 交由上层观测与告警。
						r.onWatchError(serviceName, resp.Err())
					}
					continue
				}
				if resp.Canceled {
					return
				}

				// 事件合并。
				select {
				case ch <- struct{}{}:
				default:
					if r.onWatchNotifyDrop != nil {
						// channel 已满时进行事件合并，避免通知风暴反压到底层 watch。
						r.onWatchNotifyDrop(serviceName)
					}
				}
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
	cancels := make([]context.CancelFunc, 0)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}

	r.closed = true
	for _, cancel := range r.watchCancel {
		cancels = append(cancels, cancel)
	}
	r.watchCancel = nil
	r.mu.Unlock()

	// 在锁外执行 cancel，避免回调链路或阻塞操作放大锁竞争。
	for _, cancel := range cancels {
		cancel()
	}
	return r.etcdSession.Close()
}

func isValidServiceName(serviceName string) bool {
	// 约束 serviceName 不含 "/" 避免污染层级路径语义。
	serviceName = strings.TrimSpace(serviceName)
	return serviceName != "" && !strings.Contains(serviceName, "/")
}
