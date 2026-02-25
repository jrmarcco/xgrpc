package rr

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"github.com/jrmarcco/xgrpc/client"
	"github.com/jrmarcco/xgrpc/register"
)

var _ resolver.Builder = (*ResolverBuilder)(nil)

type ResolverBuilder struct {
	registry register.Registry
	timeout  time.Duration

	onResolveError   func(serviceName string, err error)
	onResolveUpdated func(serviceName string, instanceCount int)
}

func NewResolverBuilder(registry register.Registry, timeout time.Duration) *ResolverBuilder {
	return &ResolverBuilder{
		registry: registry,
		timeout:  timeout,
	}
}

// OnResolveError 设置解析失败回调。
func (b *ResolverBuilder) OnResolveError(fn func(serviceName string, err error)) *ResolverBuilder {
	b.onResolveError = fn
	return b
}

// OnResolveUpdated 设置解析并更新状态成功回调。
func (b *ResolverBuilder) OnResolveUpdated(fn func(serviceName string, instanceCount int)) *ResolverBuilder {
	b.onResolveUpdated = fn
	return b
}

func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	watchCtx, watchCancel := context.WithCancel(context.Background())
	r := &Resolver{
		registry: b.registry,
		timeout:  b.timeout,

		target: target,
		cc:     cc,

		watchCtx:    watchCtx,
		watchCancel: watchCancel,

		onResolveError:   b.onResolveError,
		onResolveUpdated: b.onResolveUpdated,
	}

	r.resolve()
	go r.watch()
	return r, nil
}

func (b *ResolverBuilder) Scheme() string {
	return "registry"
}

var _ resolver.Resolver = (*Resolver)(nil)

// Resolver 是实现 gRPC 的自定义服务发现 Resolver。
type Resolver struct {
	stateMu   sync.Mutex
	resolveMu sync.Mutex

	registry register.Registry
	timeout  time.Duration

	target resolver.Target
	cc     resolver.ClientConn

	watchCtx    context.Context
	watchCancel context.CancelFunc
	closeOnce   sync.Once

	lastStateSig string

	onResolveError   func(serviceName string, err error)
	onResolveUpdated func(serviceName string, instanceCount int)
}

// resolve 解析服务端信息 ( 全量解析 )。
func (r *Resolver) resolve() {
	r.resolveMu.Lock()
	defer r.resolveMu.Unlock()

	ctx, cancel := context.WithTimeout(r.watchCtx, r.timeout)
	serviceName := r.endpoint()
	instances, err := r.registry.ListServices(ctx, serviceName)
	cancel()

	if err != nil {
		// resolver 已关闭时不上报由取消导致的错误。
		if r.watchCtx.Err() != nil {
			return
		}

		if r.onResolveError != nil {
			r.onResolveError(serviceName, err)
		}
		r.cc.ReportError(err)
		return
	}

	sort.Slice(instances, func(i, j int) bool {
		if instances[i].Addr != instances[j].Addr {
			return instances[i].Addr < instances[j].Addr
		}
		if instances[i].Name != instances[j].Name {
			return instances[i].Name < instances[j].Name
		}
		if instances[i].Group != instances[j].Group {
			return instances[i].Group < instances[j].Group
		}
		if instances[i].ReadWeight != instances[j].ReadWeight {
			return instances[i].ReadWeight < instances[j].ReadWeight
		}
		return instances[i].WriteWeight < instances[j].WriteWeight
	})

	// 构建 gRPC 地址列表。
	addrs := make([]resolver.Address, 0, len(instances))

	// 构建状态签名。
	var stateSigBuilder strings.Builder
	for _, inst := range instances {
		addrs = append(addrs, resolver.Address{
			Addr:       inst.Addr,
			ServerName: inst.Name,
			Attributes: attributes.New(client.AttrNameReadWeight, inst.ReadWeight).
				WithValue(client.AttrNameWriteWeight, inst.WriteWeight).
				WithValue(client.AttrNameGroup, inst.Group).
				WithValue(client.AttrNameNode, inst.Name),
		})

		_, _ = fmt.Fprintf(
			&stateSigBuilder,
			"%s|%s|%s|%d|%d;",
			inst.Addr,
			inst.Name,
			inst.Group,
			inst.ReadWeight,
			inst.WriteWeight,
		)
	}

	stateSig := stateSigBuilder.String()

	// 更新状态。
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	// 如果状态签名没有变化则直接返回。
	if r.lastStateSig == stateSig {
		return
	}

	if err = r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		if r.watchCtx.Err() != nil {
			return
		}
		if r.onResolveError != nil {
			r.onResolveError(serviceName, err)
		}
		r.cc.ReportError(err)
		return
	}

	// 更新状态签名。
	r.lastStateSig = stateSig
	if r.onResolveUpdated != nil {
		r.onResolveUpdated(serviceName, len(instances))
	}
}

// watch 监听注册中心变化，当发生变化时直接重新解析。
// 由于 gRPC 的 api 设计没办法实现单独更新某个节点，所以这里会直接进行全量解析。
func (r *Resolver) watch() {
	var events <-chan struct{}
	if contextSub, ok := r.registry.(register.ContextSubscriber); ok {
		events = contextSub.SubscribeWithContext(r.watchCtx, r.endpoint())
	} else {
		events = r.registry.Subscribe(r.endpoint())
	}

	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
			r.resolve()
		case <-r.watchCtx.Done():
			return
		}
	}
}

func (r *Resolver) endpoint() string {
	if ep := r.target.Endpoint(); ep != "" {
		return ep
	}
	if r.target.URL.Path != "" {
		return strings.TrimPrefix(r.target.URL.Path, "/")
	}
	return r.target.URL.Host
}

func (r *Resolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.resolve()
}

func (r *Resolver) Close() {
	r.closeOnce.Do(func() {
		r.watchCancel()
	})
}
