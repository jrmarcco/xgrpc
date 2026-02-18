package rr

import (
	"context"
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
}

func NewResolverBuilder(registry register.Registry, timeout time.Duration) *ResolverBuilder {
	return &ResolverBuilder{
		registry: registry,
		timeout:  timeout,
	}
}

func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	r := &Resolver{
		registry: b.registry,
		timeout:  b.timeout,
		target:   target,
		cc:       cc,
		ch:       make(chan struct{}),
	}

	r.resolve()
	go r.watch()
	return r, nil
}

func (b *ResolverBuilder) Scheme() string {
	return "registry"
}

var _ resolver.Resolver = (*Resolver)(nil)

type Resolver struct {
	registry register.Registry
	timeout  time.Duration

	target resolver.Target
	cc     resolver.ClientConn

	ch chan struct{} // 用于控制 watch 方法的退出
}

// resolve 解析服务端信息，这里是全量解析。
func (r *Resolver) resolve() {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	instances, err := r.registry.ListServices(ctx, r.target.Endpoint())
	cancel()

	if err != nil {
		r.cc.ReportError(err)
		return
	}

	addrs := make([]resolver.Address, 0, len(instances))
	for _, inst := range instances {
		addrs = append(addrs, resolver.Address{
			Addr:       inst.Addr,
			ServerName: inst.Name,
			Attributes: attributes.New(client.AttrNameReadWeight, inst.ReadWeight).
				WithValue(client.AttrNameWriteWeight, inst.WriteWeight).
				WithValue(client.AttrNameGroup, inst.Group).
				WithValue(client.AttrNameNode, inst.Name),
		})
	}

	if err = r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		r.cc.ReportError(err)
		return
	}
}

// watch 监听注册中心变化，当发生变化时直接重新解析。
// 由于 grpc 的 api 设计没办法实现单独更新某个节点，所以这里会直接进行全量解析。
func (r *Resolver) watch() {
	events := r.registry.Subscribe(r.target.Endpoint())
	for {
		select {
		case <-events:
			r.resolve()
		case <-r.ch:
			return
		}
	}
}

func (r *Resolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.resolve()
}

func (r *Resolver) Close() {
	close(r.ch)
}
