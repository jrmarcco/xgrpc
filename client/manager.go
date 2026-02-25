package client

import (
	"fmt"
	"time"

	"github.com/jrmarcco/jit/xsync"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
)

// ManagerBuilder 是 gRPC 客户端管理器 builder。
// 用于构建 gRPC 客户端管理器。
type ManagerBuilder[T any] struct {
	rb resolver.Builder
	bb balancer.Builder

	insecure        bool
	keepaliveParams keepalive.ClientParameters

	creator func(conn *grpc.ClientConn) T
}

func NewManagerBuilder[T any](rb resolver.Builder, bb balancer.Builder, creator func(conn *grpc.ClientConn) T) *ManagerBuilder[T] {
	const defaultPingTimeout = 10 * time.Second
	return &ManagerBuilder[T]{
		rb:       rb,
		bb:       bb,
		creator:  creator,
		insecure: false,
		keepaliveParams: keepalive.ClientParameters{
			Time:                time.Minute,
			Timeout:             defaultPingTimeout,
			PermitWithoutStream: true,
		},
	}
}

func (b *ManagerBuilder[T]) ResolverBuilder(rb resolver.Builder) *ManagerBuilder[T] {
	b.rb = rb
	return b
}

func (b *ManagerBuilder[T]) BalancerBuilder(bb balancer.Builder) *ManagerBuilder[T] {
	b.bb = bb
	return b
}

func (b *ManagerBuilder[T]) Insecure() *ManagerBuilder[T] {
	b.insecure = true
	return b
}

func (b *ManagerBuilder[T]) KeepAlive(params keepalive.ClientParameters) *ManagerBuilder[T] {
	b.keepaliveParams = params
	return b
}

func (b *ManagerBuilder[T]) Creator(creator func(conn *grpc.ClientConn) T) *ManagerBuilder[T] {
	b.creator = creator
	return b
}

func (b *ManagerBuilder[T]) Build() *Manager[T] {
	return &Manager[T]{
		rb:              b.rb,
		bb:              b.bb,
		insecure:        b.insecure,
		keepaliveParams: b.keepaliveParams,
		creator:         b.creator,
		sg:              &singleflight.Group{},
	}
}

type clientEntry[T any] struct {
	client T
	conn   *grpc.ClientConn
}

// Manager 是 gRPC 客户端管理器。
// 用于管理 gRPC 客户端连接。
type Manager[T any] struct {
	sg *singleflight.Group

	clients xsync.Map[string, *clientEntry[T]]

	rb resolver.Builder
	bb balancer.Builder

	insecure        bool
	keepaliveParams keepalive.ClientParameters

	creator func(conn *grpc.ClientConn) T
}

// Get 获取指定服务名的客户端。
func (m *Manager[T]) Get(serviceName string) (T, error) {
	if entry, loaded := m.clients.Load(serviceName); loaded {
		return entry.client, nil
	}

	client, err, _ := m.sg.Do(serviceName, func() (any, error) {
		cc, err := m.dial(serviceName)
		if err != nil {
			return nil, fmt.Errorf("[client-manager] failed to create grpc client connection for service %s: %w", serviceName, err)
		}

		client := m.creator(cc)
		entry := &clientEntry[T]{
			client: client,
			conn:   cc,
		}

		m.clients.Store(serviceName, entry)
		return client, nil
	})

	if err != nil {
		var zero T
		return zero, err
	}

	res, ok := client.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("[client-manager] failed to convert client to type T")
	}

	return res, nil
}

// dial 拨号连接指定服务。
func (m *Manager[T]) dial(serviceName string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithResolvers(m.rb),
		grpc.WithNoProxy(),
		grpc.WithKeepaliveParams(m.keepaliveParams),
	}

	if m.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if m.bb != nil {
		opts = append(opts, grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingPolicy": %q}`, m.bb.Name()),
		))
	}

	addr := fmt.Sprintf("%s:///%s", m.rb.Scheme(), serviceName)
	return grpc.NewClient(addr, opts...)
}

// Close 关闭指定服务连接。
func (m *Manager[T]) Close(serviceName string) error {
	entry, ok := m.clients.LoadAndDelete(serviceName)
	if !ok {
		return nil
	}

	// 直接关闭连接。
	if entry.conn != nil {
		return entry.conn.Close()
	}

	return nil
}

// CloseAll 关闭所有连接。
func (m *Manager[T]) CloseAll() error {
	var errs []error

	m.clients.Range(func(serviceName string, entry *clientEntry[T]) bool {
		// 直接关闭连接。
		if entry.conn != nil {
			if err := entry.conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close connection for %s: %w", serviceName, err))
			}
		}
		m.clients.Delete(serviceName)
		return true
	})

	if len(errs) > 0 {
		return fmt.Errorf("[client-manager] errors closing connections: %v", errs)
	}
	return nil
}
