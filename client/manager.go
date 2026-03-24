package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jrmarcco/jit/xsync"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
)

var (
	ErrManagerClosed               = errors.New("client manager is closed")
	ErrResolverBuilderRequired     = errors.New("resolver builder is required")
	ErrClientCreatorRequired       = errors.New("client creator is required")
	ErrTransportSecurityRequired   = errors.New("transport security is required")
	ErrServiceInstanceAddrRequired = errors.New("service instance address is required")
	ErrInvalidConnectTimeout       = errors.New("connect timeout must be >= 0")
)

// ManagerBuilder 是 gRPC 客户端管理器 builder。
// 用于构建 gRPC 客户端管理器。
type ManagerBuilder[T any] struct {
	rb resolver.Builder
	bb balancer.Builder

	insecure       bool
	transportCreds credentials.TransportCredentials

	keepaliveParams keepalive.ClientParameters

	dialOptions    []grpc.DialOption
	connectTimeout time.Duration

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
	b.transportCreds = nil
	return b
}

func (b *ManagerBuilder[T]) TransportCredentials(creds credentials.TransportCredentials) *ManagerBuilder[T] {
	b.transportCreds = creds
	if creds != nil {
		b.insecure = false
	}
	return b
}

func (b *ManagerBuilder[T]) KeepAlive(params keepalive.ClientParameters) *ManagerBuilder[T] {
	b.keepaliveParams = params
	return b
}

// DialOptions 允许注入额外 grpc.DialOption ( 例如 tracing/metrics 拦截器 )。
func (b *ManagerBuilder[T]) DialOptions(opts ...grpc.DialOption) *ManagerBuilder[T] {
	b.dialOptions = append(b.dialOptions, opts...)
	return b
}

// ConnectTimeout 开启首连等待 ( <=0 表示沿用 gRPC 默认异步建连行为 )。
func (b *ManagerBuilder[T]) ConnectTimeout(timeout time.Duration) *ManagerBuilder[T] {
	b.connectTimeout = timeout
	return b
}

func (b *ManagerBuilder[T]) Creator(creator func(conn *grpc.ClientConn) T) *ManagerBuilder[T] {
	b.creator = creator
	return b
}

func (b *ManagerBuilder[T]) Build() *Manager[T] {
	m := &Manager[T]{
		sg: &singleflight.Group{},

		rb: b.rb,
		bb: b.bb,

		insecure:       b.insecure,
		transportCreds: b.transportCreds,

		keepaliveParams: b.keepaliveParams,

		dialOptions:    append([]grpc.DialOption(nil), b.dialOptions...),
		connectTimeout: b.connectTimeout,

		creator: b.creator,
	}
	m.configErr = m.validateConfig()
	return m
}

type clientEntry[T any] struct {
	client T
	cc     *grpc.ClientConn
}

// Manager 是一个按服务名缓存 gRPC 客户端的范型管理器。
// 目标是“懒加载 + 连接复用 + 可统一关闭”。
type Manager[T any] struct {
	sg *singleflight.Group

	clients xsync.Map[string, *clientEntry[T]]

	balancerRegistered atomic.Bool

	rb resolver.Builder
	bb balancer.Builder

	insecure       bool
	transportCreds credentials.TransportCredentials

	keepaliveParams keepalive.ClientParameters

	dialOptions    []grpc.DialOption
	connectTimeout time.Duration

	// closed 标记管理器是否已进入关闭态 ( CloseAll 后不再允许 Get )。
	closed    atomic.Bool
	configErr error

	creator func(conn *grpc.ClientConn) T
}

// Get 获取指定服务名的客户端。
func (m *Manager[T]) Get(serviceName string) (T, error) {
	if m.closed.Load() {
		var zero T
		return zero, fmt.Errorf("[client-manager] %w", ErrManagerClosed)
	}

	if err := m.configErr; err != nil {
		var zero T
		return zero, err
	}

	if entry, loaded := m.clients.Load(serviceName); loaded {
		return entry.client, nil
	}

	client, err, _ := m.sg.Do(serviceName, func() (any, error) {
		cc, err := m.dial(serviceName)
		if err != nil {
			return nil, fmt.Errorf("[client-manager] failed to create grpc client connection for service %s: %w", serviceName, err)
		}
		if m.closed.Load() {
			_ = cc.Close()
			return nil, fmt.Errorf("[client-manager] %w", ErrManagerClosed)
		}

		client := m.creator(cc)
		entry := &clientEntry[T]{
			client: client,
			cc:     cc,
		}

		m.clients.Store(serviceName, entry)
		return entry, nil
	})

	if err != nil {
		var zero T
		return zero, err
	}

	entry, ok := client.(*clientEntry[T])
	if !ok {
		var zero T
		return zero, fmt.Errorf("[client-manager] failed to convert cached entry to expected type")
	}

	return entry.client, nil
}

// dial 拨号连接指定服务。
func (m *Manager[T]) dial(serviceName string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithResolvers(m.rb),
		grpc.WithNoProxy(),
		grpc.WithKeepaliveParams(m.keepaliveParams),
	}

	if m.transportCreds != nil {
		opts = append(opts, grpc.WithTransportCredentials(m.transportCreds))
	} else if m.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if m.bb != nil {
		if !m.balancerRegistered.Load() {
			balancer.Register(m.bb)
			m.balancerRegistered.Store(true)
		}
		opts = append(opts, grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingPolicy": %q}`, m.bb.Name()),
		))
	}
	if len(m.dialOptions) > 0 {
		opts = append(opts, m.dialOptions...)
	}

	addr := fmt.Sprintf("%s:///%s", m.rb.Scheme(), serviceName)
	cc, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	if m.connectTimeout > 0 {
		// 等待连接就绪。
		if err := waitForReady(cc, m.connectTimeout); err != nil {
			_ = cc.Close()
			return nil, fmt.Errorf("[client-manager] failed to connect to service %s within %s: %w", serviceName, m.connectTimeout, err)
		}
	}
	return cc, nil
}

// GetByAddr 获取指定服务实例地址的客户端。
func (m *Manager[T]) GetByAddr(serviceName, addr string) (T, error) {
	if m.closed.Load() {
		var zero T
		return zero, fmt.Errorf("[client-manager] %w", ErrManagerClosed)
	}

	if err := m.configErr; err != nil {
		var zero T
		return zero, err
	}

	if addr == "" {
		var zero T
		return zero, fmt.Errorf("[client-manager] %w", ErrServiceInstanceAddrRequired)
	}

	key := addrCacheKey(serviceName, addr)
	if entry, loaded := m.clients.Load(key); loaded {
		return entry.client, nil
	}

	client, err, _ := m.sg.Do(key, func() (any, error) {
		cc, err := m.dialAddr(serviceName, addr)
		if err != nil {
			return nil, fmt.Errorf("[client-manager] failed to create grpc client connection for service instance %s(%s): %w", serviceName, addr, err)
		}
		if m.closed.Load() {
			_ = cc.Close()
			return nil, fmt.Errorf("[client-manager] %w", ErrManagerClosed)
		}

		client := m.creator(cc)
		entry := &clientEntry[T]{
			client: client,
			cc:     cc,
		}

		m.clients.Store(key, entry)
		return entry, nil
	})

	if err != nil {
		var zero T
		return zero, err
	}

	entry, ok := client.(*clientEntry[T])
	if !ok {
		var zero T
		return zero, fmt.Errorf("[client-manager] failed to convert cached entry to expected type")
	}

	return entry.client, nil
}

// dialAddr 拨号连接指定地址。
func (m *Manager[T]) dialAddr(serviceName, addr string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithNoProxy(),
		grpc.WithKeepaliveParams(m.keepaliveParams),
	}

	if m.transportCreds != nil {
		opts = append(opts, grpc.WithTransportCredentials(m.transportCreds))
	} else if m.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if len(m.dialOptions) > 0 {
		opts = append(opts, m.dialOptions...)
	}

	cc, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	if m.connectTimeout > 0 {
		if err := waitForReady(cc, m.connectTimeout); err != nil {
			_ = cc.Close()
			return nil, fmt.Errorf("[client-manager] failed to connect to service instance %s(%s) within %s: %w", serviceName, addr, m.connectTimeout, err)
		}
	}
	return cc, nil
}

func (m *Manager[T]) validateConfig() error {
	if m.rb == nil {
		return fmt.Errorf("[client-manager] %w", ErrResolverBuilderRequired)
	}
	if m.creator == nil {
		return fmt.Errorf("[client-manager] %w", ErrClientCreatorRequired)
	}
	if !m.insecure && m.transportCreds == nil {
		return fmt.Errorf("[client-manager] %w: call Insecure() or TransportCredentials()", ErrTransportSecurityRequired)
	}
	if m.connectTimeout < 0 {
		return fmt.Errorf("[client-manager] %w", ErrInvalidConnectTimeout)
	}
	return nil
}

// Close 关闭指定服务连接。
func (m *Manager[T]) Close(serviceName string) error {
	entry, ok := m.clients.LoadAndDelete(serviceName)
	if !ok {
		return nil
	}

	// 直接关闭连接。
	if entry.cc != nil {
		return entry.cc.Close()
	}

	return nil
}

// CloseByAddr 关闭指定服务实例连接。
func (m *Manager[T]) CloseByAddr(serviceName, addr string) error {
	if addr == "" {
		return nil
	}

	entry, ok := m.clients.LoadAndDelete(addrCacheKey(serviceName, addr))
	if !ok {
		return nil
	}

	if entry.cc != nil {
		return entry.cc.Close()
	}
	return nil
}

func addrCacheKey(serviceName, addr string) string {
	return fmt.Sprintf("instance:%s@%s", serviceName, addr)
}

// CloseAll 关闭所有连接。
func (m *Manager[T]) CloseAll() error {
	m.closed.Store(true)

	var errs []error
	m.clients.Range(func(serviceName string, entry *clientEntry[T]) bool {
		// 直接关闭连接。
		if entry.cc != nil {
			if err := entry.cc.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close connection for %s: %w", serviceName, err))
			}
		}
		m.clients.Delete(serviceName)
		return true
	})

	if len(errs) > 0 {
		return fmt.Errorf("[client-manager] errors closing connections: %w", errors.Join(errs...))
	}
	return nil
}

func waitForReady(cc *grpc.ClientConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cc.Connect()
	for {
		state := cc.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Idle, connectivity.Connecting, connectivity.TransientFailure:
			// Keep waiting for state transitions until Ready/Shutdown/timeout.
		case connectivity.Shutdown:
			return fmt.Errorf("[client-manager] connection is shutdown")
		default:
			return fmt.Errorf("[client-manager] unexpected connection state: %v", state)
		}
		if !cc.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("[client-manager] connection state did not change")
		}
	}
}
