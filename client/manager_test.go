package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type noopResolverBuilder struct{}

func (noopResolverBuilder) Build(resolver.Target, resolver.ClientConn, resolver.BuildOptions) (resolver.Resolver, error) {
	return noopResolver{}, nil
}

func (noopResolverBuilder) Scheme() string { return "xgrpc-test" }

type noopResolver struct{}

func (noopResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (noopResolver) Close()                                {}

func TestManager_Get_ConcurrentSingleflight(t *testing.T) {
	t.Parallel()

	var creatorCalls atomic.Int32
	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn {
			creatorCalls.Add(1)
			return conn
		},
	).Insecure().Build()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]*grpc.ClientConn, goroutines)
	errs := make([]error, goroutines)

	for i := range goroutines {
		go func() {
			defer wg.Done()
			results[i], errs[i] = m.Get("svc-a")
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Get failed at index %d: %v", i, err)
		}
	}
	if creatorCalls.Load() != 1 {
		t.Fatalf("creator should be called once, got %d", creatorCalls.Load())
	}
	first := results[0]
	for i := 1; i < len(results); i++ {
		if results[i] != first {
			t.Fatalf("expected same cached client pointer at index %d", i)
		}
	}

	if err := m.CloseAll(); err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}
}

func TestManager_Get_ValidateConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		mgr  *Manager[*grpc.ClientConn]
	}{
		{
			name: "missing resolver",
			mgr: NewManagerBuilder(
				nil,
				nil,
				func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
			).Insecure().Build(),
		},
		{
			name: "missing creator",
			mgr: NewManagerBuilder[*grpc.ClientConn](
				noopResolverBuilder{},
				nil,
				nil,
			).Insecure().Build(),
		},
		{
			name: "missing transport security config",
			mgr: NewManagerBuilder(
				noopResolverBuilder{},
				nil,
				func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
			).Build(),
		},
		{
			name: "negative connect timeout",
			mgr: NewManagerBuilder(
				noopResolverBuilder{},
				nil,
				func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
			).Insecure().ConnectTimeout(-time.Second).Build(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.mgr.Get("svc-a")
			if err == nil {
				t.Fatalf("expected Get to fail")
			}
		})
	}
}

func TestManager_Get_ValidateConfig_ErrorSentinel(t *testing.T) {
	t.Parallel()

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Build()

	_, err := m.Get("svc-a")
	if !errors.Is(err, ErrTransportSecurityRequired) {
		t.Fatalf("expected ErrTransportSecurityRequired, got: %v", err)
	}
}

func TestManagerBuilder_DialOptionsAndTimeout(t *testing.T) {
	t.Parallel()

	interceptor := func(
		ctx context.Context,
		method string,
		req any,
		reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	timeout := 2 * time.Second

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Insecure().DialOptions(grpc.WithUnaryInterceptor(interceptor)).ConnectTimeout(timeout).Build()

	if len(m.dialOptions) != 1 {
		t.Fatalf("expected 1 additional dial option, got %d", len(m.dialOptions))
	}
	if m.connectTimeout != timeout {
		t.Fatalf("expected connect timeout %s, got %s", timeout, m.connectTimeout)
	}
}

func TestManager_Get_ConnectTimeout(t *testing.T) {
	t.Parallel()

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Insecure().ConnectTimeout(20 * time.Millisecond).Build()

	_, err := m.Get("svc-timeout")
	if err == nil {
		t.Fatalf("expected connect timeout error")
	}
	if !strings.Contains(err.Error(), "within") {
		t.Fatalf("expected timeout details in error, got: %v", err)
	}
}

func TestManager_CloseAll_PreventsFutureGet(t *testing.T) {
	t.Parallel()

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Insecure().Build()

	if _, err := m.Get("svc-a"); err != nil {
		t.Fatalf("initial Get failed: %v", err)
	}
	if err := m.CloseAll(); err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}
	_, err := m.Get("svc-a")
	if err == nil {
		t.Fatalf("expected Get to fail after CloseAll")
	}
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("expected ErrManagerClosed, got: %v", err)
	}
}

func TestManager_CloseAll_Idempotent(t *testing.T) {
	t.Parallel()

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Insecure().Build()

	if err := m.CloseAll(); err != nil {
		t.Fatalf("first CloseAll failed: %v", err)
	}
	if err := m.CloseAll(); err != nil {
		t.Fatalf("second CloseAll failed: %v", err)
	}
}

func TestManager_Close_ThenRecreate(t *testing.T) {
	t.Parallel()

	var creatorCalls atomic.Int32
	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn {
			creatorCalls.Add(1)
			return conn
		},
	).Insecure().Build()

	first, err := m.Get("svc-a")
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	err = m.Close("svc-a")
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	second, err := m.Get("svc-a")
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}

	if creatorCalls.Load() != 2 {
		t.Fatalf("creator should be called twice, got %d", creatorCalls.Load())
	}

	if first == second {
		t.Fatalf("expected recreated client connection after Close")
	}

	err = m.CloseAll()
	if err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}
}

func TestManager_Get_NegativeConnectTimeout_ErrorSentinel(t *testing.T) {
	t.Parallel()

	m := NewManagerBuilder(
		noopResolverBuilder{},
		nil,
		func(conn *grpc.ClientConn) *grpc.ClientConn { return conn },
	).Insecure().ConnectTimeout(-time.Second).Build()

	_, err := m.Get("svc-a")
	if !errors.Is(err, ErrInvalidConnectTimeout) {
		t.Fatalf("expected ErrInvalidConnectTimeout, got: %v", err)
	}
}
