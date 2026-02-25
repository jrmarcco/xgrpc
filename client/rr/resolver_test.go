package rr

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/jrmarcco/xgrpc/register"
)

type fakeRegistry struct {
	mu        sync.Mutex
	instances []register.ServiceInstance
	listFn    func(ctx context.Context, serviceName string) ([]register.ServiceInstance, error)
}

func (f *fakeRegistry) Register(context.Context, register.ServiceInstance) error { return nil }

func (f *fakeRegistry) Unregister(context.Context, register.ServiceInstance) error { return nil }

func (f *fakeRegistry) ListServices(ctx context.Context, serviceName string) ([]register.ServiceInstance, error) {
	if f.listFn != nil {
		return f.listFn(ctx, serviceName)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	res := make([]register.ServiceInstance, len(f.instances))
	copy(res, f.instances)
	return res, nil
}

func (f *fakeRegistry) Subscribe(string) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (f *fakeRegistry) Close() error { return nil }

type fakeClientConn struct {
	mu              sync.Mutex
	updateCount     int
	reportErrCount  int
	lastReportedErr error
}

func (f *fakeClientConn) UpdateState(resolver.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCount++
	return nil
}

func (f *fakeClientConn) ReportError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reportErrCount++
	f.lastReportedErr = err
}

func (f *fakeClientConn) NewAddress([]resolver.Address) {}

func (f *fakeClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult { return nil }

func (f *fakeClientConn) updateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateCount
}

func (f *fakeClientConn) reportErrorCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reportErrCount
}

func TestResolverResolve_SkipDuplicateStateUpdate(t *testing.T) {
	t.Parallel()

	registry := &fakeRegistry{
		instances: []register.ServiceInstance{
			{Name: "svc", Addr: "127.0.0.1:8081", Group: "g1", ReadWeight: 10, WriteWeight: 20},
			{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
		},
	}
	cc := &fakeClientConn{}

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Resolver{
		registry:    registry,
		timeout:     time.Second,
		cc:          cc,
		watchCtx:    watchCtx,
		watchCancel: cancel,
	}

	r.resolve()
	if got := cc.updateCalls(); got != 1 {
		t.Fatalf("first resolve should update state once, got %d", got)
	}

	// Same instance set but with different order should be deduplicated.
	registry.mu.Lock()
	registry.instances = []register.ServiceInstance{
		{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
		{Name: "svc", Addr: "127.0.0.1:8081", Group: "g1", ReadWeight: 10, WriteWeight: 20},
	}
	registry.mu.Unlock()

	r.resolve()
	if got := cc.updateCalls(); got != 1 {
		t.Fatalf("duplicate resolve should not update state again, got %d", got)
	}

	registry.mu.Lock()
	registry.instances = []register.ServiceInstance{
		{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
		{Name: "svc", Addr: "127.0.0.1:8082", Group: "g1", ReadWeight: 10, WriteWeight: 20},
	}
	registry.mu.Unlock()

	r.resolve()
	if got := cc.updateCalls(); got != 2 {
		t.Fatalf("changed instance set should update state again, got %d", got)
	}
}

func TestResolverResolveNow_SkipDuplicateStateUpdate(t *testing.T) {
	t.Parallel()

	registry := &fakeRegistry{
		instances: []register.ServiceInstance{
			{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
		},
	}
	cc := &fakeClientConn{}

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Resolver{
		registry:    registry,
		timeout:     time.Second,
		cc:          cc,
		watchCtx:    watchCtx,
		watchCancel: cancel,
	}

	r.ResolveNow(resolver.ResolveNowOptions{})
	if got := cc.updateCalls(); got != 1 {
		t.Fatalf("first ResolveNow should update state once, got %d", got)
	}

	r.ResolveNow(resolver.ResolveNowOptions{})
	if got := cc.updateCalls(); got != 1 {
		t.Fatalf("duplicate ResolveNow should not update state again, got %d", got)
	}
}

func TestResolverResolve_CloseSuppressesCanceledError(t *testing.T) {
	t.Parallel()

	registry := &fakeRegistry{
		listFn: func(ctx context.Context, _ string) ([]register.ServiceInstance, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	cc := &fakeClientConn{}

	watchCtx, cancel := context.WithCancel(context.Background())
	r := &Resolver{
		registry:    registry,
		timeout:     time.Second,
		cc:          cc,
		watchCtx:    watchCtx,
		watchCancel: cancel,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.resolve()
	}()

	r.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resolve should return quickly after Close")
	}

	if got := cc.reportErrorCalls(); got != 0 {
		t.Fatalf("canceled resolve should not report error, got %d", got)
	}
}

func TestResolverResolve_ListErrorReportsError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("list failed")
	registry := &fakeRegistry{
		listFn: func(context.Context, string) ([]register.ServiceInstance, error) {
			return nil, expectedErr
		},
	}
	cc := &fakeClientConn{}

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Resolver{
		registry:    registry,
		timeout:     time.Second,
		cc:          cc,
		watchCtx:    watchCtx,
		watchCancel: cancel,
	}

	r.resolve()
	if got := cc.reportErrorCalls(); got != 1 {
		t.Fatalf("list failure should report exactly one error, got %d", got)
	}
	if !errors.Is(cc.lastReportedErr, expectedErr) {
		t.Fatalf("reported error mismatch, expected %v, got %v", expectedErr, cc.lastReportedErr)
	}
}

func TestResolverEndpoint_PreferEndpointMethod(t *testing.T) {
	t.Parallel()

	r := &Resolver{
		target: resolver.Target{
			URL: url.URL{
				Scheme: "registry",
				Host:   "svc-host",
				Path:   "/svc-path",
			},
		},
	}

	if got := r.endpoint(); got != "svc-path" {
		t.Fatalf("endpoint should prefer Target.Endpoint(), got %q", got)
	}
}

func TestResolverEndpoint_FallbackToHostWhenPathEmpty(t *testing.T) {
	t.Parallel()

	r := &Resolver{
		target: resolver.Target{
			URL: url.URL{
				Scheme: "registry",
				Host:   "svc-host",
			},
		},
	}

	if got := r.endpoint(); got != "svc-host" {
		t.Fatalf("endpoint should fallback to URL host when path empty, got %q", got)
	}
}

func TestResolverResolve_SerializedByMutex(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	active := 0
	maxActive := 0
	enter := make(chan struct{}, 2)
	release := make(chan struct{})

	registry := &fakeRegistry{
		listFn: func(context.Context, string) ([]register.ServiceInstance, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()

			enter <- struct{}{}
			<-release

			mu.Lock()
			active--
			mu.Unlock()

			return []register.ServiceInstance{
				{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
			}, nil
		},
	}

	cc := &fakeClientConn{}
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Resolver{
		registry:    registry,
		timeout:     time.Second,
		cc:          cc,
		watchCtx:    watchCtx,
		watchCancel: cancel,
	}

	done := make(chan struct{}, 2)
	go func() {
		r.resolve()
		done <- struct{}{}
	}()
	go func() {
		r.resolve()
		done <- struct{}{}
	}()

	<-enter
	select {
	case <-enter:
		t.Fatal("second resolve entered registry concurrently, expected serialization")
	case <-time.After(100 * time.Millisecond):
	}

	release <- struct{}{}
	<-enter
	release <- struct{}{}
	<-done
	<-done

	mu.Lock()
	gotMaxActive := maxActive
	mu.Unlock()
	if gotMaxActive != 1 {
		t.Fatalf("expected max concurrent ListServices calls to be 1, got %d", gotMaxActive)
	}
}

func TestResolverResolve_ObservabilityCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("resolve updated callback", func(t *testing.T) {
		t.Parallel()

		registry := &fakeRegistry{
			instances: []register.ServiceInstance{
				{Name: "svc", Addr: "127.0.0.1:8080", Group: "g1", ReadWeight: 10, WriteWeight: 20},
			},
		}
		cc := &fakeClientConn{}

		var gotService string
		var gotCount int
		watchCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &Resolver{
			registry:    registry,
			timeout:     time.Second,
			cc:          cc,
			watchCtx:    watchCtx,
			watchCancel: cancel,
			onResolveUpdated: func(serviceName string, instanceCount int) {
				gotService = serviceName
				gotCount = instanceCount
			},
			target: resolver.Target{
				URL: url.URL{
					Scheme: "registry",
					Path:   "/svc",
				},
			},
		}

		r.resolve()
		if gotService != "svc" || gotCount != 1 {
			t.Fatalf("unexpected callback payload, service=%q count=%d", gotService, gotCount)
		}
	})

	t.Run("resolve error callback", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("list failed")
		registry := &fakeRegistry{
			listFn: func(context.Context, string) ([]register.ServiceInstance, error) {
				return nil, expectedErr
			},
		}
		cc := &fakeClientConn{}

		var gotErr error
		var gotService string
		watchCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &Resolver{
			registry:    registry,
			timeout:     time.Second,
			cc:          cc,
			watchCtx:    watchCtx,
			watchCancel: cancel,
			onResolveError: func(serviceName string, err error) {
				gotService = serviceName
				gotErr = err
			},
			target: resolver.Target{
				URL: url.URL{
					Scheme: "registry",
					Path:   "/svc",
				},
			},
		}

		r.resolve()
		if gotService != "svc" || !errors.Is(gotErr, expectedErr) {
			t.Fatalf("unexpected error callback payload, service=%q err=%v", gotService, gotErr)
		}
	})
}
