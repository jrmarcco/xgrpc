package br

import (
	"context"
	"testing"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/jrmarcco/xgrpc/client"
)

type testSubConn struct {
	balancer.SubConn
}

func buildInfoFromAddrs(addrs ...string) (pi base.PickerBuildInfo, scs map[string]balancer.SubConn) {
	ready := make(map[balancer.SubConn]base.SubConnInfo, len(addrs))
	scs = make(map[string]balancer.SubConn, len(addrs))

	for _, addr := range addrs {
		sc := &testSubConn{}
		ready[sc] = base.SubConnInfo{Address: resolver.Address{Addr: addr}}
		if addr != "" {
			scs[addr] = sc
		}
	}
	return base.PickerBuildInfo{ReadySCs: ready}, scs
}

func pickWithBizID(t *testing.T, p balancer.Picker, bizID uint64) balancer.SubConn {
	t.Helper()

	ctx := client.ContextWithBizId(context.Background(), bizID)
	res, err := p.Pick(balancer.PickInfo{Ctx: ctx})
	if err != nil {
		t.Fatalf("pick failed: %v", err)
	}
	return res.SubConn
}

func TestCHBalancerDynamicNodeSync(t *testing.T) {
	t.Parallel()

	builder := NewCHBalancerBuilder()
	builder.VirtualNodeCnt(16)

	info, byAddr := buildInfoFromAddrs("10.0.0.1:8080", "10.0.0.2:8080")
	picker := builder.Build(info)
	ch, ok := picker.(*CHBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	first := pickWithBizID(t, picker, 20260218)
	if first == nil {
		t.Fatalf("expected non-nil SubConn")
	}

	// 删除一个节点后，路由结果应只会落在剩余节点上。
	info2, byAddr2 := buildInfoFromAddrs("10.0.0.2:8080")
	picker2 := builder.Build(info2)
	second := pickWithBizID(t, picker2, 20260218)
	if second != byAddr2["10.0.0.2:8080"] {
		t.Fatalf("expected remaining node after removal")
	}

	stats := ch.SyncStats()
	if stats.Removed != 1 {
		t.Fatalf("expected 1 removed node, got %d", stats.Removed)
	}
	if stats.Added != 0 {
		t.Fatalf("expected 0 added nodes in second sync, got %d", stats.Added)
	}
	if stats.PhysicalNodeCount != 1 {
		t.Fatalf("expected 1 physical node, got %d", stats.PhysicalNodeCount)
	}

	_ = byAddr
}

func TestCHBalancerVirtualNodeCntFrozenAfterFirstBuild(t *testing.T) {
	t.Parallel()

	builder := NewCHBalancerBuilder()
	builder.VirtualNodeCnt(8)

	info, _ := buildInfoFromAddrs("10.0.0.1:8080")
	picker := builder.Build(info)

	ch, ok := picker.(*CHBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}
	if ch.virtualNodeCnt != 8 {
		t.Fatalf("expected virtual node cnt 8, got %d", ch.virtualNodeCnt)
	}

	// 首次 Build 后再修改应无效，保持环配置稳定。
	builder.VirtualNodeCnt(64)

	info2, _ := buildInfoFromAddrs("10.0.0.1:8080", "10.0.0.2:8080")
	_ = builder.Build(info2)

	if ch.virtualNodeCnt != 8 {
		t.Fatalf("expected frozen virtual node cnt 8, got %d", ch.virtualNodeCnt)
	}
	if len(ch.ring) != 16 {
		t.Fatalf("expected 16 virtual nodes, got %d", len(ch.ring))
	}
}

func TestCHBalancerIgnoreEmptyAddress(t *testing.T) {
	t.Parallel()

	builder := NewCHBalancerBuilder()
	builder.VirtualNodeCnt(4)

	info, byAddr := buildInfoFromAddrs("", "10.0.0.9:8080")
	picker := builder.Build(info)
	ch, ok := picker.(*CHBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	if len(ch.scs) != 1 {
		t.Fatalf("expected only one valid physical node, got %d", len(ch.scs))
	}
	if len(ch.ring) != 4 {
		t.Fatalf("expected 4 virtual nodes, got %d", len(ch.ring))
	}

	res := pickWithBizID(t, picker, 1)
	if res != byAddr["10.0.0.9:8080"] {
		t.Fatalf("expected pick on valid non-empty address")
	}
}
