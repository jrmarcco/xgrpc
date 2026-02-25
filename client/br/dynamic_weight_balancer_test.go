package br

import (
	"errors"
	"testing"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func pickOnce(t *testing.T, p balancer.Picker, doneErr error) {
	t.Helper()

	res, err := p.Pick(balancer.PickInfo{})
	if err != nil {
		t.Fatalf("pick failed: %v", err)
	}
	if res.Done == nil {
		t.Fatalf("expected non-nil done callback")
	}

	res.Done(balancer.DoneInfo{Err: doneErr})
}

func getDynamicNode(t *testing.T, picker *DynamicWeightBalancer, addr string) *dynamicServiceNode {
	t.Helper()

	picker.mu.RLock()
	node, ok := picker.nodes[addr]
	picker.mu.RUnlock()
	if !ok {
		t.Fatalf("node not found: %s", addr)
	}
	return node
}

func TestDynamicWeightBalancerRecoverWeightWithUpperBound(t *testing.T) {
	t.Parallel()

	builder := NewDynamicWeightBalancerBuilder()
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 3,
	})
	p := builder.Build(info)
	picker, ok := p.(*DynamicWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	// 先通过错误回调把有效权重降到 1，再验证成功回调可逐步恢复且不超过静态权重。
	pickOnce(t, p, status.Error(codes.Unavailable, "temporary unavailable"))
	for range 10 {
		pickOnce(t, p, nil)
	}

	node := getDynamicNode(t, picker, "10.0.0.1:8080")
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.efficientWeight != 3 {
		t.Fatalf("expected effective weight to recover and cap at 3, got %d", node.efficientWeight)
	}
}

func TestDynamicWeightBalancerDegradeOnErrors(t *testing.T) {
	t.Parallel()

	builder := NewDynamicWeightBalancerBuilder()
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8081": 3,
	})
	p := builder.Build(info)
	picker, ok := p.(*DynamicWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	pickOnce(t, p, errors.New("transient error"))
	node := getDynamicNode(t, picker, "10.0.0.1:8081")
	node.mu.Lock()
	gotAfterGeneric := node.efficientWeight
	node.mu.Unlock()
	if gotAfterGeneric != 2 {
		t.Fatalf("expected generic error to decrement effective weight to 2, got %d", gotAfterGeneric)
	}

	pickOnce(t, p, status.Error(codes.Unavailable, "node unavailable"))
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.efficientWeight != 1 {
		t.Fatalf("expected unavailable error to degrade effective weight to 1, got %d", node.efficientWeight)
	}
}

func TestDynamicWeightBalancerClampEffectiveWeightOnWeightUpdate(t *testing.T) {
	t.Parallel()

	builder := NewDynamicWeightBalancerBuilder()
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 10,
	})
	p := builder.Build(info)
	picker, ok := p.(*DynamicWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 2,
	})
	_ = builder.Build(info2)

	node := getDynamicNode(t, picker, "10.0.0.1:8080")
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.weight != 2 {
		t.Fatalf("expected static weight to update to 2, got %d", node.weight)
	}
	if node.efficientWeight != 2 {
		t.Fatalf("expected effective weight to clamp to 2 after update, got %d", node.efficientWeight)
	}
}

func TestDynamicWeightBalancerDefaultWeightWhenAttributeMissing(t *testing.T) {
	t.Parallel()

	builder := NewDynamicWeightBalancerBuilder()
	info, _ := buildInfoFromAddrs("10.0.0.1:8080")
	p := builder.Build(info)
	picker, ok := p.(*DynamicWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	res, err := p.Pick(balancer.PickInfo{})
	if err != nil {
		t.Fatalf("pick failed: %v", err)
	}
	if res.SubConn == nil {
		t.Fatalf("expected non-nil SubConn")
	}

	node := getDynamicNode(t, picker, "10.0.0.1:8080")
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.weight != 1 {
		t.Fatalf("expected default weight 1 when attribute missing, got %d", node.weight)
	}
	if node.efficientWeight != 1 {
		t.Fatalf("expected default effective weight 1 when attribute missing, got %d", node.efficientWeight)
	}
}
