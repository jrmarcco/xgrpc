package br

import (
	"context"
	"testing"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/jrmarcco/xgrpc/client"
)

func buildInfoFromAddrWeights(addrWeights map[string]uint32) (pi base.PickerBuildInfo, scs map[string]balancer.SubConn) {
	ready := make(map[balancer.SubConn]base.SubConnInfo, len(addrWeights))
	scs = make(map[string]balancer.SubConn, len(addrWeights))

	for addr, weight := range addrWeights {
		sc := &testSubConn{}
		ready[sc] = base.SubConnInfo{
			Address: resolver.Address{
				Addr:       addr,
				Attributes: attributes.New(client.AttrNameWeight, weight),
			},
		}
		scs[addr] = sc
	}
	return base.PickerBuildInfo{ReadySCs: ready}, scs
}

type rwNodeSpec struct {
	addr        string
	nodeName    string
	group       string
	readWeight  uint32
	writeWeight uint32
}

func buildInfoFromRwNodes(nodes []rwNodeSpec) (pi base.PickerBuildInfo, scs map[string]balancer.SubConn) {
	ready := make(map[balancer.SubConn]base.SubConnInfo, len(nodes))
	scs = make(map[string]balancer.SubConn, len(nodes))

	for _, node := range nodes {
		sc := &testSubConn{}
		ready[sc] = base.SubConnInfo{
			Address: resolver.Address{
				Addr: node.addr,
				Attributes: attributes.New(client.AttrNameReadWeight, node.readWeight).
					WithValue(client.AttrNameWriteWeight, node.writeWeight).
					WithValue(client.AttrNameGroup, node.group).
					WithValue(client.AttrNameNode, node.nodeName),
			},
		}
		scs[node.nodeName] = sc
	}
	return base.PickerBuildInfo{ReadySCs: ready}, scs
}

func TestHashBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &HashBalancerBuilder{}
	info, _ := buildInfoFromAddrs("10.0.0.1:8080", "10.0.0.2:8080")

	p1 := builder.Build(info)
	hashPicker, ok := p1.(*HashBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrs("10.0.0.2:8080")
	p2 := builder.Build(info2)

	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	got := pickWithBizID(t, p2, 123)
	if got != byAddr2["10.0.0.2:8080"] {
		t.Fatalf("expected remaining node after sync")
	}

	hashPicker.mu.RLock()
	if len(hashPicker.addrs) != 1 {
		hashPicker.mu.RUnlock()
		t.Fatalf("expected one address after sync")
	}
	hashPicker.mu.RUnlock()
}

func TestRandomBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &RandomBalancerBuilder{}
	info, _ := buildInfoFromAddrs("10.0.0.1:8080", "10.0.0.2:8080")

	p1 := builder.Build(info)
	_, ok := p1.(*RandomBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrs("10.0.0.2:8080")
	p2 := builder.Build(info2)

	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{Ctx: context.Background()})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byAddr2["10.0.0.2:8080"] {
			t.Fatalf("expected random picker to select the only remaining node")
		}
	}
}

func TestWeightBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &WeightBalancerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 10,
		"10.0.0.2:8080": 20,
	})

	p1 := builder.Build(info)
	weightPicker, ok := p1.(*WeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.2:8080": 20,
	})
	p2 := builder.Build(info2)

	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{Ctx: context.Background()})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byAddr2["10.0.0.2:8080"] {
			t.Fatalf("expected weighted picker to select the only remaining node")
		}
	}

	weightPicker.mu.RLock()
	if len(weightPicker.list) != 1 {
		weightPicker.mu.RUnlock()
		t.Fatalf("expected one weighted node after sync")
	}
	weightPicker.mu.RUnlock()
}

func TestRoundRobinBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &RoundRobinBalancerBuilder{}
	info, _ := buildInfoFromAddrs("10.0.0.1:8080", "10.0.0.2:8080")

	p1 := builder.Build(info)
	picker, ok := p1.(*RoundRobinBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrs("10.0.0.2:8080")
	p2 := builder.Build(info2)

	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byAddr2["10.0.0.2:8080"] {
			t.Fatalf("expected round robin picker to select only remaining node")
		}
	}

	picker.mu.RLock()
	if len(picker.list) != 1 {
		picker.mu.RUnlock()
		t.Fatalf("expected one node in round robin list")
	}
	picker.mu.RUnlock()
}

func TestWeightRandomBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &WeightRandomBalancerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 10,
		"10.0.0.2:8080": 20,
	})
	p1 := builder.Build(info)
	picker, ok := p1.(*WeightRandomBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.2:8080": 20,
	})
	p2 := builder.Build(info2)
	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byAddr2["10.0.0.2:8080"] {
			t.Fatalf("expected weighted-random picker to select only remaining node")
		}
	}

	picker.mu.RLock()
	if len(picker.list) != 1 {
		picker.mu.RUnlock()
		t.Fatalf("expected one node in weighted-random list")
	}
	picker.mu.RUnlock()
}

func TestDynamicWeightBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := &DynamicWeightBalancerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.1:8080": 10,
		"10.0.0.2:8080": 20,
	})
	p1 := builder.Build(info)
	picker, ok := p1.(*DynamicWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byAddr2 := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.0.2:8080": 20,
	})
	p2 := builder.Build(info2)
	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byAddr2["10.0.0.2:8080"] {
			t.Fatalf("expected dynamic-weight picker to select only remaining node")
		}
	}

	picker.mu.RLock()
	if len(picker.list) != 1 {
		picker.mu.RUnlock()
		t.Fatalf("expected one node in dynamic-weight list")
	}
	picker.mu.RUnlock()
}

func TestRwWeightBalancerBuilderSync(t *testing.T) {
	t.Parallel()

	builder := NewRwWeightBalancerBuilder()
	info, _ := buildInfoFromRwNodes([]rwNodeSpec{
		{addr: "10.0.0.1:8080", nodeName: "n1", group: "g1", readWeight: 10, writeWeight: 5},
		{addr: "10.0.0.2:8080", nodeName: "n2", group: "g1", readWeight: 20, writeWeight: 10},
	})
	p1 := builder.Build(info)
	picker, ok := p1.(*RwWeightBalancer)
	if !ok {
		t.Fatalf("picker type mismatch")
	}

	info2, byNode2 := buildInfoFromRwNodes([]rwNodeSpec{
		{addr: "10.0.0.2:8080", nodeName: "n2", group: "g1", readWeight: 20, writeWeight: 10},
	})
	p2 := builder.Build(info2)
	if p1 != p2 {
		t.Fatalf("expected builder to reuse same picker instance")
	}

	for range 10 {
		res, err := p2.Pick(balancer.PickInfo{Ctx: context.Background()})
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		if res.SubConn != byNode2["n2"] {
			t.Fatalf("expected rw-weight picker to select only remaining node")
		}
	}

	picker.mu.RLock()
	if len(picker.list) != 1 {
		picker.mu.RUnlock()
		t.Fatalf("expected one node in rw-weight list")
	}
	picker.mu.RUnlock()
}
