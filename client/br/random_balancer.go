package br

import (
	"maps"
	"math/rand/v2"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*RandomBalancerBuilder)(nil)

type RandomBalancerBuilder struct {
	mu sync.Mutex

	picker *RandomBalancer
}

func (b *RandomBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &RandomBalancer{
			nodes: make(map[string]balancer.SubConn, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		readySCs[addr] = sc
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*RandomBalancer)(nil)

type RandomBalancer struct {
	mu sync.RWMutex

	list  []balancer.SubConn
	nodes map[string]balancer.SubConn
}

func (b *RandomBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.list) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	//nolint:gosec // 这里使用 rand.IntN 是安全的。
	index := rand.IntN(len(b.list))
	return balancer.PickResult{
		SubConn: b.list[index],
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (b *RandomBalancer) syncReadySCs(readySCs map[string]balancer.SubConn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for addr := range b.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(b.nodes, addr)
	}

	maps.Copy(b.nodes, readySCs)

	b.list = make([]balancer.SubConn, 0, len(b.nodes))
	for _, node := range b.nodes {
		b.list = append(b.list, node)
	}
}
