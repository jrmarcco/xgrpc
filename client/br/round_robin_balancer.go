package br

import (
	"maps"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*RoundRobinBalancerBuilder)(nil)

type RoundRobinBalancerBuilder struct {
	mu sync.Mutex

	picker *RoundRobinBalancer
}

func (b *RoundRobinBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &RoundRobinBalancer{
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

var _ balancer.Picker = (*RoundRobinBalancer)(nil)

type RoundRobinBalancer struct {
	mu sync.RWMutex

	list  []balancer.SubConn
	nodes map[string]balancer.SubConn

	index uint64
}

func (b *RoundRobinBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	list := b.list
	b.mu.RUnlock()

	if len(list) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	index := atomic.AddUint64(&b.index, 1)

	// index - 1 是为了从 0 开始。
	// 这里做不做 -1 没有什么实质性影响，不 -1 也只是第一个节点少参与一次轮询。
	sc := list[(index-1)%uint64(len(list))]
	return balancer.PickResult{
		SubConn: sc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (b *RoundRobinBalancer) syncReadySCs(readySCs map[string]balancer.SubConn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for addr := range b.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(b.nodes, addr)
	}
	maps.Copy(b.nodes, readySCs)

	b.list = b.list[:0]
	for _, node := range b.nodes {
		b.list = append(b.list, node)
	}
}
