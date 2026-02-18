package br

import (
	"maps"
	"math/rand"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/jrmarcco/xgrpc/client"
)

var _ base.PickerBuilder = (*WeightRandomBalancerBuilder)(nil)

type WeightRandomBalancerBuilder struct {
	mu sync.Mutex

	picker *WeightRandomBalancer
}

func (b *WeightRandomBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &WeightRandomBalancer{
			nodes: make(map[string]weightRandomNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]weightRandomNode, len(info.ReadySCs))
	for cc, ccInfo := range info.ReadySCs {
		addr := ccInfo.Address.Addr
		if addr == "" {
			continue
		}
		weight, _ := ccInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		readySCs[addr] = weightRandomNode{
			sc:     cc,
			weight: weight,
		}
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*WeightRandomBalancer)(nil)

type WeightRandomBalancer struct {
	mu sync.RWMutex

	list        []weightRandomNode
	nodes       map[string]weightRandomNode
	totalWeight uint32
}

func (b *WeightRandomBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	list := b.list
	totalWeight := b.totalWeight
	b.mu.RUnlock()

	if len(list) == 0 || totalWeight == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	//nolint:gosec // 这里使用 rand.IntN 是安全的。
	target := rand.Intn(int(totalWeight))
	for _, node := range list {
		target -= int(node.weight)
		if target < 0 {
			return balancer.PickResult{
				SubConn: node.sc,
				Done:    func(_ balancer.DoneInfo) {},
			}, nil
		}
	}
	panic("[weight-random-balancer] unreachable")
}

func (b *WeightRandomBalancer) syncReadySCs(readySCs map[string]weightRandomNode) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for addr := range b.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(b.nodes, addr)
	}
	maps.Copy(b.nodes, readySCs)

	b.totalWeight = 0
	b.list = b.list[:0]
	for _, node := range b.nodes {
		b.totalWeight += node.weight
		b.list = append(b.list, node)
	}
}

type weightRandomNode struct {
	sc     balancer.SubConn
	weight uint32
}
