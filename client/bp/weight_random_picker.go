package bp

import (
	"maps"
	"math/rand"
	"sync"

	"github.com/jrmarcco/xgrpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*weightRandomPickerBuilder)(nil)

type weightRandomPickerBuilder struct {
	mu sync.Mutex

	picker *WeightRandomPicker
}

func (b *weightRandomPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &WeightRandomPicker{
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

var _ balancer.Picker = (*WeightRandomPicker)(nil)

type WeightRandomPicker struct {
	mu sync.RWMutex

	list        []weightRandomNode
	nodes       map[string]weightRandomNode
	totalWeight uint32
}

func (p *WeightRandomPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	list := p.list
	totalWeight := p.totalWeight
	p.mu.RUnlock()

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

func (p *WeightRandomPicker) syncReadySCs(readySCs map[string]weightRandomNode) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr := range p.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(p.nodes, addr)
	}
	maps.Copy(p.nodes, readySCs)

	p.totalWeight = 0
	p.list = p.list[:0]
	for _, node := range p.nodes {
		p.totalWeight += node.weight
		p.list = append(p.list, node)
	}
}

type weightRandomNode struct {
	sc     balancer.SubConn
	weight uint32
}
