package bp

import (
	"maps"
	"math/rand"
	"sync"
	"sync/atomic"

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

	nodes    map[string]weightRandomNode
	snapshot atomic.Pointer[weightRandomSnapshot]
}

type weightRandomSnapshot struct {
	list        []weightRandomNode
	totalWeight uint32
}

func (p *WeightRandomPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	snapshot := p.snapshot.Load()
	if snapshot == nil || len(snapshot.list) == 0 || snapshot.totalWeight == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	//nolint:gosec // 这里使用 rand.IntN 是安全的。
	target := rand.Intn(int(snapshot.totalWeight))
	for _, node := range snapshot.list {
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

	var totalWeight uint32
	list := make([]weightRandomNode, 0, len(p.nodes))
	for _, node := range p.nodes {
		totalWeight += node.weight
		list = append(list, node)
	}
	p.snapshot.Store(&weightRandomSnapshot{
		list:        list,
		totalWeight: totalWeight,
	})
}

type weightRandomNode struct {
	sc     balancer.SubConn
	weight uint32
}
