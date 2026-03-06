package bp

import (
	"maps"
	"math/rand/v2"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*randomPickerBuilder)(nil)

type randomPickerBuilder struct {
	mu sync.Mutex

	picker *RandomPicker
}

func (b *randomPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &RandomPicker{
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

var _ balancer.Picker = (*RandomPicker)(nil)

type RandomPicker struct {
	mu sync.RWMutex

	list  []balancer.SubConn
	nodes map[string]balancer.SubConn
}

func (p *RandomPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.list) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	//nolint:gosec // 这里使用 rand.IntN 是安全的。
	index := rand.IntN(len(p.list))
	return balancer.PickResult{
		SubConn: p.list[index],
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (p *RandomPicker) syncReadySCs(readySCs map[string]balancer.SubConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr := range p.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(p.nodes, addr)
	}

	maps.Copy(p.nodes, readySCs)

	p.list = make([]balancer.SubConn, 0, len(p.nodes))
	for _, node := range p.nodes {
		p.list = append(p.list, node)
	}
}
