package bp

import (
	"maps"
	"math/rand/v2"
	"sync"
	"sync/atomic"

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

	nodes    map[string]balancer.SubConn
	snapshot atomic.Pointer[randomSnapshot]
}

type randomSnapshot struct {
	list []balancer.SubConn
}

func (p *RandomPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	snapshot := p.snapshot.Load()
	if snapshot == nil || len(snapshot.list) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	//nolint:gosec // 这里使用 rand.IntN 是安全的。
	index := rand.IntN(len(snapshot.list))
	return balancer.PickResult{
		SubConn: snapshot.list[index],
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

	list := make([]balancer.SubConn, 0, len(p.nodes))
	for _, node := range p.nodes {
		list = append(list, node)
	}
	p.snapshot.Store(&randomSnapshot{list: list})
}
