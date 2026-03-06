package bp

import (
	"maps"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*roundRobinPickerBuilder)(nil)

type roundRobinPickerBuilder struct {
	mu sync.Mutex

	picker *RoundRobinPicker
}

func (b *roundRobinPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &RoundRobinPicker{
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

var _ balancer.Picker = (*RoundRobinPicker)(nil)

type RoundRobinPicker struct {
	mu sync.RWMutex

	nodes    map[string]balancer.SubConn
	snapshot atomic.Pointer[roundRobinSnapshot]

	index uint64
}

type roundRobinSnapshot struct {
	list []balancer.SubConn
}

func (p *RoundRobinPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	snapshot := p.snapshot.Load()
	if snapshot == nil || len(snapshot.list) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	index := atomic.AddUint64(&p.index, 1)

	// index - 1 是为了从 0 开始。
	// 这里做不做 -1 没有什么实质性影响，不 -1 也只是第一个节点少参与一次轮询。
	sc := snapshot.list[(index-1)%uint64(len(snapshot.list))]
	return balancer.PickResult{
		SubConn: sc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (p *RoundRobinPicker) syncReadySCs(readySCs map[string]balancer.SubConn) {
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
	p.snapshot.Store(&roundRobinSnapshot{list: list})
}
