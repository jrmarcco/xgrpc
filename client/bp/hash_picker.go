package bp

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/jrmarcco/xgrpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*hashPickerBuilder)(nil)

type hashPickerBuilder struct {
	mu sync.Mutex

	picker *HashPicker
}

func (b *hashPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &HashPicker{
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

var _ balancer.Picker = (*HashPicker)(nil)

type HashPicker struct {
	mu sync.RWMutex

	nodes map[string]balancer.SubConn
	addrs []string
}

func (p *HashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.addrs) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	hash, err := p.hashFromPickInfo(info)
	if err != nil {
		return balancer.PickResult{}, err
	}

	index := hash % uint32(len(p.addrs))
	addr := p.addrs[index]
	sc, ok := p.nodes[addr]
	if !ok {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{
		SubConn: sc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (p *HashPicker) syncReadySCs(readySCs map[string]balancer.SubConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr := range p.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(p.nodes, addr)
	}

	changed := false
	for addr, sc := range readySCs {
		oldSC, ok := p.nodes[addr]
		p.nodes[addr] = sc
		if !ok || oldSC != sc {
			changed = true
		}
	}

	if changed || len(p.addrs) != len(p.nodes) {
		p.addrs = make([]string, 0, len(p.nodes))
		for addr := range p.nodes {
			p.addrs = append(p.addrs, addr)
		}
		sort.Strings(p.addrs)
	}
}

func (p *HashPicker) hashFromPickInfo(info balancer.PickInfo) (uint32, error) {
	var ctx context.Context
	if ctx = info.Ctx; ctx == nil {
		return 0, fmt.Errorf("[hash-balancer] context not found in pick info")
	}

	// 获取 bizId。
	bizId, ok := client.ContextBizId(ctx)
	if !ok {
		return 0, fmt.Errorf("[hash-balancer] bizId not found in context")
	}

	hash := xxhash.Sum64String(strconv.FormatUint(bizId, 10))
	return uint32(hash), nil
}
