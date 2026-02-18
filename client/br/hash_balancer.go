package br

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/jrmarcco/xgrpc/client"
)

var _ base.PickerBuilder = (*HashBalancerBuilder)(nil)

type HashBalancerBuilder struct {
	mu sync.Mutex

	picker *HashBalancer
}

func (b *HashBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &HashBalancer{
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

var _ balancer.Picker = (*HashBalancer)(nil)

type HashBalancer struct {
	mu sync.RWMutex

	nodes map[string]balancer.SubConn
	addrs []string
}

func (b *HashBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.addrs) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	hash, err := b.hashFromPickInfo(info)
	if err != nil {
		return balancer.PickResult{}, err
	}

	index := hash % uint32(len(b.addrs))
	addr := b.addrs[index]
	sc, ok := b.nodes[addr]
	if !ok {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{
		SubConn: sc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (b *HashBalancer) syncReadySCs(readySCs map[string]balancer.SubConn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for addr := range b.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(b.nodes, addr)
	}

	changed := false
	for addr, sc := range readySCs {
		oldSC, ok := b.nodes[addr]
		b.nodes[addr] = sc
		if !ok || oldSC != sc {
			changed = true
		}
	}

	if changed || len(b.addrs) != len(b.nodes) {
		b.addrs = make([]string, 0, len(b.nodes))
		for addr := range b.nodes {
			b.addrs = append(b.addrs, addr)
		}
		sort.Strings(b.addrs)
	}
}

func (b *HashBalancer) hashFromPickInfo(info balancer.PickInfo) (uint32, error) {
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
