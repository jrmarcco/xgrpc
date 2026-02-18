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

const defaultVirtualNodeCnt = 100

var _ base.PickerBuilder = (*CHBalancerBuilder)(nil)

type CHBalancerBuilder struct {
	mu sync.Mutex

	picker         *CHBalancer
	virtualNodeCnt int // 虚拟节点数量
}

func NewCHBalancerBuilder() *CHBalancerBuilder {
	return &CHBalancerBuilder{
		virtualNodeCnt: defaultVirtualNodeCnt,
	}
}

func (b *CHBalancerBuilder) VirtualNodeCnt(cnt int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// picker 一旦创建，虚拟节点数量即固定，避免运行中修改造成路由突变。
	if b.picker != nil {
		return
	}
	b.virtualNodeCnt = cnt
}

func (b *CHBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if b.virtualNodeCnt <= 0 {
		return base.NewErrPicker(fmt.Errorf("[consistent-hash-balancer] virtual node count must be greater than 0"))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &CHBalancer{
			scs:            make(map[string]balancer.SubConn, len(info.ReadySCs)),
			ring:           make([]uint32, 0, b.virtualNodeCnt*len(info.ReadySCs)),
			nodes:          make([]string, 0, b.virtualNodeCnt*len(info.ReadySCs)),
			virtualNodeCnt: b.virtualNodeCnt,
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

var _ balancer.Picker = (*CHBalancer)(nil)

type CHBalancer struct {
	mu sync.RWMutex

	scs            map[string]balancer.SubConn // 虚拟节点地址 -> SubConn
	ring           []uint32                    // 哈希环，存储虚拟节点的哈希值
	nodes          []string                    // 与哈希环对应的虚拟节点地址
	virtualNodeCnt int                         // 虚拟节点数量

	lastSyncStats CHSyncStats
}

// CHSyncStats 为最近一次同步快照信息。
type CHSyncStats struct {
	Added   int
	Removed int
	Updated int

	VirtualNodeCount  int
	PhysicalNodeCount int
}

func (p *CHBalancer) addNodeLocked(sc balancer.SubConn, addr string) {
	p.scs[addr] = sc

	// 为每个物理节点创建多个虚拟节点，虚拟节点在一致性哈希中起到关键的负载均衡作用。
	//
	// 假设有 3 个物理节点，哈希后可能分布在环上的位置：
	// 	s1: hash=100
	// 	s2: hash=200
	// 	s3: hash=300
	//
	// 在为每个物理节点创建 100 个虚拟节点后，使用虚拟节点的哈希环：
	//  0 - s1 - s2 - s3 - s1 - s2 - s3 - s1 - s2 - s3 - ... - s1 - s2 - 1000
	//
	// 负载分布：
	// 	s1 ( 33% )
	//  s2 ( 33% )
	//  s3 ( 34% )
	for i := 0; i < p.virtualNodeCnt; i++ {
		hash := p.hash(fmt.Sprintf("%s#%d", addr, i))
		p.ring = append(p.ring, hash)
		p.nodes = append(p.nodes, addr)
	}
}

func (p *CHBalancer) sortRingLocked() {
	// 索引切片 ( 用于排序)。
	indices := make([]int, len(p.ring))
	for i := range indices {
		indices[i] = i
	}

	// 根据哈希值排序索引。
	sort.Slice(indices, func(i, j int) bool {
		return p.ring[indices[i]] < p.ring[indices[j]]
	})

	sortedRing := make([]uint32, len(p.ring))
	sortedNodes := make([]string, len(p.nodes))
	for i, index := range indices {
		sortedRing[i] = p.ring[index]
		sortedNodes[i] = p.nodes[index]
	}

	p.ring = sortedRing
	p.nodes = sortedNodes
}

func (p *CHBalancer) removeNodeLocked(addr string) {
	if _, ok := p.scs[addr]; !ok {
		return
	}
	delete(p.scs, addr)

	newRing := make([]uint32, 0, len(p.ring))
	newNodes := make([]string, 0, len(p.nodes))
	for i, node := range p.nodes {
		if node == addr {
			continue
		}
		newRing = append(newRing, p.ring[i])
		newNodes = append(newNodes, node)
	}

	p.ring = newRing
	p.nodes = newNodes
}

func (p *CHBalancer) syncReadySCs(readySCs map[string]balancer.SubConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	updated := 0
	added := 0

	needSort := false
	for addr := range p.scs {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		p.removeNodeLocked(addr)
		removed++
	}

	for addr, sc := range readySCs {
		if _, ok := p.scs[addr]; ok {
			p.scs[addr] = sc
			updated++
			continue
		}

		p.addNodeLocked(sc, addr)
		added++
		needSort = true
	}

	if needSort {
		p.sortRingLocked()
	}

	p.lastSyncStats = CHSyncStats{
		Added:   added,
		Removed: removed,
		Updated: updated,

		VirtualNodeCount:  len(p.ring),
		PhysicalNodeCount: len(p.scs),
	}
}

func (p *CHBalancer) SyncStats() CHSyncStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.lastSyncStats
}

func (p *CHBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.scs) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	hashKey, err := p.hashFromPickInfo(info)
	if err != nil {
		return balancer.PickResult{}, err
	}

	nodeAddr := p.getNodeAddr(hashKey)
	if nodeAddr == "" {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	sc, ok := p.scs[nodeAddr]
	if !ok {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{
		SubConn: sc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (p *CHBalancer) getNodeAddr(hash uint32) string {
	if len(p.ring) == 0 {
		return ""
	}

	// 查找第一个大于等于当前 hash 值的节点。
	index := sort.Search(len(p.ring), func(i int) bool {
		return p.ring[i] >= hash
	})

	// 没找到则取第一个节点 ( 环结构 )。
	if index >= len(p.ring) {
		index = 0
	}
	return p.nodes[index]
}

func (p *CHBalancer) hashFromPickInfo(info balancer.PickInfo) (uint32, error) {
	var ctx context.Context
	if ctx = info.Ctx; ctx == nil {
		return 0, fmt.Errorf("[consistent-hash-balancer] context not found in pick info")
	}

	// 获取 bizId。
	bizId, ok := client.ContextBizId(ctx)
	if !ok {
		return 0, fmt.Errorf("[consistent-hash-balancer] bizId not found in context")
	}
	return p.hash(strconv.FormatUint(bizId, 10)), nil
}

// hash 这里取低 32 位就够了。
func (p *CHBalancer) hash(src string) uint32 {
	hashVal := xxhash.Sum64String(src)
	return uint32(hashVal)
}
