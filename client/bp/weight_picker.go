package bp

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/jrmarcco/xgrpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*weightPickerBuilder)(nil)

type weightPickerBuilder struct {
	mu sync.Mutex

	picker *WeightPicker
}

func (b *weightPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &WeightPicker{
			nodes: make(map[string]*weightNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*weightNode, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		weight, _ := scInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		readySCs[addr] = &weightNode{
			sc:            sc,
			weight:        weight,
			currentWeight: weight,
		}
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*WeightPicker)(nil)

type WeightPicker struct {
	mu sync.RWMutex

	nodes    map[string]*weightNode
	snapshot atomic.Pointer[weightSnapshot] // 快照 ( 用于 Pick 时避免并发访问 )
}

type weightSnapshot struct {
	nodes []*weightNode
}

func (p *WeightPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	snapshot := p.snapshot.Load()
	if snapshot == nil || len(snapshot.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *weightNode

	for _, node := range snapshot.nodes {
		node.mu.Lock()
		totalWeight += node.efficientWeight
		node.currentWeight += node.efficientWeight

		if selectedNode == nil || selectedNode.currentWeight < node.currentWeight {
			selectedNode = node
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	selectedNode.currentWeight -= totalWeight
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.sc,
		Done: func(info balancer.DoneInfo) {
			selectedNode.mu.Lock()
			defer selectedNode.mu.Unlock()

			const twice = 2
			if info.Err == nil {
				selectedNode.efficientWeight++
				selectedNode.efficientWeight = min(selectedNode.efficientWeight, max(selectedNode.weight*twice, 1))
				return
			}

			if errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF) {
				if selectedNode.efficientWeight > 1 {
					selectedNode.efficientWeight--
				}
			}
		},
	}, nil
}

func (p *WeightPicker) syncReadySCs(readySCs map[string]*weightNode) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr := range p.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(p.nodes, addr)
	}

	for addr, nextNode := range readySCs {
		oldNode, ok := p.nodes[addr]
		if !ok {
			p.nodes[addr] = nextNode
			continue
		}

		oldNode.mu.Lock()
		oldNode.sc = nextNode.sc
		oldNode.weight = nextNode.weight
		if oldNode.currentWeight == 0 {
			oldNode.currentWeight = nextNode.currentWeight
		}
		oldNode.mu.Unlock()
	}

	list := make([]*weightNode, 0, len(p.nodes))
	for _, node := range p.nodes {
		list = append(list, node)
	}
	p.snapshot.Store(&weightSnapshot{nodes: list})
}

type weightNode struct {
	mu sync.RWMutex

	sc              balancer.SubConn
	weight          uint32
	currentWeight   uint32
	efficientWeight uint32
}
