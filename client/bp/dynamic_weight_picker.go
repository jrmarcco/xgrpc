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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ base.PickerBuilder = (*dynamicWeightPickerBuilder)(nil)

type dynamicWeightPickerBuilder struct {
	mu sync.Mutex

	picker *DynamicWeightPicker
}

func (b *dynamicWeightPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &DynamicWeightPicker{
			nodes: make(map[string]*dynamicNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*dynamicNode, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		weight, _ := scInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		initialWeight := int64(weight)
		if initialWeight < 1 {
			initialWeight = 1
		}
		readySCs[addr] = &dynamicNode{
			sc:              sc,
			weight:          initialWeight,
			currentWeight:   initialWeight,
			efficientWeight: initialWeight,
		}
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*DynamicWeightPicker)(nil)

type DynamicWeightPicker struct {
	mu sync.RWMutex

	nodes    map[string]*dynamicNode
	snapshot atomic.Pointer[dynamicWeightSnapshot] // 快照 ( 用于 Pick 时避免并发访问 )
}

type dynamicWeightSnapshot struct {
	nodes []*dynamicNode
}

func (p *DynamicWeightPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	snapshot := p.snapshot.Load()
	if snapshot == nil || len(snapshot.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var selectedNode *dynamicNode
	var selectedWeight int64
	var totalWeight int64

	// 计算总权重。
	for _, node := range snapshot.nodes {
		node.mu.Lock()
		totalWeight += node.efficientWeight
		node.currentWeight += node.efficientWeight
		currentWeight := node.currentWeight
		node.mu.Unlock()

		if selectedNode == nil || selectedWeight < currentWeight {
			selectedNode = node
			selectedWeight = currentWeight
		}
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

			if info.Err == nil {
				selectedNode.efficientWeight++
				selectedNode.efficientWeight = min(selectedNode.efficientWeight, selectedNode.weight)
				return
			}

			if errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF) {
				selectedNode.efficientWeight = 1
				return
			}

			res, _ := status.FromError(info.Err)
			switch res.Code() {
			case codes.Unavailable:
				selectedNode.efficientWeight = 1
				return
			default:
				if selectedNode.efficientWeight > 1 {
					selectedNode.efficientWeight--
				}
			}
		},
	}, nil
}

func (p *DynamicWeightPicker) syncReadySCs(readySCs map[string]*dynamicNode) {
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
		oldNode.weight = max(nextNode.weight, 1)

		if oldNode.currentWeight == 0 {
			oldNode.currentWeight = nextNode.currentWeight
		}
		if oldNode.efficientWeight == 0 {
			oldNode.efficientWeight = nextNode.efficientWeight
		}
		if oldNode.efficientWeight < 1 {
			oldNode.efficientWeight = 1
		}

		oldNode.efficientWeight = min(oldNode.efficientWeight, oldNode.weight)
		oldNode.mu.Unlock()
	}

	list := make([]*dynamicNode, 0, len(p.nodes))
	for _, node := range p.nodes {
		list = append(list, node)
	}
	p.snapshot.Store(&dynamicWeightSnapshot{nodes: list})
}

type dynamicNode struct {
	mu sync.Mutex

	sc              balancer.SubConn
	weight          int64
	currentWeight   int64
	efficientWeight int64
}
