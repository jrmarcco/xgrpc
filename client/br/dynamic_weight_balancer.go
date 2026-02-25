package br

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jrmarcco/xgrpc/client"
)

var _ base.PickerBuilder = (*DynamicWeightBalancerBuilder)(nil)

type DynamicWeightBalancerBuilder struct {
	mu sync.Mutex

	picker *DynamicWeightBalancer
}

func NewDynamicWeightBalancerBuilder() *DynamicWeightBalancerBuilder {
	return &DynamicWeightBalancerBuilder{}
}

func (b *DynamicWeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &DynamicWeightBalancer{
			nodes: make(map[string]*dynamicServiceNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*dynamicServiceNode, len(info.ReadySCs))
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
		readySCs[addr] = &dynamicServiceNode{
			sc:              sc,
			weight:          initialWeight,
			currentWeight:   initialWeight,
			efficientWeight: initialWeight,
		}
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*DynamicWeightBalancer)(nil)

type DynamicWeightBalancer struct {
	mu sync.RWMutex

	nodes map[string]*dynamicServiceNode
	list  []*dynamicServiceNode
}

func (p *DynamicWeightBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	nodes := append([]*dynamicServiceNode(nil), p.list...)
	p.mu.RUnlock()

	if len(nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var selectedNode *dynamicServiceNode
	var selectedWeight int64
	var totalWeight int64

	// 计算总权重。
	for _, node := range nodes {
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

func (p *DynamicWeightBalancer) syncReadySCs(readySCs map[string]*dynamicServiceNode) {
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

	p.list = p.list[:0]
	for _, node := range p.nodes {
		p.list = append(p.list, node)
	}
}

type dynamicServiceNode struct {
	mu sync.Mutex

	sc              balancer.SubConn
	weight          int64
	currentWeight   int64
	efficientWeight int64
}
