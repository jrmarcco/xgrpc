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
		readySCs[addr] = &dynamicServiceNode{
			sc:              sc,
			weight:          weight,
			currentWeight:   weight,
			efficientWeight: weight,
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
	nodes := p.list
	p.mu.RUnlock()

	if len(nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *dynamicServiceNode
	for _, node := range nodes {
		node.mu.RLock()
		totalWeight += node.efficientWeight
		node.currentWeight += node.efficientWeight

		if selectedNode == nil || selectedNode.currentWeight < node.currentWeight {
			selectedNode = node
		}
		node.mu.RUnlock()
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
				const twice = 2

				selectedNode.efficientWeight++
				selectedNode.efficientWeight = max(selectedNode.efficientWeight, selectedNode.weight*twice)
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
		oldNode.weight = nextNode.weight
		if oldNode.currentWeight == 0 {
			oldNode.currentWeight = nextNode.currentWeight
		}
		if oldNode.efficientWeight == 0 {
			oldNode.efficientWeight = nextNode.efficientWeight
		}
		oldNode.mu.Unlock()
	}

	p.list = p.list[:0]
	for _, node := range p.nodes {
		p.list = append(p.list, node)
	}
}

type dynamicServiceNode struct {
	mu sync.RWMutex

	sc              balancer.SubConn
	weight          uint32
	currentWeight   uint32
	efficientWeight uint32
}
