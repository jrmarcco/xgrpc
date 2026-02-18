package br

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/jrmarcco/xgrpc/client"
)

var _ base.PickerBuilder = (*WeightBalancerBuilder)(nil)

type WeightBalancerBuilder struct {
	mu sync.Mutex

	picker *WeightBalancer
}

func (b *WeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &WeightBalancer{
			nodes: make(map[string]*weightServiceNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*weightServiceNode, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		weight, _ := scInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		readySCs[addr] = &weightServiceNode{
			sc:            sc,
			weight:        weight,
			currentWeight: weight,
		}
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*WeightBalancer)(nil)

type WeightBalancer struct {
	mu sync.RWMutex

	list  []*weightServiceNode
	nodes map[string]*weightServiceNode
}

func (b *WeightBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	nodes := b.list
	b.mu.RUnlock()

	if len(nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *weightServiceNode

	for _, node := range nodes {
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
				selectedNode.efficientWeight = max(selectedNode.efficientWeight, selectedNode.weight*twice)
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

func (b *WeightBalancer) syncReadySCs(readySCs map[string]*weightServiceNode) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for addr := range b.nodes {
		if _, ok := readySCs[addr]; ok {
			continue
		}
		delete(b.nodes, addr)
	}

	for addr, nextNode := range readySCs {
		oldNode, ok := b.nodes[addr]
		if !ok {
			b.nodes[addr] = nextNode
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

	b.list = b.list[:0]
	for _, node := range b.nodes {
		b.list = append(b.list, node)
	}
}

type weightServiceNode struct {
	mu sync.RWMutex

	sc              balancer.SubConn
	weight          uint32
	currentWeight   uint32
	efficientWeight uint32
}
