package bp

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/jrmarcco/jit/xslice"
	"github.com/jrmarcco/xgrpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*readWriteWeightPickerBuilder)(nil)

type readWriteWeightPickerBuilder struct {
	mu sync.Mutex

	picker *ReadWriteWeightPicker
}

func (b *readWriteWeightPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &ReadWriteWeightPicker{
			nodes: make(map[string]*readWriteWeightNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*readWriteWeightNode, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		readWeight, ok := scInfo.Address.Attributes.Value(client.AttrNameReadWeight).(uint32)
		if !ok {
			continue
		}
		writeWeight, ok := scInfo.Address.Attributes.Value(client.AttrNameWriteWeight).(uint32)
		if !ok {
			continue
		}
		groupName, ok := scInfo.Address.Attributes.Value(client.AttrNameGroup).(string)
		if !ok {
			continue
		}
		nodeName, ok := scInfo.Address.Attributes.Value(client.AttrNameNode).(string)
		if !ok {
			continue
		}
		readySCs[nodeName] = newReadWriteWeightNode(sc, readWeight, writeWeight, groupName)
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

var _ balancer.Picker = (*ReadWriteWeightPicker)(nil)

type ReadWriteWeightPicker struct {
	mu sync.RWMutex

	list  []*readWriteWeightNode
	nodes map[string]*readWriteWeightNode
}

func (p *ReadWriteWeightPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	nodes := p.list
	p.mu.RUnlock()

	if len(nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// 获取候选节点。
	ctx := info.Ctx
	group, hasGroup := client.ContextGroup(ctx)

	var candidateNodes []*readWriteWeightNode
	if !hasGroup {
		// ctx 中没有 group 字段，不进行筛选，返回所有节点。
		candidateNodes = nodes
	} else {
		// ctx 中有 group 字段，进行筛选。
		candidateNodes = xslice.FilterMap(nodes, func(_ int, src *readWriteWeightNode) (*readWriteWeightNode, bool) {
			src.mu.RLock()
			nodeGroup := src.group
			src.mu.RUnlock()

			return src, group == nodeGroup
		})
	}

	if len(candidateNodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *readWriteWeightNode

	isWriteReq := p.isWriteReq(ctx)

	// 权重计算。
	for _, node := range candidateNodes {
		node.mu.Lock()
		if isWriteReq {
			totalWeight += node.efficientWriteWeight
			node.currentWriteWeight += node.efficientWriteWeight
			if selectedNode == nil || selectedNode.currentWriteWeight < node.currentWriteWeight {
				selectedNode = node
			}
			node.mu.Unlock()
			continue
		}

		totalWeight += node.efficientReadWeight
		node.currentReadWeight += node.efficientReadWeight
		if selectedNode == nil || selectedNode.currentReadWeight < node.currentReadWeight {
			selectedNode = node
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	if isWriteReq {
		selectedNode.currentWriteWeight -= totalWeight
	} else {
		selectedNode.currentReadWeight -= totalWeight
	}
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.sc,
		Done: func(info balancer.DoneInfo) {
			p.recalculateWeight(isWriteReq, selectedNode, info)
		},
	}, nil
}

// recalculateWeight 重新计算被选中节点的权重。
func (p *ReadWriteWeightPicker) recalculateWeight(isWriteReq bool, node *readWriteWeightNode, info balancer.DoneInfo) {
	node.mu.Lock()
	defer node.mu.Unlock()

	isDecrementErr := info.Err != nil && (errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF))
	const twice = 2
	if isWriteReq {
		if info.Err == nil {
			node.efficientWriteWeight++
			node.currentWriteWeight = max(node.efficientWriteWeight, node.writeWeight*twice)
			return
		}
		if isDecrementErr && node.efficientWriteWeight > 1 {
			node.efficientWriteWeight--
		}
		return
	}

	if info.Err == nil {
		node.efficientReadWeight++
		node.currentReadWeight = max(node.efficientReadWeight, node.readWeight*twice)
		return
	}

	if isDecrementErr && node.efficientReadWeight > 1 {
		node.efficientReadWeight--
	}
}

func (p *ReadWriteWeightPicker) syncReadySCs(readySCs map[string]*readWriteWeightNode) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for nodeName := range p.nodes {
		if _, ok := readySCs[nodeName]; ok {
			continue
		}
		delete(p.nodes, nodeName)
	}

	for nodeName, nextNode := range readySCs {
		oldNode, ok := p.nodes[nodeName]
		if !ok {
			p.nodes[nodeName] = nextNode
			continue
		}

		oldNode.mu.Lock()
		weightChanged := oldNode.readWeight != nextNode.readWeight || oldNode.writeWeight != nextNode.writeWeight
		if !weightChanged {
			oldNode.sc = nextNode.sc
			oldNode.group = nextNode.group
			oldNode.mu.Unlock()
			continue
		}
		oldNode.mu.Unlock()

		p.nodes[nodeName] = nextNode
	}

	p.list = p.list[:0]
	for _, node := range p.nodes {
		p.list = append(p.list, node)
	}
}

func (p *ReadWriteWeightPicker) isWriteReq(ctx context.Context) bool {
	if reqType, ok := client.ContextReqType(ctx); ok {
		return reqType == 1
	}
	return false
}

type readWriteWeightNode struct {
	mu sync.RWMutex

	sc balancer.SubConn

	readWeight          uint32
	currentReadWeight   uint32
	efficientReadWeight uint32

	writeWeight          uint32
	currentWriteWeight   uint32
	efficientWriteWeight uint32

	group string
}

func newReadWriteWeightNode(sc balancer.SubConn, readWeight, writeWeight uint32, group string) *readWriteWeightNode {
	return &readWriteWeightNode{
		sc: sc,

		readWeight:          readWeight,
		currentReadWeight:   readWeight,
		efficientReadWeight: readWeight,

		writeWeight:          writeWeight,
		currentWriteWeight:   writeWeight,
		efficientWriteWeight: writeWeight,

		group: group,
	}
}
