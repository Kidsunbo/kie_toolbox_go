package kflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type State struct {
}

type BasicNode struct {
	name string
}

func (b BasicNode) Name() string {
	return b.name
}

func (b BasicNode) Run(ctx context.Context, state State) error {
	time.Sleep(1 * time.Second)
	fmt.Println(b.name)
	return nil
}

type FlowNode struct {
	name string
}

func (b FlowNode) Name() string {
	return b.name
}

func (b FlowNode) Run(ctx context.Context, state State, plan *Plan) error {
	time.Sleep(1 * time.Second)
	fmt.Println(b.name, plan.InParallel())
	return nil
}

type StringDependence struct {
	dependence []string
}

func (m StringDependence) Dependence() []string {
	return m.dependence
}

type ConditionDependence struct {
	dependence []*Dependence[State]
}

func (m ConditionDependence) Dependence() []*Dependence[State] {
	return m.dependence
}

type NodeType1 struct {
	BasicNode
	StringDependence
}

func NewNodeType1(name string, dep []string) *NodeType1 {
	return &NodeType1{
		BasicNode: BasicNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

type NodeType2 struct {
	BasicNode
	ConditionDependence
}

func NewNodeType2(name string, dep []*Dependence[State]) *NodeType2 {
	return &NodeType2{
		BasicNode: BasicNode{
			name: name,
		},
		ConditionDependence: ConditionDependence{
			dependence: dep,
		},
	}
}

type NodeType3 struct {
	FlowNode
	StringDependence
}

func NewNodeType3(name string, dep []string) *NodeType3 {
	return &NodeType3{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

type NodeType4 struct {
	FlowNode
	ConditionDependence
}

func NewNodeType4(name string, dep []*Dependence[State]) *NodeType4 {
	return &NodeType4{
		FlowNode: FlowNode{
			name: name,
		},
		ConditionDependence: ConditionDependence{
			dependence: dep,
		},
	}
}

func TestAddNodes(t *testing.T) {
	node := new(Node[State])

	eng := NewEngine[State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[State]{
		node.StaticDependence("Type1_3"), node.ConditionalDependence("Type1_1", func(ctx context.Context, s State) bool { return false }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[State]{
		node.StaticDependence("Type1_2"), node.ConditionalDependence("Type2_1", func(ctx context.Context, s State) bool { return true }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())

	assert.NoError(t, eng.Run(context.Background(), State{}, "Type2_2"))
	fmt.Println(eng.Dot())
}
