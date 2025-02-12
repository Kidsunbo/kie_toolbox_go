package kflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type State struct {
}

type MetaBasicNodeWithStringDependence struct {
	name       string
	dependence []string
	f          func(ctx context.Context, state State) error
}

func (m *MetaBasicNodeWithStringDependence) Name() string {
	return m.name
}

func (m *MetaBasicNodeWithStringDependence) Dependence() []string {
	return m.dependence
}

func (m *MetaBasicNodeWithStringDependence) Run(ctx context.Context, state State) error {
	return m.f(ctx, state)
}

func NewMetaBasicNodeWithStringDependence(name string, dependence []string, f func(ctx context.Context, state State) error) *MetaBasicNodeWithStringDependence {
	return &MetaBasicNodeWithStringDependence{
		name:       name,
		dependence: dependence,
		f:          f,
	}
}

type MetaFlowNodeWithStringDependence struct {
	name       string
	dependence []string
	f          func(ctx context.Context, state State, plan *Plan) error
}

func NewMetaFlowNodeWithStringDependence(name string, dependence []string, f func(ctx context.Context, state State, plan *Plan) error) *MetaFlowNodeWithStringDependence {
	return &MetaFlowNodeWithStringDependence{
		name:       name,
		dependence: dependence,
		f:          f,
	}
}

func (m *MetaFlowNodeWithStringDependence) Name() string {
	return m.name
}

func (m *MetaFlowNodeWithStringDependence) Dependence() []string {
	return m.dependence
}

func (m *MetaFlowNodeWithStringDependence) Run(ctx context.Context, state State, plan *Plan) error {
	return m.f(ctx, state, plan)
}

type MetaBasicNodeWithStructDependence struct {
	name       string
	dependence []*Dependence[State]
	f          func(ctx context.Context, state State) error
}

func NewMetaBasicNodeWithStructDependence(name string, dependence []*Dependence[State], f func(ctx context.Context, state State) error) *MetaBasicNodeWithStructDependence {
	return &MetaBasicNodeWithStructDependence{
		name:       name,
		dependence: dependence,
		f:          f,
	}
}

func (m *MetaBasicNodeWithStructDependence) Name() string {
	return m.name
}

func (m *MetaBasicNodeWithStructDependence) Dependence() []*Dependence[State] {
	return m.dependence
}

func (m *MetaBasicNodeWithStructDependence) Run(ctx context.Context, state State) error {
	return m.f(ctx, state)
}

type MetaFlowNodeWithStructDependence struct {
	name       string
	dependence []*Dependence[State]
	f          func(ctx context.Context, state State, plan *Plan) error
}

func NewMetaFlowNodeWithStructDependence(name string, dependence []*Dependence[State], f func(ctx context.Context, state State, plan *Plan) error) *MetaFlowNodeWithStructDependence {
	return &MetaFlowNodeWithStructDependence{
		name:       name,
		dependence: dependence,
		f:          f,
	}
}

func (m *MetaFlowNodeWithStructDependence) Name() string {
	return m.name
}

func (m *MetaFlowNodeWithStructDependence) Dependence() []*Dependence[State] {
	return m.dependence
}

func (m *MetaFlowNodeWithStructDependence) Run(ctx context.Context, state State, plan *Plan) error {
	return m.f(ctx, state, plan)
}


func TestAddNodes(t *testing.T) {
	node := new(Node[State])

	eng := NewEngine[State]("")
	node1 := NewMetaBasicNodeWithStringDependence("A1", nil, func(ctx context.Context, state State) error { fmt.Println("A1"); return nil })
	node2 := NewMetaBasicNodeWithStringDependence("A2", nil, func(ctx context.Context, state State) error { fmt.Println("A2"); return nil })
	node3 := NewMetaBasicNodeWithStringDependence("A3", []string{"A1", "A2"}, func(ctx context.Context, state State) error { fmt.Println("A3"); return nil })
	node4 := NewMetaFlowNodeWithStringDependence("B1", []string{"A1"}, func(ctx context.Context, state State, plan *Plan) error { fmt.Println("B1", plan.CurrentNode); return nil })
	node5 := NewMetaFlowNodeWithStringDependence("B2", []string{"A2"}, func(ctx context.Context, state State, plan *Plan) error { fmt.Println("B2", plan.CurrentNode); return nil })
	node6 := NewMetaFlowNodeWithStringDependence("B3", []string{}, func(ctx context.Context, state State, plan *Plan) error { fmt.Println("B3", plan.CurrentNode); return nil })
	node7 := NewMetaBasicNodeWithStructDependence("C1", nil, func(ctx context.Context, state State) error { fmt.Println("C1"); return nil })
	node8 := NewMetaBasicNodeWithStructDependence("C2", []*Dependence[State]{node.StaticDependence("B3")}, func(ctx context.Context, state State) error { fmt.Println("C2"); return nil })
	node9 := NewMetaBasicNodeWithStructDependence("C3", []*Dependence[State]{node.ConditionalDependence("B1", func(ctx context.Context, s State) bool { fmt.Println("Cond1"); return true }, []string{"A1"})}, func(ctx context.Context, state State) error { fmt.Println("C3"); return nil })
	node10 := NewMetaBasicNodeWithStructDependence("C4", []*Dependence[State]{node.ConditionalDependence("B2", func(ctx context.Context, s State) bool { fmt.Println("Cond2"); return false }, []string{"A1"})}, func(ctx context.Context, state State) error { fmt.Println("C4"); return nil })


	assert.NoError(t, AddNode(eng, node1))
	assert.NoError(t, AddNode(eng, node2))
	assert.NoError(t, AddNode(eng, node3))
	assert.NoError(t, AddNode(eng, node4))
	assert.NoError(t, AddNode(eng, node5))
	assert.NoError(t, AddNode(eng, node6))
	assert.NoError(t, AddNode(eng, node7))
	assert.NoError(t, AddNode(eng, node8))
	assert.NoError(t, AddNode(eng, node9))
	assert.NoError(t, AddNode(eng, node10))

	assert.NoError(t, eng.Prepare())

	// assert.NoError(t, eng.Run(context.Background(), State{}, "A1"))
	// fmt.Println("===")
	// assert.NoError(t, eng.Run(context.Background(), State{}, "C3"))
	// fmt.Println("===")
	// assert.NoError(t, eng.Run(context.Background(), State{}, "C4"))
	// fmt.Println("===")
	// assert.NoError(t, eng.Run(context.Background(), State{}, "C4", "C3"))
	fmt.Println(eng.Dot())
}
