package kflow

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type State struct {
	Lock           sync.Mutex
	Stamps         []string
	ConcurrentInfo []bool
}

type BasicNode struct {
	name string
}

func (b BasicNode) Name() string {
	return b.name
}

func (b BasicNode) Run(ctx context.Context, state *State) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, b.name)
	return nil
}

type FlowNode struct {
	name string
}

func (b FlowNode) Name() string {
	return b.name
}

func (b FlowNode) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, b.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
	return nil
}

type StringDependence struct {
	dependence []string
}

func (m StringDependence) Dependence() []string {
	return m.dependence
}

type ConditionDependence struct {
	dependence []*Dependence[*State]
}

func (m ConditionDependence) Dependence() []*Dependence[*State] {
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

func NewNodeType2(name string, dep []*Dependence[*State]) *NodeType2 {
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

func NewNodeType4(name string, dep []*Dependence[*State]) *NodeType4 {
	return &NodeType4{
		FlowNode: FlowNode{
			name: name,
		},
		ConditionDependence: ConditionDependence{
			dependence: dep,
		},
	}
}

func TestNormalNodeGraph1(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"), node.ConditionalDependence("Type1_1", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.StaticDependence("Type1_2"), node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1"))
	assert.Equal(t, "Type1_1", state.Stamps[0])
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[1:3])
	assert.Equal(t, "Type2_1", state.Stamps[3])
	assert.Equal(t, []bool{false, true, true, false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2"))
	assert.Equal(t, "Type1_1", state.Stamps[0])
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_4"}, state.Stamps[1:3])
	assert.Equal(t, "Type1_3", state.Stamps[3])
	assert.Equal(t, "Type2_1", state.Stamps[4])
	assert.Equal(t, "Type2_2", state.Stamps[5])
	assert.Equal(t, []bool{false, true, true, false, false, false}, state.ConcurrentInfo)
}

func TestNormalNodeGraph2(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"),
		node.ConditionalDependence("Type1_5", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1"))
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[0:2])
	assert.Equal(t, "Type2_1", state.Stamps[2])
	assert.Equal(t, []bool{true, true, false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2"))
	assert.ElementsMatch(t, []string{"Type1_1", "Type1_2", "Type1_4"}, state.Stamps[0:3])
	assert.Equal(t, "Type1_3", state.Stamps[3])
	assert.Equal(t, "Type2_1", state.Stamps[4])
	assert.Equal(t, "Type2_2", state.Stamps[5])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[:2])
	assert.Equal(t, []bool{false, false, false}, state.ConcurrentInfo[3:])
}

func TestNormalNodeGraph3(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"),
		node.ConditionalDependence("Type1_5", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1"))
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[0:2])
	assert.Equal(t, "Type2_1", state.Stamps[2])
	assert.Equal(t, []bool{true, true, false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2"))
	assert.ElementsMatch(t, []string{"Type1_1", "Type1_2", "Type1_4"}, state.Stamps[0:3])
	assert.Equal(t, "Type2_2", state.Stamps[3])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[:2])
	assert.False(t, state.ConcurrentInfo[3])
}

func TestNormalNodeGraph4(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"),
		node.ConditionalDependence("Type1_5", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1"))
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[0:2])
	assert.Equal(t, "Type1_6", state.Stamps[2])
	assert.Equal(t, "Type1_5", state.Stamps[3])
	assert.Equal(t, "Type2_1", state.Stamps[4])
	assert.Equal(t, []bool{true, true, false, false, false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2"))
	assert.ElementsMatch(t, []string{"Type1_1", "Type1_2", "Type1_4"}, state.Stamps[0:3])
	assert.ElementsMatch(t, []string{"Type1_3", "Type1_6", "Type1_5"}, state.Stamps[3:6])
	assert.Equal(t, "Type2_1", state.Stamps[6])
	assert.Equal(t, "Type2_2", state.Stamps[7])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[:2])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[3:5])
	assert.Equal(t, []bool{false, false}, state.ConcurrentInfo[6:])
}

func TestNormalNodeGraph5(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))

	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"),
		node.ConditionalDependence("Type1_5", func(ctx context.Context, s *State) bool { panic("") }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { panic("") }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	fmt.Println(eng.Dot())

	state := new(State)
	// assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	// assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	// assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	// state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1"))
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[0:2])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2"))
	assert.Equal(t, 3, len(state.Stamps))
	assert.Equal(t, 3, len(state.ConcurrentInfo))
	assert.ElementsMatch(t, []string{"Type1_1", "Type1_2", "Type1_4"}, state.Stamps[0:3])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[:2])
}
