package kflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

// NodeType1 is a basic node with string dependence.
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

// NodeType2 is a basic node with condition dependence.
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

// NodeType3 is a flow node with string dependence.
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

// NodeType4 is a flow node with condition dependence.
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

// NodeTimeCostType is a flow node with timeout.
type NodeTimeCostType struct {
	FlowNode
	ConditionDependence
	timecost time.Duration
}

func (n *NodeTimeCostType) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
	state.Lock.Unlock()
	time.Sleep(n.timecost * time.Second)
	return nil
}

func NewNodeTimeCostType(name string, dep []*Dependence[*State], timeout int64) *NodeTimeCostType {
	return &NodeTimeCostType{
		FlowNode: FlowNode{
			name: name,
		},
		ConditionDependence: ConditionDependence{
			dependence: dep,
		},
		timecost: time.Duration(timeout),
	}
}

// NodeErrorType is a basic node with error.
type NodeErrorType struct {
	BasicNode
	StringDependence
}

func (*NodeErrorType) Run(ctx context.Context, state *State) error {
	return errors.New("something wrong")
}

func NewNodeErrorType(name string, dep []string) *NodeErrorType {
	return &NodeErrorType{
		BasicNode: BasicNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

// NodePanicType is a basic node with panic.
type NodePanicType struct {
	BasicNode
	StringDependence
}

func (*NodePanicType) Run(ctx context.Context, state *State) error {
	panic("panic, something wrong")
}

func NewNodePanicType(name string, dep []string) *NodePanicType {
	return &NodePanicType{
		BasicNode: BasicNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

// NodeStopType is a flow node to stop the plan.
type NodeStopType struct {
	FlowNode
	StringDependence
}

func (n *NodeStopType) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
	plan.Stop()
	return nil
}

func NewNodeStopType(name string, dep []string) *NodeStopType {
	return &NodeStopType{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

// NodePlanExtractor is a flow node to extract the plan.
type NodePlanExtractor struct {
	plan **Plan
	FlowNode
	StringDependence
}

func (n *NodePlanExtractor) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())

	*n.plan = plan
	return nil
}

func NewNodePlanExtractor(name string, dep []string, plan **Plan) *NodePlanExtractor {
	return &NodePlanExtractor{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
		plan: plan,
	}
}

// NodeAddNodeType is a flow node to add target nodes.
type NodeAddNodeType struct {
	FlowNode
	StringDependence
	nodes []string
}

func (n *NodeAddNodeType) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
	return plan.AddTargetNodes(n.nodes...)
}

func NewNodeAddNodeType(name string, dep []string, nodes []string) *NodeAddNodeType {
	return &NodeAddNodeType{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
		nodes: nodes,
	}
}

// NodePlanOperatorType is a flow node to operate the plan.
type NodePlanOperatorType struct {
	FlowNode
	StringDependence
	t *testing.T
}

func (n *NodePlanOperatorType) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())

	t := n.t
	assert.NotNil(t, plan.GetConfig())
	assert.NotEmpty(t, plan.GetCurrentNode())
	_, err := plan.GetExecuteResult()
	assert.Error(t, err)
	_, err = plan.GetTargetNodes()
	assert.Error(t, err)
	err = plan.AddTargetNodes("Type_2")
	assert.Error(t, err)

	return nil
}

func NewNodePlanOperatorType(name string, dep []string) *NodePlanOperatorType {
	return &NodePlanOperatorType{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

// NodeExecuteInRunTimeType is a flow node to execute in run time. It will run first batch in sequence and second batch in parallel.
type NodeExecuteInRunTimeType struct {
	FlowNode
	StringDependence
	batches  [][]string
	parallel []bool
}

func (n *NodeExecuteInRunTimeType) WithSequence(batch ...string) *NodeExecuteInRunTimeType {
	n.batches = append(n.batches, batch)
	n.parallel = append(n.parallel, false)
	return n
}

func (n *NodeExecuteInRunTimeType) WithParallel(batch ...string) *NodeExecuteInRunTimeType {
	n.batches = append(n.batches, batch)
	n.parallel = append(n.parallel, true)
	return n
}

func (n *NodeExecuteInRunTimeType) Run(ctx context.Context, state *State, plan *Plan) error {

	for i, batch := range n.batches {
		if i != 0 {
			state.Lock.Lock()
			state.Stamps = append(state.Stamps, n.name)
			state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
			state.Lock.Unlock()
		}
		if n.parallel[i] {
			err := ExecuteInParallel(ctx, state, plan, batch...)
			if err != nil {
				return err
			}
		} else {
			err := ExecuteInSequence(ctx, state, plan, batch...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func NewNodeExecuteInRunType(name string, dep []string) *NodeExecuteInRunTimeType {
	return &NodeExecuteInRunTimeType{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
	}
}

type NodeResultResultType[T any] struct {
	FlowNode
	StringDependence
	removes []string
}

func (n *NodeResultResultType[T]) Run(ctx context.Context, state *State, plan *Plan) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	state.Stamps = append(state.Stamps, n.name)
	state.ConcurrentInfo = append(state.ConcurrentInfo, plan.InParallel())
	for _, remove := range n.removes {
		err := RemoveResult[T](plan, remove)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewNodeResultResultType[T any](name string, dep []string, removes []string) *NodeResultResultType[T] {
	return &NodeResultResultType[T]{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
		removes: removes,
	}
}

// ==================== Test Cases ====================

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

	var plan *Plan

	eng := NewEngine[*State]("", SafeRun)
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"),
		node.ConditionalDependence("Type1_5", func(ctx context.Context, s *State) bool { panic("") }, []string{"Type1_2"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { panic("") }, []string{"Type1_2", "Type1_4"}),
	})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1", "PlanExtractor"))
	assert.ElementsMatch(t, []string{"Type1_2", "Type1_3"}, state.Stamps[0:2])
	assert.Equal(t, []bool{true, true, false}, state.ConcurrentInfo)
	assert.True(t, plan.finishedNodes["Type1_5_by_Type2_1"].IsPanic)
	assert.NotContains(t, plan.finishedNodes, "Type1_5")

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_2", "PlanExtractor"))
	assert.Equal(t, 4, len(state.Stamps))
	assert.Equal(t, 4, len(state.ConcurrentInfo))
	assert.ElementsMatch(t, []string{"Type1_1", "Type1_2", "Type1_4"}, state.Stamps[0:3])
	assert.Equal(t, []bool{true, true}, state.ConcurrentInfo[:2])
	assert.True(t, plan.finishedNodes["Type2_1_by_Type2_2"].IsPanic)
	assert.NotContains(t, plan.finishedNodes, "Type2_1")
}

func TestNormalNodeGraph6(t *testing.T) {
	var plan *Plan

	eng := NewEngine[*State]("", SafeRun)
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodePanicType("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodePanicType("Type1_5", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_3", "Type1_2", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false}, state.ConcurrentInfo)
	assert.False(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_3"].IsPanic)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_4", "Type1_2", "Type1_5", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false}, state.ConcurrentInfo)
	assert.False(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_3"].IsPanic)
	assert.False(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Skipped)
	assert.Equal(t, "节点[Type1_4]存在执行失败的依赖节点[Type1_3]", plan.finishedNodes["Type1_4"].SkippedReason)
	assert.False(t, plan.finishedNodes["Type1_4"].IsPanic)
	assert.False(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Skipped)
	assert.Equal(t, "节点[Type1_5]存在执行失败的依赖节点[Type1_3]", plan.finishedNodes["Type1_5"].SkippedReason)
	assert.False(t, plan.finishedNodes["Type1_5"].IsPanic)

	eng = NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodePanicType("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodePanicType("Type1_5", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.Panics(t, func() {
		state = new(State)
		assert.NoError(t, eng.Run(context.Background(), state, "Type1_3", "Type1_2", "PlanExtractor"))
	})

}

func TestNormalNodeGraph7(t *testing.T) {
	node := new(Node[*State])

	var plan *Plan
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType2("Type1_1", []*Dependence[*State]{
		node.ConditionalDependence("Type1_2", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_3"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_2"})))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_4", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_3", "Type1_1", "Type1_2", "Type1_4", "PlanExtractor"}, state.Stamps)
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_2_by_Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_2_by_Type1_1"].Skipped)
}

func TestNormalNodeGraph8(t *testing.T) {
	node := new(Node[*State])
	var plan *Plan
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1", nil).WithParallel("Type1_2", "Type1_1", "Type1_3", "Type1_4")))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_1", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_3", []*Dependence[*State]{
		node.ConditionalDependence("Type1_2", func(ctx context.Context, s *State) bool { return false }, nil),
	}, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_4", []*Dependence[*State]{
		node.StaticDependence("Type1_2"),
	}, 1)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Operator1", "PlanExtractor"))
	now := time.Now()
	assert.Greater(t, now.Sub(plan.GetStartTime()).Seconds(), 0.9)
	assert.Less(t, now.Sub(plan.GetStartTime()).Seconds(), 1.5)
	type12Count := 0
	for _, item := range state.Stamps {
		if item == "Type1_2" {
			type12Count++
		}
	}
	assert.Equal(t, 1, type12Count)
}

func TestStop(t *testing.T) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_0", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeStopType("Type1_2", []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_3", []*Dependence[*State]{nil})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_4", []*Dependence[*State]{node.StaticDependence("Type1_3")})))

	assert.NoError(t, eng.Prepare())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_0", "Type1_2", "Type1_4"))
	assert.Equal(t, []string{"Type1_0", "Type1_1", "Type1_2"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false}, state.ConcurrentInfo)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_0", "Type1_1", "Type1_4"))
	assert.Equal(t, []string{"Type1_0", "Type1_1", "Type1_3", "Type1_4"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false}, state.ConcurrentInfo)
}

func TestLanguage(t *testing.T) {
	node := new(Node[*State])
	eng := NewEngine[*State]("", ReportInChinese)
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))

	state := new(State)
	assert.EqualError(t, eng.Run(context.Background(), state, "Type2_2"), "调用前未准备, 请先调用Prepare方法")

	eng = NewEngine[*State]("", ReportInEnglish)
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.EqualError(t, eng.Run(context.Background(), state, "Type2_2"), "Not prepare before run, please call Prepare method before")

	eng = NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_2", []*Dependence[*State]{
		node.StaticDependence("xxx"),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_3", []*Dependence[*State]{nil})))

	assert.EqualError(t, eng.Prepare(), "节点[xxx]不存在")
}

func TestCycle(t *testing.T) {
	node := new(Node[*State])
	eng := NewEngine[*State]("", ReportInChinese)
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{"Type1_2"})))
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_2", []string{"Type1_1"})))
	assert.Error(t, eng.Prepare())
	assert.True(t, strings.HasPrefix(eng.Prepare().Error(), "检测到环存在"))

	eng = NewEngine[*State]("", ReportInChinese)
	assert.NoError(t, AddNode(eng, NewNodeType2("Type1_1", []*Dependence[*State]{
		node.ConditionalDependence("Type1_2", func(ctx context.Context, s *State) bool { return false }, nil),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType2("Type1_2", []*Dependence[*State]{
		node.ConditionalDependence("Type1_1", func(ctx context.Context, s *State) bool { return false }, nil),
	})))
	assert.Error(t, eng.Prepare())
	assert.True(t, strings.HasPrefix(eng.Prepare().Error(), "检测到环存在"))
	// fmt.Println(eng.Dot())
}

func TestTimeoutAndError(t *testing.T) {
	node := new(Node[*State])

	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("", AsyncTimeout(1))
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{
		"TypeTimeout_1", "TypeTimeout_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("TypeTimeout_1", []*Dependence[*State]{}, 2)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("TypeTimeout_2", []*Dependence[*State]{}, 3)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, eng.Prepare())
	assert.EqualError(t, eng.Run(context.Background(), state, "PlanExtractor", "Type1_1"), "节点运行超时")
	now := time.Now()
	assert.Greater(t, now.Sub(plan.GetStartTime()).Seconds(), 1.0)
	assert.Less(t, now.Sub(plan.GetStartTime()).Seconds(), 1.5)

	plan = nil
	eng = NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType2("Type2_1", []*Dependence[*State]{
		node.StaticDependence("TypeError_1"), node.StaticDependence("TypePanic_1"),
	})))
	assert.NoError(t, AddNode(eng, NewNodeErrorType("TypeError_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodePanicType("TypePanic_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, eng.Prepare())

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type2_1", "PlanExtractor"))
	result, err := plan.GetExecuteResult()
	assert.Nil(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, "PlanExtractor", plan.GetCurrentNode())
	targets, err := plan.GetTargetNodes()
	assert.Nil(t, err)
	assert.Equal(t, []string{}, targets)
	assert.False(t, plan.finishedNodes["TypeError_1"].Success)
	assert.False(t, plan.finishedNodes["TypePanic_1"].Success)
	assert.False(t, plan.finishedNodes["Type2_1"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	assert.False(t, plan.finishedNodes["TypeError_1"].Skipped)
	assert.False(t, plan.finishedNodes["TypePanic_1"].Skipped)
	assert.True(t, plan.finishedNodes["Type2_1"].Skipped)
	assert.False(t, plan.finishedNodes["PlanExtractor"].Skipped)

	assert.Equal(t, "Type2_1", plan.finishedNodes["TypeError_1"].ExecuteBy)
	assert.Equal(t, "Type2_1", plan.finishedNodes["TypePanic_1"].ExecuteBy)
	assert.Equal(t, "Type2_1", plan.finishedNodes["Type2_1"].ExecuteBy)
	assert.Equal(t, "PlanExtractor", plan.finishedNodes["PlanExtractor"].ExecuteBy)

	chain := plan.GetChainNodes()
	assert.Equal(t, []string{"Type2_1", "PlanExtractor"}, chain)
}

func TestAddNodesDynamically(t *testing.T) {
	node := new(Node[*State])

	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("", AsyncTimeout(5))
	assert.NoError(t, AddNode(eng, NewNodeAddNodeType("TypeAddNode_1", []string{
		"Type1_1",
	}, []string{"Type1_2", "Type1_8"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_3", []*Dependence[*State]{
		node.ConditionalDependence("Type1_4", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_5"}),
		node.ConditionalDependence("Type1_6", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_7"}),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_7", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_8", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Prepare())
	fmt.Println(eng.Dot())
	assert.NoError(t, eng.Run(context.Background(), state, "TypeAddNode_1", "PlanExtractor"))
	results, err := plan.GetExecuteResult()
	assert.NoError(t, err)
	assert.Equal(t, 11, len(results))
	assert.ElementsMatch(t, []string{"TypeAddNode_1", "Type1_1", "Type1_2", "Type1_3", "Type1_4", "Type1_5", "Type1_7", "Type1_8", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false}, state.ConcurrentInfo[0:2])
	assert.False(t, state.ConcurrentInfo[8])

}

func TestParallelFlowNode(t *testing.T) {
	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{
		"Operator1_1",
		"Operator1_2",
		"Operator1_3",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))

}

func TestExecuteInRunTime(t *testing.T) {
	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{
		"Operator1_1",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_1", nil).WithSequence("Type1_3", "Type1_5").WithParallel("Type1_4")))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_7", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))
	assert.ElementsMatch(t, []string{"Type1_3", "Type1_6", "Type1_5"}, state.Stamps[:3])
	assert.Equal(t, "Operator1_1", state.Stamps[3])
	assert.ElementsMatch(t, []string{"Type1_4", "Type1_1", "PlanExtractor"}, state.Stamps[4:7])
	assert.Equal(t, 7, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Operator1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	eng = NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{
		"Operator1_1",
		"Type1_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_1", nil).WithSequence("Type1_3", "Type1_5").WithParallel("Type1_4")))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_2", nil).WithParallel("Type1_3", "Type1_5").WithSequence("Type1_4")))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", []string{"Type1_6"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_7", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_8", []string{
		"Operator1_2",
		"Type1_2",
	})))

	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, eng.Prepare())

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))
	assert.Equal(t, 2, len(state.Stamps))
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, 4, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
	assert.False(t, plan.finishedNodes["Operator1_1"].Success)
	assert.Error(t, errors.New("操作不支持在并行环境中运行"), plan.finishedNodes["Operator1_1"].Err)
	assert.False(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_1"].Skipped)
	assert.NoError(t, plan.finishedNodes["Type1_1"].Err)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_8", "PlanExtractor"))
	assert.Equal(t, 2, len(state.Stamps))
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, 4, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_8", "PlanExtractor"}, plan.GetChainNodes())
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
	assert.False(t, plan.finishedNodes["Operator1_2"].Success)
	assert.Error(t, errors.New("操作不支持在并行环境中运行"), plan.finishedNodes["Operator1_2"].Err)
	assert.False(t, plan.finishedNodes["Type1_8"].Success)
	assert.True(t, plan.finishedNodes["Type1_8"].Skipped)
	assert.NoError(t, plan.finishedNodes["Type1_8"].Err)

}

func TestExecuteInRunTimeCheckParallel(t *testing.T) {
	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", []string{
		"Operator1_1",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", nil)))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_1", nil).WithSequence("Type1_2", "Type1_4").WithParallel("Type1_5", "Type1_6", "Type1_7")))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_7", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))
	assert.Equal(t, []bool{false, false, false, false, true, true, true, false, false}, state.ConcurrentInfo)
	assert.Equal(t, 9, len(state.Stamps))
	assert.Equal(t, []string{"Type1_2", "Type1_3", "Type1_4", "Operator1_1"}, state.Stamps[0:4])
	assert.ElementsMatch(t, []string{"Type1_5", "Type1_6", "Type1_7"}, state.Stamps[4:7])
	assert.Equal(t, []string{"Type1_1", "PlanExtractor"}, state.Stamps[7:])
	assert.Equal(t, 9, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["Operator1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_7"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

}

func TestExecuteInRunTimeCheckParallelTime(t *testing.T) {
	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", []string{
		"Operator1_1",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{
		"Operator1_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_1", nil).WithSequence("Type1_3", "Type1_4").WithSequence("Type1_5", "Type1_6")))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_2", nil).WithParallel("Type1_3", "Type1_4").WithParallel("Type1_5", "Type1_6", "Type1_7")))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_3", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_4", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_5", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_6", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_7", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodeTimeCostType("Type1_8", nil, 1)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	now := time.Now()
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))
	assert.Equal(t, []bool{false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_3", "Type1_4", "Operator1_1", "Type1_5", "Type1_6", "Type1_1", "PlanExtractor"}, state.Stamps)
	cost := time.Since(now).Seconds()
	assert.GreaterOrEqual(t, cost, 4.0)
	assert.LessOrEqual(t, cost, 4.5)
	assert.Equal(t, 7, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Operator1_1"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
	assert.Equal(t, "Type1_1", plan.finishedNodes["Type1_3"].ExecuteBy)
	assert.Equal(t, "Type1_1", plan.finishedNodes["Type1_4"].ExecuteBy)
	assert.Equal(t, "Type1_1", plan.finishedNodes["Type1_5"].ExecuteBy)
	assert.Equal(t, "Type1_1", plan.finishedNodes["Type1_6"].ExecuteBy)

	now = time.Now()
	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_2", "PlanExtractor"))
	assert.Equal(t, []bool{true, true, false, true, true, true, false, false}, state.ConcurrentInfo)
	assert.ElementsMatch(t, []string{"Type1_3", "Type1_4"}, state.Stamps[:2])
	assert.Equal(t, "Operator1_2", state.Stamps[2])
	assert.ElementsMatch(t, []string{"Type1_5", "Type1_6", "Type1_7"}, state.Stamps[3:6])
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, state.Stamps[6:])
	cost = time.Since(now).Seconds()
	assert.GreaterOrEqual(t, cost, 2.0)
	assert.LessOrEqual(t, cost, 2.5)
	assert.Equal(t, 8, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["Type1_7"].Success)
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["Operator1_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
	assert.Equal(t, "Type1_2", plan.finishedNodes["Type1_3"].ExecuteBy)
	assert.Equal(t, "Type1_2", plan.finishedNodes["Type1_4"].ExecuteBy)
	assert.Equal(t, "Type1_2", plan.finishedNodes["Type1_5"].ExecuteBy)
	assert.Equal(t, "Type1_2", plan.finishedNodes["Type1_6"].ExecuteBy)
	assert.Equal(t, "Type1_2", plan.finishedNodes["Type1_7"].ExecuteBy)

	now = time.Now()
	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Operator1_2", "PlanExtractor"))
	assert.Equal(t, []bool{true, true, false, true, true, true, false}, state.ConcurrentInfo)
	assert.Equal(t, "Operator1_2", state.Stamps[2])
	assert.ElementsMatch(t, []string{"Type1_5", "Type1_6", "Type1_7"}, state.Stamps[3:6])
	assert.Equal(t, []string{"PlanExtractor"}, state.Stamps[6:])
	cost = time.Since(now).Seconds()
	assert.GreaterOrEqual(t, cost, 2.0)
	assert.LessOrEqual(t, cost, 2.5)
	assert.Equal(t, 7, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["Type1_7"].Success)
	assert.True(t, plan.finishedNodes["Operator1_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
	assert.Equal(t, "Operator1_2", plan.finishedNodes["Type1_3"].ExecuteBy)
	assert.Equal(t, "Operator1_2", plan.finishedNodes["Type1_4"].ExecuteBy)
	assert.Equal(t, "Operator1_2", plan.finishedNodes["Type1_5"].ExecuteBy)
	assert.Equal(t, "Operator1_2", plan.finishedNodes["Type1_6"].ExecuteBy)
	assert.Equal(t, "Operator1_2", plan.finishedNodes["Type1_7"].ExecuteBy)
}

func TestExecuteInRunTimeCheckEmptyBatch(t *testing.T) {
	var plan *Plan

	state := new(State)
	eng := NewEngine[*State]("")
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", []string{
		"Operator1_1",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{
		"Operator1_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_1", nil).WithSequence().WithParallel("Type1_5", "Type1_6", "Type1_7")))
	assert.NoError(t, AddNode(eng, NewNodeExecuteInRunType("Operator1_2", nil).WithParallel().WithParallel("Type1_5", "Type1_6", "Type1_7")))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_7", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"PlanExtractor"}, state.Stamps)
	assert.Equal(t, 3, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.False(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_1"].Skipped)
	assert.NoError(t, plan.finishedNodes["Type1_1"].Err)
	assert.False(t, plan.finishedNodes["Operator1_1"].Success)
	assert.False(t, plan.finishedNodes["Operator1_1"].Skipped)
	assert.EqualError(t, plan.finishedNodes["Operator1_1"].Err, "没有目标节点可以运行")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_2", "PlanExtractor"))
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"PlanExtractor"}, state.Stamps)
	assert.Equal(t, 3, len(plan.finishedNodes))
	assert.Equal(t, []string{"Type1_2", "PlanExtractor"}, plan.GetChainNodes())
	assert.False(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["Type1_2"].Skipped)
	assert.NoError(t, plan.finishedNodes["Type1_2"].Err)
	assert.False(t, plan.finishedNodes["Operator1_2"].Success)
	assert.False(t, plan.finishedNodes["Operator1_2"].Skipped)
	assert.EqualError(t, plan.finishedNodes["Operator1_2"].Err, "没有目标节点可以运行")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

}

func TestRemoveResult(t *testing.T) {
	node := new(Node[*State])
	var plan *Plan

	eng := NewEngine[*State]("", SafeRun)
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_1", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_2", []string{"Type1_4", "Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_3", []string{"Type1_4"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_5"})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_5", nil)))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_6", []string{
		"RemoveResult_1", "RemoveResult_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeType4("Type1_7", []*Dependence[*State]{
		node.ConditionalDependence("Type1_8", func(ctx context.Context, s *State) bool { return true }, nil),
	})))
	assert.NoError(t, AddNode(eng, NewNodeType3("Type1_8", nil)))
	assert.NoError(t, AddNode(eng, NewNodePanicType("Type1_9", nil)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_1", nil, []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_2", nil, []string{"Type1_3"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_3", nil, []string{"Type1_8_by_Type1_7"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_4", nil, []string{"Type1_5"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_5", nil, []string{"nothing"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[State]("RemoveResult_6", nil, []string{"Type1_1"})))
	assert.NoError(t, AddNode(eng, NewNodeResultResultType[*State]("RemoveResult_7", nil, []string{"Type1_9"})))

	assert.NoError(t, eng.Prepare())
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "Type1_6", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2"}, state.Stamps[:5])
	assert.ElementsMatch(t, []string{"RemoveResult_1", "RemoveResult_2"}, state.Stamps[5:7])
	assert.Equal(t, []string{"PlanExtractor"}, state.Stamps[7:])
	assert.Equal(t, []bool{false, false, false, false, false, true, true, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "Type1_6", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 9, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.False(t, plan.finishedNodes["Type1_6"].Success)
	assert.True(t, plan.finishedNodes["Type1_6"].Skipped)
	assert.NoError(t, plan.finishedNodes["Type1_6"].Err)
	assert.False(t, plan.finishedNodes["RemoveResult_1"].Success)
	assert.False(t, plan.finishedNodes["RemoveResult_1"].Skipped)
	assert.EqualError(t, plan.finishedNodes["RemoveResult_1"].Err, "操作不支持在并行环境中运行")
	assert.False(t, plan.finishedNodes["RemoveResult_2"].Success)
	assert.False(t, plan.finishedNodes["RemoveResult_2"].Skipped)
	assert.EqualError(t, plan.finishedNodes["RemoveResult_2"].Err, "操作不支持在并行环境中运行")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_1", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_1", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 6, len(plan.finishedNodes))
	assert.NotContains(t, plan.finishedNodes, "Type1_1")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["RemoveResult_1"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_1", "Type1_1", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_1", "Type1_1", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_1", "Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 7, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["RemoveResult_1"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_7", "Type1_2", "RemoveResult_5", "RemoveResult_3", "Type1_1", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_8", "Type1_7", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_5", "RemoveResult_3", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_7", "Type1_2", "RemoveResult_5", "RemoveResult_3", "Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 11, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_2"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["Type1_3"].Success)
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.True(t, plan.finishedNodes["Type1_8_by_Type1_7"].Success)
	assert.True(t, plan.finishedNodes["Type1_7"].Success)
	assert.True(t, plan.finishedNodes["Type1_8"].Success)
	assert.False(t, plan.finishedNodes["RemoveResult_5"].Success)
	assert.EqualError(t, plan.finishedNodes["RemoveResult_5"].Err, "节点[nothing]不存在")
	assert.False(t, plan.finishedNodes["RemoveResult_3"].Success)
	assert.EqualError(t, plan.finishedNodes["RemoveResult_3"].Err, "节点[Type1_8_by_Type1_7]的类型不支持")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_2", "Type1_1", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_2", "Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 5, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_4")
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_5")
	assert.NotContains(t, plan.finishedNodes, "Type1_3")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_3")
	assert.NotContains(t, plan.finishedNodes, "Type1_2")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["RemoveResult_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_2", "Type1_1", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_2", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_2", "Type1_1", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 5, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_4")
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_5")
	assert.NotContains(t, plan.finishedNodes, "Type1_3")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_3")
	assert.NotContains(t, plan.finishedNodes, "Type1_2")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["RemoveResult_2"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_4", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_4", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_4", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 3, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.NotContains(t, plan.finishedNodes, "Type1_4")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_4")
	assert.NotContains(t, plan.finishedNodes, "Type1_5")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_5")
	assert.NotContains(t, plan.finishedNodes, "Type1_3")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_3")
	assert.NotContains(t, plan.finishedNodes, "Type1_2")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["RemoveResult_4"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "Type1_2", "RemoveResult_4", "Type1_4", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "Type1_5", "Type1_4", "Type1_3", "Type1_2", "RemoveResult_4", "Type1_5", "Type1_4", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false, false, false, false, false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "Type1_2", "RemoveResult_4", "Type1_4", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 5, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.True(t, plan.finishedNodes["Type1_4"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_4")
	assert.True(t, plan.finishedNodes["Type1_5"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_5")
	assert.NotContains(t, plan.finishedNodes, "Type1_3")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_3")
	assert.NotContains(t, plan.finishedNodes, "Type1_2")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_2")
	assert.True(t, plan.finishedNodes["RemoveResult_4"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "RemoveResult_6", "PlanExtractor"))
	assert.Equal(t, []string{"Type1_1", "RemoveResult_6", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_1", "RemoveResult_6", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 3, len(plan.finishedNodes))
	assert.True(t, plan.finishedNodes["Type1_1"].Success)
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_1")
	assert.False(t, plan.finishedNodes["RemoveResult_6"].Success)
	assert.EqualError(t, plan.finishedNodes["RemoveResult_6"].Err, "类型断言失败，请检查类型是否正确")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_9", "PlanExtractor"))
	assert.Equal(t, []string{"PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_9", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 2, len(plan.finishedNodes))
	assert.False(t, plan.finishedNodes["Type1_9"].Success)
	assert.True(t, plan.finishedNodes["Type1_9"].IsPanic)
	assert.EqualError(t, plan.finishedNodes["Type1_9"].Err, "panic: panic, something wrong")
	assert.Contains(t, plan.finishedOriginalNodes, "Type1_9")
	assert.Contains(t, plan.failedNodes, "Type1_9")
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)

	state = new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_9", "RemoveResult_7", "PlanExtractor"))
	assert.Equal(t, []string{"RemoveResult_7", "PlanExtractor"}, state.Stamps)
	assert.Equal(t, []bool{false, false}, state.ConcurrentInfo)
	assert.Equal(t, []string{"Type1_9", "RemoveResult_7", "PlanExtractor"}, plan.GetChainNodes())
	assert.Equal(t, 2, len(plan.finishedNodes))
	assert.NotContains(t, plan.finishedNodes, "Type1_9")
	assert.NotContains(t, plan.failedNodes, "Type1_9")
	assert.NotContains(t, plan.finishedOriginalNodes, "Type1_9")
	assert.True(t, plan.finishedNodes["RemoveResult_7"].Success)
	assert.True(t, plan.finishedNodes["PlanExtractor"].Success)
}

func BenchmarkNormalNodeGraph1(b *testing.B) {
	node := new(Node[*State])

	eng := NewEngine[*State]("")
	AddNode(eng, NewNodeType3("Type1_1", nil))
	AddNode(eng, NewNodeType3("Type1_2", []string{"Type1_1"}))
	AddNode(eng, NewNodeType3("Type1_3", []string{"Type1_1"}))
	AddNode(eng, NewNodeType3("Type1_4", []string{"Type1_1"}))

	AddNode(eng, NewNodeType4("Type2_1", []*Dependence[*State]{
		node.StaticDependence("Type1_3"), node.ConditionalDependence("Type1_1", func(ctx context.Context, s *State) bool { return false }, []string{"Type1_2"}),
	}))
	AddNode(eng, NewNodeType4("Type2_2", []*Dependence[*State]{
		node.StaticDependence("Type1_2"), node.ConditionalDependence("Type2_1", func(ctx context.Context, s *State) bool { return true }, []string{"Type1_2", "Type1_4"}),
	}))

	eng.Prepare()

	state := new(State)
	for i := 0; i < b.N; i++ {
		eng.Run(context.Background(), state, "Type2_2")
	}
}
