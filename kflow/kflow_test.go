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

type NodeTimeoutType struct {
	FlowNode
	ConditionDependence
	timeout time.Duration
}

func (n *NodeTimeoutType) Run(ctx context.Context, state *State, plan *Plan) error {
	time.Sleep(n.timeout * time.Second)
	return nil
}

func NewNodeTimeoutType(name string, dep []*Dependence[*State], timeout int64) *NodeTimeoutType {
	return &NodeTimeoutType{
		FlowNode: FlowNode{
			name: name,
		},
		ConditionDependence: ConditionDependence{
			dependence: dep,
		},
		timeout: time.Duration(timeout),
	}
}

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

	for _, node := range n.nodes {
		err := plan.AddTargetNode(node)
		if err != nil {
			return err
		}
	}
	return nil
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
	err = plan.AddTargetNode("Type_2")
	assert.Error(t, err)

	return nil
}

func NewNodePlanOperatorType(name string, dep []string, t *testing.T) *NodePlanOperatorType {
	return &NodePlanOperatorType{
		FlowNode: FlowNode{
			name: name,
		},
		StringDependence: StringDependence{
			dependence: dep,
		},
		t: t,
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
	// fmt.Println(eng.Dot())

	state := new(State)
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1"))
	assert.Equal(t, []string{"Type1_1"}, state.Stamps)
	assert.Equal(t, []bool{false}, state.ConcurrentInfo)

	state = new(State)
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
	eng := NewEngine[*State]("", AsyncTimeout(5))
	assert.NoError(t, AddNode(eng, NewNodeType1("Type1_1", []string{
		"TypeTimeout_1", "TypeTimeout_2",
	})))
	assert.NoError(t, AddNode(eng, NewNodeTimeoutType("TypeTimeout_1", []*Dependence[*State]{}, 7)))
	assert.NoError(t, AddNode(eng, NewNodeTimeoutType("TypeTimeout_2", []*Dependence[*State]{}, 10)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))
	assert.NoError(t, eng.Prepare())
	assert.EqualError(t, eng.Run(context.Background(), state, "PlanExtractor", "Type1_1"), "节点运行超时")
	now := time.Now()
	assert.Greater(t, now.Sub(plan.GetStartTime()).Seconds(), 5.0)
	assert.Less(t, now.Sub(plan.GetStartTime()).Seconds(), 10.0)

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
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_1", nil, t)))
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_2", nil, t)))
	assert.NoError(t, AddNode(eng, NewNodePlanOperatorType("Operator1_3", nil, t)))
	assert.NoError(t, AddNode(eng, NewNodePlanExtractor("PlanExtractor", nil, &plan)))

	assert.NoError(t, eng.Prepare())
	assert.NoError(t, eng.Run(context.Background(), state, "Type1_1", "PlanExtractor"))

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
