package kflowex_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Kidsunbo/kie_toolbox_go/kflowex"
	"github.com/stretchr/testify/assert"
)

type State struct {
	lock sync.Mutex
	Step []string
}

func (s *State) AddStep(step string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Step = append(s.Step, step)
}

type Dependency[S any] struct {
	Name         string
	Condition    kflowex.Condition[S]
	Dependencies []string
}

func (d Dependency[S]) GetName() string {
	return d.Name
}

func (d Dependency[S]) GetCondition() kflowex.Condition[S] {
	return d.Condition
}

func (d Dependency[S]) GetDependencies() []string {
	return d.Dependencies
}

type Description[S any] struct {
	Name                    string
	Dependencies            []string
	ConditionalDependencies []Dependency[S]
}

func (d Description[S]) GetName() string {
	return d.Name
}

func (d Description[S]) GetDependencies() []kflowex.IDependency[S] {
	deps := make([]kflowex.IDependency[S], 0, len(d.Dependencies))
	for _, dep := range d.Dependencies {
		deps = append(deps, Dependency[S]{
			Name: dep,
		})
	}
	for _, dep := range d.ConditionalDependencies {
		deps = append(deps, dep)
	}
	return deps
}

type Node1 struct{}

func NewNode1() *Node1 {
	return &Node1{}
}

func (n *Node1) Description() Description[*State] {
	return Description[*State]{
		Name: "Node1",
	}
}

func (n *Node1) Run(ctx context.Context, state *State) error {
	state.AddStep("Node1")
	return nil
}

type Node2 struct{}

func NewNode2() *Node2 {
	return &Node2{}
}

func (n *Node2) Description() Description[*State] {
	return Description[*State]{
		Name: "Node2",
	}
}

func (n *Node2) Run(ctx context.Context, state *State) error {
	state.AddStep("Node2")
	return nil
}

type Node3 struct{}

func NewNode3() *Node3 {
	return &Node3{}
}

func (n *Node3) Description() Description[*State] {
	return Description[*State]{
		Name:         "Node3",
		Dependencies: []string{"Node1", "Node2"},
	}
}

func (n *Node3) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node3")
	return nil
}

type Node4 struct{}

func NewNode4() *Node4 {
	return &Node4{}
}

func (n *Node4) Description() Description[*State] {
	return Description[*State]{
		Name:         "Node4",
		Dependencies: []string{"Node1"},
		ConditionalDependencies: []Dependency[*State]{
			{
				Name:         "Node2",
				Condition:    func(ctx context.Context, state *State) bool { return true },
				Dependencies: []string{"Node3"},
			},
		},
	}
}

func (n *Node4) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node4")
	return nil
}

type Node5 struct{}

func NewNode5() *Node5 {
	return &Node5{}
}

func (n *Node5) Description() Description[*State] {
	return Description[*State]{
		Name:         "Node5",
		Dependencies: []string{"Node1"},
		ConditionalDependencies: []Dependency[*State]{
			{
				Name:      "Node2",
				Condition: func(ctx context.Context, state *State) bool { return false },
			},
		},
	}
}

func (n *Node5) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node5")
	return kflowex.ExecuteSequentially(ctx, state, plan, "Node3", "Node4")
}

type Node6 struct{}

func NewNode6() *Node6 {
	return &Node6{}
}

func (n *Node6) Description() Description[*State] {
	return Description[*State]{
		Name: "Node6",
	}
}

func (n *Node6) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node6")
	err := kflowex.ExecuteSequentially(ctx, state, plan, "Node1", "Node2", "Node5")
	if err != nil {
		return err
	}
	err = kflowex.RemoveResult[*State](plan, "Node1")
	if err != nil {
		return err
	}

	err = kflowex.ExecuteParallel(ctx, state, plan, "Node3", "Node4", "Node5")
	if err != nil {
		return err
	}
	return nil
}

type Node7 struct{}

func NewNode7() *Node7 {
	return &Node7{}
}

func (n *Node7) Description() Description[*State] {
	return Description[*State]{
		Name: "Node7",
	}
}

func (n *Node7) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node7")
	panic("test panic")
}

type AddMW[S kflowex.IState, D kflowex.IDescription[S]] struct {
	name   string
	values *[]string
}

func (a *AddMW[S, D]) Before(desc D) {
	*a.values = append(*a.values, fmt.Sprintf("b_%v", a.name))
}

func (a *AddMW[S, D]) After(desc D, err error) {
	*a.values = append(*a.values, fmt.Sprintf("a_%v", a.name))
}

func RunMWFunc[S kflowex.IState, D kflowex.IDescription[S]](name string) kflowex.RunMiddleware[S, D] {
	return func(next kflowex.RunEndpoint[S, D]) kflowex.RunEndpoint[S, D] {
		return func(ctx context.Context, desc D, state S, plan *kflowex.Plan) error {
			any(state).(*State).AddStep(fmt.Sprintf("b_%v", name))
			err := next(ctx, desc, state, plan)
			any(state).(*State).AddStep(fmt.Sprintf("a_%v", name))
			return err
		}
	}
}

func TestFlowAddNodeSuccess(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node1", "Node2"))
	assert.Equal(t, []string{"Node1", "Node2"}, state.Step)
}

func TestFlowAddNodeFail(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.Error(t, kflowex.AddNode(f, NewNode1))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node1", "Node2"))
	assert.Error(t, engine.Run(context.Background(), state, "Node1", "Node3"))
	assert.Equal(t, []string{"Node1", "Node2"}, state.Step)
}

func TestFlowNodeDescription(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node1", "Node2"))
	assert.Equal(t, []string{"Node1", "Node2"}, state.Step)

	state = &State{
		Step: []string{},
	}
	assert.NoError(t, engine.Run(context.Background(), state, "Node3"))
	assert.Equal(t, 3, len(state.Step))
	assert.ElementsMatch(t, []string{"Node1", "Node2"}, state.Step[:2])
	assert.Equal(t, "Node3", state.Step[2])
}

func TestFlowNodeCondition(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
		assert.NoError(t, kflowex.AddNode(f, NewNode4))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node4"))
	assert.ElementsMatch(t, []string{"Node1", "Node2", "Node3"}, state.Step[:3])
	assert.Equal(t, "Node4", state.Step[3])
}

func TestFlowNodeExecute(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
		assert.NoError(t, kflowex.AddNode(f, NewNode4))
		assert.NoError(t, kflowex.AddNode(f, NewNode5))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node5"))
	assert.Equal(t, []string{"Node1", "Node5", "Node2", "Node3", "Node4"}, state.Step)
}

func TestFlowNodeExecuteAndRemove(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
		assert.NoError(t, kflowex.AddNode(f, NewNode4))
		assert.NoError(t, kflowex.AddNode(f, NewNode5))
		assert.NoError(t, kflowex.AddNode(f, NewNode6))
	}).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node6"))
	fmt.Println(state.Step)
	assert.Equal(t, []string{"Node6", "Node1", "Node2", "Node5", "Node3", "Node4"}, state.Step[:6])
	assert.ElementsMatch(t, []string{"Node1", "Node3", "Node4", "Node5"}, state.Step[6:])
}

func TestFlowNodeMiddleware(t *testing.T) {
	state := &State{}

	step := make([]string, 0)
	addMW1 := &AddMW[*State, Description[*State]]{
		name:   "addMW1",
		values: &step,
	}
	addMW2 := &AddMW[*State, Description[*State]]{
		name:   "addMW2",
		values: &step,
	}

	_, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
	}).WithAddMiddleware(addMW1, addMW2).Build()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b_addMW1", "b_addMW2", "a_addMW2", "a_addMW1", "b_addMW1", "b_addMW2", "a_addMW2", "a_addMW1", "b_addMW1", "b_addMW2", "a_addMW2", "a_addMW1"}, step)

	runMW1 := RunMWFunc[*State, Description[*State]]("runMW1")
	runMW2 := RunMWFunc[*State, Description[*State]]("runMW2")
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
	}).WithRunMiddleware(runMW1, runMW2).Build()
	assert.NoError(t, err)

	state = &State{
		Step: []string{},
	}
	assert.NoError(t, engine.Run(context.Background(), state, "Node3"))
	assert.Equal(t, 15, len(state.Step))
	assert.ElementsMatch(t, []string{"b_runMW1", "b_runMW2", "Node1", "a_runMW2", "a_runMW1", "b_runMW1", "b_runMW2", "Node2", "a_runMW2", "a_runMW1"}, state.Step[:10])
	assert.Equal(t, []string{"b_runMW1", "b_runMW2", "Node3", "a_runMW2", "a_runMW1"}, state.Step[10:15])
}

func TestOptions(t *testing.T) {
	state := &State{
		Step: []string{},
	}
	engine, err := kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
		assert.NoError(t, kflowex.AddNode(f, NewNode4))
		assert.NoError(t, kflowex.AddNode(f, NewNode5))
		assert.NoError(t, kflowex.AddNode(f, NewNode6))
		assert.NoError(t, kflowex.AddNode(f, NewNode7))
	}).WithReportInEnglish().WithSafeRun().WithAsyncTimeout(10).Build()
	assert.NoError(t, err)
	assert.NoError(t, engine.Run(context.Background(), state, "Node7"))
	assert.Equal(t, 1, len(state.Step))
	assert.Equal(t, "Node7", state.Step[0])

	assert.EqualError(t, engine.Run(context.Background(), state, "Node8"), "node [Node8] does not exist")

	engine, err = kflowex.NewFlowBuilder("test", func(f *kflowex.Flow[*State, Description[*State]]) {
		assert.NoError(t, kflowex.AddNode(f, NewNode1))
		assert.NoError(t, kflowex.AddNode(f, NewNode2))
		assert.NoError(t, kflowex.AddNode(f, NewNode3))
		assert.NoError(t, kflowex.AddNode(f, NewNode4))
		assert.NoError(t, kflowex.AddNode(f, NewNode5))
		assert.NoError(t, kflowex.AddNode(f, NewNode6))
		assert.NoError(t, kflowex.AddNode(f, NewNode7))
	}).WithReportInEnglish().WithAsyncTimeout(10).Build()
	assert.NoError(t, err)
	assert.Panics(t, func() {
		engine.Run(context.Background(), state, "Node7")
	})
}
