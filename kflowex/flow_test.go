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

type Dependence[S any] struct {
	Name        string
	Condition    kflowex.Condition[S]
	Dependences []string
}

func (d Dependence[S]) GetName() string {
	return d.Name
}

func (d Dependence[S]) GetCondition() kflowex.Condition[S] {
	return d.Condition
}

func (d Dependence[S]) GetDependences() []string {
	return d.Dependences
}

type Description[S any] struct {
	Name       string
	Dependence []string
}

func (d Description[S]) GetName() string {
	return d.Name
}

func (d Description[S]) GetDependences() []kflowex.IDependence[*State] {
	deps := make([]kflowex.IDependence[*State], 0, len(d.Dependence))
	for _, dep := range d.Dependence {
		deps = append(deps, Dependence[*State]{
			Name: dep,
		})
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
		Name:       "Node3",
		Dependence: []string{"Node1", "Node2"},
	}
}

func (n *Node3) Run(ctx context.Context, state *State, plan *kflowex.Plan) error {
	state.AddStep("Node3")
	return nil
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

type RunMW[S kflowex.IState, D kflowex.IDescription[S]] struct {
	name string
}

func (a *RunMW[S, D]) Before(ctx context.Context, state S, desc D) {
	any(state).(*State).AddStep(fmt.Sprintf("b_%v", a.name))
}

func (a *RunMW[S, D]) After(ctx context.Context, state S, desc D, err error) {
	any(state).(*State).AddStep(fmt.Sprintf("a_%v", a.name))
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

func TestFlowNodeMiddleware(t *testing.T) {
	state := &State{
	}

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

	runMW1 := &RunMW[*State, Description[*State]]{
		name: "runMW1",
	}
	runMW2 := &RunMW[*State, Description[*State]]{
		name: "runMW2",
	}
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
