package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type flowBuilder[S IState, D IDescription[S]] struct {
	name         string
	nodeRegister func(*Flow[S, D])
	runMW        []IRunMiddleware[S, D]
	addMW        []IAddMiddleware[S, D]
}

func NewFlowBuilder[S IState, D IDescription[S]](name string, nodeRegister func(*Flow[S, D])) *flowBuilder[S, D] {
	return &flowBuilder[S, D]{
		name:         name,
		nodeRegister: nodeRegister,
	}
}

func (f *flowBuilder[S, D]) WithAddMiddleware(mw ...IAddMiddleware[S, D]) *flowBuilder[S, D] {
	f.addMW = append(f.addMW, mw...)
	return f
}

func (f *flowBuilder[S, D]) WithRunMiddleware(mw ...IRunMiddleware[S, D]) *flowBuilder[S, D] {
	f.runMW = append(f.runMW, mw...)
	return f
}

func (f *flowBuilder[S, D]) Build() (*Flow[S, D], error) {
	flow := &Flow[S, D]{
		engine: kflow.NewEngine[S](f.name),
		runMW:  f.runMW,
		addMW:  f.addMW,
	}
	f.nodeRegister(flow)
	err := flow.engine.Prepare()
	if err != nil {
		return nil, err
	}
	return flow, nil
}

type Flow[S IState, D IDescription[S]] struct {
	engine *kflow.Engine[S]
	runMW  []IRunMiddleware[S, D]
	addMW  []IAddMiddleware[S, D]
}

func (f *Flow[S, D]) Run(ctx context.Context, state S, node string, rest ...string) error {
	err := f.engine.Run(ctx, state, node, rest...)
	if err != nil {
		return err
	}
	return nil
}

func AddNode[S IState, D IDescription[S], T INode[S, D]](flow *Flow[S, D], constructor func() T) error {
	node := wrap(constructor, flow.runMW)
	desc := node.node.Description()

	for i := 0; i < len(flow.addMW); i++ {
		flow.addMW[i].Before(desc)
	}

	err := kflow.AddNode(flow.engine, node)

	for i := len(flow.addMW) - 1; i >= 0; i-- {
		flow.addMW[i].After(desc, err)
	}

	return err
}

func Execute[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	return kflow.Execute(ctx, state, plan, targets...)
}