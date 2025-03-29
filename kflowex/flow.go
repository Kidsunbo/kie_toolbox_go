package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type flowBuilder[S IState, D IDescription[S]] struct {
	name            string
	nodeRegister    func(*Flow[S, D])
	runMW           []RunMiddleware[S, D]
	addMW           []IAddMiddleware[S, D]
	safeRun         bool
	reportInEnglish bool
	asyncTimeout    int64
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

func (f *flowBuilder[S, D]) WithRunMiddleware(mw ...RunMiddleware[S, D]) *flowBuilder[S, D] {
	f.runMW = append(f.runMW, mw...)
	return f
}

func (f *flowBuilder[S, D]) WithSafeRun() *flowBuilder[S, D] {
	f.safeRun = true
	return f
}

func (f *flowBuilder[S, D]) WithReportInEnglish() *flowBuilder[S, D] {
	f.reportInEnglish = true
	return f
}

func (f *flowBuilder[S, D]) WithAsyncTimeout(asyncTimeout int64) *flowBuilder[S, D] {
	f.asyncTimeout = asyncTimeout
	return f
}

func (f *flowBuilder[S, D]) Build() (*Flow[S, D], error) {
	options := make([]any, 0)
	if f.safeRun {
		options = append(options, kflow.SafeRun)
	}
	if f.reportInEnglish {
		options = append(options, kflow.ReportInEnglish)
	}
	if f.asyncTimeout > 0 {
		options = append(options, kflow.AsyncTimeout(f.asyncTimeout))
	}

	flow := &Flow[S, D]{
		engine: kflow.NewEngine[S](f.name, options...),
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
	runMW  []RunMiddleware[S, D]
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

func ExecuteSequentially[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	return kflow.ExecuteInSequence(ctx, state, plan, targets...)
}

func ExecuteParallel[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	return kflow.ExecuteInParallel(ctx, state, plan, targets...)
}

func RemoveResult[T any](plan *Plan, target string) error {
	return kflow.RemoveResult[T](plan, target)
}
