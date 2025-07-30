package kflowex

import (
	"context"
	"errors"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type nodeWrapper[S IState, D IDescription[S], T INode[S, D]] struct {
	kflow.Node[S]
	handler     RunEndpoint[S, D]
	node        T
	constructor func() T
}

func (n *nodeWrapper[S, D, T]) Name() string {
	return n.node.Description().GetName()
}

func (n *nodeWrapper[S, D, T]) Dependencies() []*kflow.Dependency[S] {
	description := n.node.Description()
	dependencies := make([]*kflow.Dependency[S], 0, len(description.GetDependencies()))
	for _, dependency := range description.GetDependencies() {
		if dependency.GetCondition() == nil {
			dependencies = append(dependencies, n.StaticDependency(dependency.GetName()))
		} else {
			dependencies = append(dependencies, n.ConditionalDependency(dependency.GetName(), kflow.Condition[S](dependency.GetCondition()), dependency.GetDependencies()))
		}
	}
	return dependencies
}

func (n *nodeWrapper[S, D, T]) Run(ctx context.Context, state S, plan *Plan) error {
	return n.handler(ctx, n.node.Description(), state, plan)
}

func (n *nodeWrapper[S, D, T]) terminalHandler(ctx context.Context, desc D, state S, plan *Plan) error {
	_ = desc
	v := n.constructor()
	if value, ok := any(v).(kflow.IBasicNode[S]); ok {
		return value.Run(ctx, state)
	} else if value, ok := any(v).(kflow.IFlowNode[S]); ok {
		return value.Run(ctx, state, plan)
	} else {
		return errors.New("not supported")
	}
}

func wrap[S any, D IDescription[S], T INode[S, D]](constructor func() T, mw []RunMiddleware[S, D]) *nodeWrapper[S, D, T] {
	w := &nodeWrapper[S, D, T]{
		node:        constructor(),
		constructor: constructor,
	}
	w.handler = chain[S, D, T](w.terminalHandler, mw)
	return w
}

func chain[S any, D IDescription[S], T INode[S, D]](handler RunEndpoint[S, D], mw []RunMiddleware[S, D]) RunEndpoint[S, D] {
	for i := len(mw) - 1; i >= 0; i-- {
		handler = mw[i](handler)
	}
	return handler
}
