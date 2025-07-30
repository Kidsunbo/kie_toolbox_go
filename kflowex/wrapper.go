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

func (n *nodeWrapper[S, D, T]) Dependence() []*kflow.Dependence[S] {
	description := n.node.Description()
	dependencies := make([]*kflow.Dependence[S], 0, len(description.GetDependence()))
	for _, dependence := range description.GetDependence() {
		if dependence.GetCondition() == nil {
			dependencies = append(dependencies, n.StaticDependence(dependence.GetName()))
		} else {
			dependencies = append(dependencies, n.ConditionalDependence(dependence.GetName(), kflow.Condition[S](dependence.GetCondition()), dependence.GetDependence()))
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
