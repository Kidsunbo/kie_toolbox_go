package kflowex

import (
	"context"
	"errors"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type nodeWrapper[S IState, D IDescription[S], T INode[S, D]] struct {
	kflow.Node[S]
	mw          []IRunMiddleware[S, D]
	node        T
	constructor func() T
}

func (n *nodeWrapper[S, D, T]) Name() string {
	return n.node.Description().GetName()
}

func (n *nodeWrapper[S, D, T]) Dependence() []*kflow.Dependence[S] {
	description := n.node.Description()
	dependences := make([]*kflow.Dependence[S], 0, len(description.GetDependences()))
	for _, dependence := range description.GetDependences() {
		if dependence.GetCondition() == nil {
			dependences = append(dependences, n.StaticDependence(dependence.GetName()))
		} else {
			dependences = append(dependences, n.ConditionalDependence(dependence.GetName(), kflow.Condition[S](dependence.GetCondition()), dependence.GetDependences()))
		}
	}
	return dependences
}

func (n *nodeWrapper[S, D, T]) Run(ctx context.Context, state S, plan *Plan) error {
	v := n.constructor()
	desc := v.Description()

	for i := 0; i < len(n.mw); i++ {
		if err := n.mw[i].Before(ctx, desc, state, plan); err != nil {
			return err
		}
	}

	var err error
	if value, ok := any(v).(kflow.IBasicNode[S]); ok {
		err = value.Run(ctx, state)
	} else if value, ok := any(v).(kflow.IFlowNode[S]); ok {
		err = value.Run(ctx, state, plan)
	} else {
		return errors.New("not supported")
	}

	for i := len(n.mw) - 1; i >= 0; i-- {
		if err := n.mw[i].After(ctx, desc, state, plan, err); err != nil {
			return err
		}
	}
	return err
}

func wrap[S any, D IDescription[S], T INode[S, D]](constructor func() T, mw []IRunMiddleware[S, D]) *nodeWrapper[S, D, T] {
	node := constructor()
	return &nodeWrapper[S, D, T]{
		mw:          mw,
		node:        node,
		constructor: constructor,
	}
}
