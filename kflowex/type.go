package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type Plan = kflow.Plan

type Condition[T any] kflow.Condition[T]

type IDependence[S any] interface {
	GetName() string
	GetCondition() Condition[S]
	GetDependences() []string
}

type IDescription[S any] interface {
	GetName() string
	GetDependences() []IDependence[S]
}

type INode[S any, D IDescription[S]] interface {
	Description() D
}

type IState any

// IRunMiddleware is the interface for middleware, if error is return, stop the execution of the node immediately. You can also use plan to stop the execution of the whole flow.
type IRunMiddleware[S IState, D IDescription[S]] interface {
	Before(ctx context.Context, desc D, state S, plan *Plan) error
	After(ctx context.Context, desc D, state S, plan *Plan, err error) error
}

type IAddMiddleware[S IState, D IDescription[S]] interface {
	Before(desc D)
	After(desc D, err error)
}
