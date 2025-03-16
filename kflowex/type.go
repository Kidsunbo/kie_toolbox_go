package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type Plan = kflow.Plan

type Condition[T any] kflow.Condition[T]

type IDependence[S any] interface {
	GetName() string
	GetFunction() Condition[S]
	GetDependences() []string
}

type IDescription[S any] interface {
	GetName() string
	GetDependence() []IDependence[S]
}

type INode[S any, D IDescription[S]] interface {
	Description() D
}

type IState any

type IRunMiddleware[S IState, D IDescription[S]] interface {
	Before(ctx context.Context, state S, desc D)
	After(ctx context.Context, state S, desc D, err error)
}

type IAddMiddleware[S IState, D IDescription[S]] interface {
	Before(desc D)
	After(desc D, err error)
}
