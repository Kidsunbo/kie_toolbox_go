package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type Plan = kflow.Plan

type ExecuteResult = kflow.ExecuteResult

type Condition[T any] kflow.Condition[T]

type IDependence[S any] interface {
	GetName() string
	GetCondition() Condition[S]
	GetDependencies() []string
}

type IDescription[S any] interface {
	GetName() string
	GetDependencies() []IDependence[S]
}

type INode[S any, D IDescription[S]] interface {
	Description() D
}

type IState any

type RunEndpoint[S IState, D IDescription[S]] func(ctx context.Context, desc D, state S, plan *Plan) error

type RunMiddleware[S IState, D IDescription[S]] func(next RunEndpoint[S, D]) RunEndpoint[S, D]

type IAddMiddleware[S IState, D IDescription[S]] interface {
	Before(desc D)
	After(desc D, err error)
}
