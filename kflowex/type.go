package kflowex

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type Plan = kflow.Plan

type Condition[T any] kflow.Condition[T]

type Dependence[S any] struct {
	Name        string
	Function    Condition[S]
	Dependences []string
}

func (d Dependence[S]) GetName() string {
	return d.Name
}

func (d Dependence[S]) GetFunction() Condition[S] {
	return d.Function
}

func (d Dependence[S]) GetDependences() []string {
	return d.Dependences
}

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
