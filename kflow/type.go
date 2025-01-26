package kflow

import (
	"context"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

type INode[T any, R dependenceReturnType[T]] interface {
	Name() string
	Dependence() []R
}

type Condition[T any] func(context.Context, T) bool

type IExecutor[T any] interface {
	Execute(context.Context, *container.Dag[string, NodeBox[T]], T, *Plan) error
}

type IBefore[T any] interface {
	Before(context.Context, T)
}

type IAfter[T any] interface {
	After(context.Context, T)
}

type dependenceReturnType[T any] interface {
	string | dependence[T] | any
}

type IDependency[T any, R dependenceReturnType[T]] interface {
	Dependence() []R
}

type dependence[T any] struct {
	DependenceName      string
	Condition           Condition[T]
	ConditionDependence []string
}

type Plan struct {
	Nodes     []string
	StartTime time.Time
}
