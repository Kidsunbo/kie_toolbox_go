package kflow

import (
	"context"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

// INode specifies the must-have requirement for basic node and flow node. It must have a name and dependence.
type INode[T any] interface {
	Name() string
}

type dependenceReturnType[T any] interface {
	string | Dependence[T]
}

type IDependency[T any, R dependenceReturnType[T]] interface {
	Dependence() []R
}

// IBasicNode specifies the interface for basic node.
type IBasicNode[T any] interface {
	INode[T]
	Run(ctx context.Context, state T) error
}

// IFlowNode specifies the interface for flow node.
type IFlowNode[T any] interface {
	INode[T]
	Run(ctx context.Context, state T, plan *Plan) error
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

type Dependence[T any] struct {
	DependenceName      string
	Condition           Condition[T]
	ConditionDependence []string
}

type ExecuteResult struct {
	NodeName          string    // the name of this node
	Success           bool      // if this execution is successful, which means the run method does not return error
	Err               error     // if the execution is failed, the error message
	IsPanic           bool      // if the execution is panic. The panic will be recovered, user can use this field to detect if there is a panic
	RunInParallel     bool      // if the node is running in parallel.
	Skipped           bool      // if the node is skipped
	SkippedReasonCode int64     // if the node is skipped, the reason code indicates the reason
	StartTime         time.Time // when the node is started
	TimeCost          int64     // the time cost for this node
	ExecuteBy         string    // the name of specified node which passed with run method which execute the current node
}

type Plan struct {
	Config         *config                   // the config
	SpecifiedNodes []string                  // the nodes specified by Run method in engine, when nil is passed in, all the nodes will be added into this field
	CurrentNode    string                    // the node name which is running currently.
	TargetNodes    map[string]struct{}       // the target nodes in this execution, normally, it's the same with SpecifiedNodes, but it can also be modified in flow nodes.
	FinishNodes    map[string]*ExecuteResult // the finished nodes in this execution, it will contain all the nodes executed this time.
	FailedNodes    map[string]struct{}       // the node reference to all the failed running node.
	StartTime      time.Time                 // the start time for this plan
}
