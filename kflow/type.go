package kflow

import (
	"context"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

// INode specifies the must-have requirement for basic node and flow node. It must have a name and dependence.
type INode interface {
	Name() string
}

type dependenceReturnType[T any] interface {
	string | *Dependence[T]
}

type IDependency[T any, R dependenceReturnType[T]] interface {
	Dependence() []R
}

// IBasicNode specifies the interface for basic node.
type IBasicNode[T any] interface {
	INode
	Run(ctx context.Context, state T) error
}

// IFlowNode specifies the interface for flow node.
type IFlowNode[T any] interface {
	INode
	Run(ctx context.Context, state T, plan *Plan) error
}

type Condition[T any] func(context.Context, T) bool

type IExecutor[T any] interface {
	Execute(context.Context, *container.Dag[string, *nodeBox[T]], T, *Plan) error
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
	BoxName       string    // the name of this NodeBox
	OriginalName  string    // the name of the original node
	Success       bool      // if this execution is successful, which means the run method does not return error
	Err           error     // if the execution is failed, the error message
	IsPanic       bool      // if the execution is panic. The panic will be recovered, user can use this field to detect if there is a panic
	RunInParallel bool      // if the node is running in parallel.
	Skipped       bool      // if the node is skipped
	SkippedReason string    // if the node is skipped, this field indicates the reason
	StartTime     time.Time // when the node is started
	EndTime       time.Time // when the node is ended
	ExecuteBy     string    // the name of specified node which passed with run method which execute the current node
}

type Plan struct {
	Config                *config                   // the config
	ChainNodes            []string                  // the nodes specified by Run method in engine
	CurrentNode           string                    // the node name which is running currently.
	InParallel            bool                      // if nodes are running in parallel. This field can be used to check if it's safe to write the following fields
	TargetNodes           map[string]struct{}       // the target nodes in this execution. The current chain node will be added and flow node can add new nodes to it dynamically. Never remove nodes from it manually, it will be cleaned up at the right time. Key is BoxName.
	RunningNodes          map[string]struct{}       // the nodes that are running at the moment. Key is OriginalName
	FinishedNodes         map[string]*ExecuteResult // the finished nodes in this execution, it will contain all the nodes executed this time. Key is BoxName.
	FailedNodes           map[string]struct{}       // the node reference to all the failed running node. Key is BoxName
	FinishedOriginalNodes map[string]struct{}       // the FinishedNodes uses BoxName as its key. But different BoxName might has the same node, so it's convenient to maintain this field for filtering
	StartTime             time.Time                 // the start time for this plan
}
