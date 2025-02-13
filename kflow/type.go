package kflow

import (
	"context"
	"errors"
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

type nodeBox[T any] struct {
	Node          INode
	BoxName       string // box name is the same with node.Name() if there is no condition. But if there is, the name will be <node.Name()>_by_<relying_node_name>。
	Condition     Condition[T]
	ConditionalBy string
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
	Node          INode     // the node object that is for some flow nodes to use
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
	config                 *config                   // The config. User should not modify it at runtime
	chainNodes             []string                  // The nodes specified by Run method in engine.
	failedNodes            map[string]struct{}       // The node reference to all the failed running node. Key is BoxName
	finishedOriginalNodes  map[string]struct{}       // The FinishedNodes uses BoxName as its key. But different BoxName might has the same node, so it's convenient to maintain this field for filtering
	conditionalTargetNodes map[string]struct{}       // The TargetNodes can be modified at runtime. But if conditional node is about to execute, the original node shoud be added to targets at runtime without risk. Key is OriginalName
	startTime              time.Time                 // The start time for this plan
	finishedNodes          map[string]*ExecuteResult // The finished nodes in this execution, it will contain all the nodes executed this time. Key is BoxName.
	runningNodes           map[string]struct{}       // The nodes that are running at the moment. Key is OriginalName
	inParallel             bool                      // If nodes are running in parallel. This field can be used to check if it's safe to write the following fields
	currentNode            string                    // The node name which is running currently.
	targetsSummary         []string                  // The nodes name contains conditionalTargetNodes and targetNodes. It's here for performance purpose.
	targetNodes            map[string]struct{}       // The target nodes in this execution. The current chain node will be added and flow node can add new nodes to it dynamically. Never remove nodes from it manually, it will be cleaned up at the right time. Key is BoxName.
}

// GetTargetNodes gets the current target nodes. There is no chance to read and write in parallel, so parallel checking is not necessary.
func (p *Plan) GetTargetNodes() ([]string, error) {
	if p.inParallel {
		return nil, errors.New(message(p.config.Language, operationNotSupportedInParallel))
	}
	return keys(p.targetNodes), nil
}

// AddTargetNode adds node to the target dynamically.
func (p *Plan) AddTargetNode(node string) error {
	if p.inParallel {
		return errors.New(message(p.config.Language, operationNotSupportedInParallel))
	}
	p.targetNodes[node] = struct{}{}
	return nil
}

// InParallel shows that if the nodes are running in parallel. You can use this method to check if you can add node to targets safely.
func (p *Plan) InParallel() bool {
	return p.inParallel
}

// GetExecuteResult return the executing result
func (p *Plan) GetExecuteResult() ([]*ExecuteResult, error) {
	if p.inParallel {
		return nil, errors.New(message(p.config.Language, operationNotSupportedInParallel))
	}
	return values(p.finishedNodes), nil
}

// GetCurrentNode returns the node name that trigger this process. It's the name passed into the Run method
func (p *Plan) GetCurrentNode() string {
	return p.currentNode
}

// GetStartTime gets the start time of the process.
func (p *Plan) GetStartTime() time.Time {
	return p.startTime
}
