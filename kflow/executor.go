package kflow

import (
	"context"
	"fmt"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

type nodeExecutor[T any] struct {
}

func newNodeExecutor[T any]() *nodeExecutor[T] {
	return &nodeExecutor[T]{}
}

func (n *nodeExecutor[T]) Execute(ctx context.Context, nodes *container.Dag[string, *NodeBox[T]], state T, plan *Plan) error {

	for _, node := range plan.ChainNodes {
		plan.CurrentNode = node
		if err := n.executeNode(ctx, nodes, state, plan); err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeExecutor[T]) executeNode(ctx context.Context, nodes *container.Dag[string, *NodeBox[T]], state T, plan *Plan) error {
	// add currrent node to UnfinishedNodes
	plan.TargetNodes[plan.CurrentNode] = struct{}{}
	defer func() {
		plan.TargetNodes = make(map[string]struct{})
	}()

	tunnel := make(chan *ExecuteResult)
	for {
		var result *ExecuteResult
		stop, err := n.executeNodesInParallel(ctx, nodes, state, plan, tunnel, &result)
		if err != nil {
			return err
		}
		if stop {
			break
		}
		if result != nil {
			// write the result
		} else {
			// wait for tunnel
		}
	}

	return nil
}

func (n *nodeExecutor[T]) executeNodesInParallel(ctx context.Context, nodes *container.Dag[string, *NodeBox[T]], state T, plan *Plan, tunnel chan *ExecuteResult, out **ExecuteResult) (bool, error) {
	if len(plan.TargetNodes) == 0 {
		return true, nil
	}

	target := make([]string, 0, len(plan.TargetNodes))
	for key := range plan.TargetNodes {
		target = append(target, key)
	}

	alreadyDone := make([]string, 0, len(plan.FinishedNodes))
	for key := range plan.FinishedNodes {
		alreadyDone = append(alreadyDone, key)
	}

	candidates, err := nodes.NextBatch(target, container.AlreadyDone[string](alreadyDone), container.Reverse)
	if err != nil {
		return false, err
	}

	batch := make([]*NodeBox[T], 0, len(candidates))
	for _, node := range candidates {
		
	}

	if len(batch) == 0 && len(plan.RunningNodes) == 0 {
		return true, nil
	}

	// if there is only one node needs to be run, it will not start a new goroutine to provide thread-safe feature
	if len(batch) == 1 && len(plan.RunningNodes) == 0 {
		plan.InParallel = false
		node := batch[0]
		result, err := n.runOneNode(ctx, node, state, plan)
		if err != nil {
			return false, err
		}
		*out = result
	} else {
		plan.InParallel = true

	}

	return false, nil
}

func (n *nodeExecutor[T]) runOneNode(ctx context.Context, node *NodeBox[T], state T, plan *Plan) (*ExecuteResult, error) {

	originalName := node.Node.Name()
	result := &ExecuteResult{
		BoxName:       node.BoxName,
		OriginalName:  originalName,
		Success:       true,
		RunInParallel: plan.InParallel,
		StartTime:     time.Now(),
		TimeCost:      0,
		ExecuteBy:     plan.CurrentNode,
	}

	// if the dependence is failed, skip execution
	hasFailedDependence, failedNode, err := n.hasFailedDependence(nodes, node, plan)
	if err != nil {
		return false, err
	} else if hasFailedDependence {
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, nodeHasFailedDependence), node.BoxName, failedNode)
	}
	if !result.Skipped {
		if !contains(plan.FinishedOriginalNodes, originalName) {
			if err, isPanic := safeRun(func() error { return n.runOneNode(ctx, node, state, plan, result) }); err != nil {
				result.Success = false
				result.Err = err
				result.IsPanic = isPanic
			}
		} else {

		}
	}

	return nil, nil
}

func (n *nodeExecutor[T]) canRun(ctx context.Context, nodes *container.Dag[string, *NodeBox[T]], node *NodeBox[T], state T, plan *Plan) (bool, *ExecuteResult, error) {
	originalName := node.Node.Name()
	result := &ExecuteResult{
		BoxName:       node.BoxName,
		OriginalName:  originalName,
		RunInParallel: plan.InParallel,
		IsPanic:       false,
		StartTime:     time.Now(),
		TimeCost:      0,
		ExecuteBy:     plan.CurrentNode,
	}
	// first check if it's already done by other nodes with the same underline node.
	if contains(plan.FinishedOriginalNodes, originalName) {
		result.Success = true
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, underlineNodeHasExecuted), originalName)
		return false, result, nil
	}

	// second check if it has failed dependence
	hasFailedDependence, failedNode, err := n.hasFailedDependence(nodes, node, plan)
	if err != nil {
		return false, nil, err
	}
	if hasFailedDependence {
		result.Success = false
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, nodeHasFailedDependence), node.BoxName, failedNode)
		return false, result, nil
	}

	// third check if it meet the condition
	if node.Conditions != nil {
		var pass bool
		err, isPanic := safeRun(func() error {
			pass = node.Conditions(ctx, state)
			return nil
		})
		if err != nil {
			result.IsPanic = isPanic
			result.Err = err
			result.Success = false
			return false, result, nil
		}
		if !pass {
			result.Success = true
			result.Skipped = false
			result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, conditionEvaludateToFalse), node.BoxName)
			return false, result, nil
		}
	}

	return true, nil, nil
}

func (n *nodeExecutor[T]) hasFailedDependence(nodes *container.Dag[string, *NodeBox[T]], node *NodeBox[T], plan *Plan) (bool, string, error) {
	for key := range plan.FailedNodes {
		canReach, err := nodes.CanReach(node.BoxName, key)
		if err != nil {
			return false, "", err
		}
		if canReach {
			return true, key, nil
		}
	}
	return false, "", nil
}
