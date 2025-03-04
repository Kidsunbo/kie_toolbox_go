package kflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

type nodeExecutor[T any] struct {
}

func newNodeExecutor[T any]() *nodeExecutor[T] {
	return &nodeExecutor[T]{}
}

func (n *nodeExecutor[T]) Execute(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], state T, plan *Plan) error {

	for _, node := range plan.chainNodes {
		plan.currentNode = node
		if err := n.executeNode(ctx, nodes, state, plan); err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeExecutor[T]) executeNode(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], state T, plan *Plan) error {
	// add currrent node to UnfinishedNodes
	plan.targetNodes[plan.currentNode] = struct{}{}
	defer func() {
		plan.targetNodes = make(map[string]struct{})
		plan.conditionalTargetNodes = make(map[string]struct{})
		plan.targetsSummary = make([]string, 0)
	}()

	tunnel := make(chan *ExecuteResult)
	for {
		var results []*ExecuteResult
		stop, err := n.executeNodesInParallel(ctx, nodes, state, plan, tunnel, &results)
		if err != nil {
			return err
		}
		if stop {
			break
		}
		if len(results) != 0 {
			for _, result := range results {
				n.saveResult(result, plan)
			}
		} else if len(plan.runningNodes) > 0 {
			select {
			case result := <-tunnel:
				n.saveResult(result, plan)
			case <-time.After(plan.config.Timeout * time.Second):
				return errors.New(message(plan.config.Language, nodeTimeoutError))
			}
		}
	}

	return nil
}

func (n *nodeExecutor[T]) saveResult(result *ExecuteResult, plan *Plan) {
	plan.finishedNodes[result.BoxName] = result
	plan.finishedOriginalNodes[result.OriginalName] = struct{}{}
	delete(plan.runningNodes, result.OriginalName)
	if !result.Success {
		plan.failedNodes[result.BoxName] = struct{}{}
	}
}

func (n *nodeExecutor[T]) executeNodesInParallel(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], state T, plan *Plan, tunnel chan *ExecuteResult, out *[]*ExecuteResult) (bool, error) {
	if len(plan.targetNodes) == 0 {
		return true, nil
	}

	// decide the targets to be run this time
	if len(plan.targetsSummary) == 0 {
		targetMap := make(map[string]struct{}, len(plan.targetNodes)+len(plan.conditionalTargetNodes))
		for key := range plan.targetNodes {
			targetMap[key] = struct{}{}
		}
		for key := range plan.conditionalTargetNodes {
			targetMap[key] = struct{}{}
		}
		targets := make([]string, 0, len(targetMap))
		for key := range targetMap {
			targets = append(targets, key)
		}
		plan.targetsSummary = targets
	}

	// also decide which nodes have already done
	alreadyDone := make([]string, 0, len(plan.finishedNodes))
	for key := range plan.finishedNodes {
		alreadyDone = append(alreadyDone, key)
	}

	// get the next batch
	candidates, err := nodes.NextBatch(plan.targetsSummary, container.AlreadyDone[string](alreadyDone), container.Reverse)
	if err != nil {
		return false, err
	}

	// if there is nothing to run and nothing running, stop the execution
	if len(candidates) == 0 && len(plan.runningNodes) == 0 {
		return true, nil
	}

	// filter the nodes that can be executed
	batch := make([]*nodeBox[T], 0, len(candidates))
	for _, node := range candidates {
		canRun, result, err := n.canRun(ctx, nodes, node, state, plan)
		if err != nil {
			return false, err
		}
		if !canRun {
			if result != nil {
				*out = append(*out, result)
			}
		} else {
			batch = append(batch, node)
		}
	}

	// if there is condition node, add the underline node to targets and recalculate the next batch
	length := len(plan.conditionalTargetNodes)
	for _, node := range batch {
		if node.Condition != nil {
			plan.conditionalTargetNodes[node.Node.Name()] = struct{}{}
		}
	}
	if len(plan.conditionalTargetNodes) != length {
		plan.targetsSummary = nil
		return false, nil
	}

	// if there is only one node needs to be run and no other node running at the same time, it will not start a new goroutine to provide thread-safe feature
	if len(batch) == 1 && len(plan.runningNodes) == 0 {
		plan.inParallel.Store(false)
		node := batch[0]
		result := n.runOneNode(ctx, node, state, plan)
		*out = append(*out, result)
	} else if len(batch) > 0 {
		// if there is running nodes at the same time, run engine in async mode.
		plan.inParallel.Store(true)
		n.asyncRunNode(ctx, batch, state, plan, tunnel)
	}

	return false, nil
}

func (n *nodeExecutor[T]) runOneNode(ctx context.Context, node *nodeBox[T], state T, plan *Plan) *ExecuteResult {
	result := &ExecuteResult{
		BoxName:       node.BoxName,
		OriginalName:  node.Node.Name(),
		Node:          node.Node,
		RunInParallel: plan.inParallel.Load(),
		StartTime:     time.Now(),
		ExecuteBy:     plan.currentNode,
	}

	err, isPanic := safeRun(func() error {
		if basicNode, ok := node.Node.(IBasicNode[T]); ok {
			err := basicNode.Run(ctx, state)
			if err != nil {
				return err
			}
			return nil
		} else if flowNode, ok := node.Node.(IFlowNode[T]); ok {
			err := flowNode.Run(ctx, state, plan)
			if err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf(message(plan.config.Language, unsupportedNodeType), node.Node.Name())
	})
	if err != nil {
		result.Success = false
		result.Err = err
		result.IsPanic = isPanic
		result.EndTime = time.Now()
		return result
	}

	result.Success = true
	result.EndTime = time.Now()
	return result
}

func (n *nodeExecutor[T]) canRun(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], node *nodeBox[T], state T, plan *Plan) (bool, *ExecuteResult, error) {
	originalName := node.Node.Name()
	// check if it has already executed by other nodes with the same underline node.
	if contains(plan.finishedOriginalNodes, originalName) {
		result := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  originalName,
			Node:          node.Node,
			RunInParallel: plan.inParallel.Load(),
			IsPanic:       false,
			StartTime:     time.Now(),
			ExecuteBy:     plan.currentNode,
		}
		result.Success = true
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.config.Language, underlineNodeHasExecuted), originalName)
		result.EndTime = time.Now()
		return false, result, nil
	}

	// check if its underline node is executing, if it is, filter it out
	if contains(plan.runningNodes, originalName) {
		return false, nil, nil
	}

	// check if it has failed dependence
	hasFailedDependence, failedNode, err := n.hasFailedDependence(nodes, node, plan)
	if err != nil {
		return false, nil, err
	}
	if hasFailedDependence {
		result := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  originalName,
			Node:          node.Node,
			RunInParallel: plan.inParallel.Load(),
			IsPanic:       false,
			StartTime:     time.Now(),
			ExecuteBy:     plan.currentNode,
		}
		result.Success = false
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.config.Language, nodeHasFailedDependence), node.BoxName, failedNode)
		result.EndTime = time.Now()
		return false, result, nil
	}

	// check if it meet the condition
	if node.Condition != nil {
		if contains(plan.conditionalTargetNodes, originalName) {
			return false, nil, nil
		}

		var pass bool
		err, isPanic := safeRun(func() error {
			pass = node.Condition(ctx, state)
			return nil
		})
		if err != nil {
			result := &ExecuteResult{
				BoxName:       node.BoxName,
				OriginalName:  originalName,
				Node:          node.Node,
				RunInParallel: plan.inParallel.Load(),
				IsPanic:       false,
				StartTime:     time.Now(),
				ExecuteBy:     plan.currentNode,
			}
			result.Success = false
			result.IsPanic = isPanic
			result.Err = err
			result.EndTime = time.Now()
			return false, result, nil
		}
		if !pass {
			result := &ExecuteResult{
				BoxName:       node.BoxName,
				OriginalName:  originalName,
				Node:          node.Node,
				RunInParallel: plan.inParallel.Load(),
				IsPanic:       false,
				StartTime:     time.Now(),
				ExecuteBy:     plan.currentNode,
			}
			result.Success = true
			result.Skipped = true
			result.SkippedReason = fmt.Sprintf(message(plan.config.Language, conditionEvaludateToFalse), node.BoxName)
			result.EndTime = time.Now()
			return false, result, nil
		}
	}

	return true, nil, nil
}

func (n *nodeExecutor[T]) hasFailedDependence(nodes *container.Dag[string, *nodeBox[T]], node *nodeBox[T], plan *Plan) (bool, string, error) {
	for key := range plan.failedNodes {
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

func (n *nodeExecutor[T]) asyncRunNode(ctx context.Context, batch []*nodeBox[T], state T, plan *Plan, tunnel chan *ExecuteResult) {
	for _, node := range batch {
		node := node
		plan.runningNodes[node.Node.Name()] = struct{}{}
		backupResult := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  node.Node.Name(),
			Node:          node.Node,
			RunInParallel: plan.inParallel.Load(),
			StartTime:     time.Now(),
			ExecuteBy:     plan.currentNode,
		}
		go func() {
			defer func() {
				if a := recover(); a != nil {
					backupResult.Success = false
					backupResult.Err = fmt.Errorf("panic: %v", a)
					backupResult.IsPanic = true
					backupResult.EndTime = time.Now()
					tunnel <- backupResult
				}
			}()

			result := n.runOneNode(ctx, node, state, plan)
			tunnel <- result
		}()
	}
}
