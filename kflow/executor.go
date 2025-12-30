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
		if plan.stop.Load() {
			return nil
		}
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
		plan.targetsSummary = nil
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
	if !result.Conditional() {
		plan.finishedOriginalNodes[result.OriginalName] = struct{}{}
		delete(plan.runningNodes, result.OriginalName)
	}
	if !result.Success() {
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
		BoxName:      node.BoxName,
		OriginalName: node.Node.Name(),
		Node:         node.Node,
		StartTime:    time.Now(),
		ExecuteBy:    plan.currentNode,
	}
	if plan.inParallel.Load() {
		result.setRunInParallel()
	}
	// conditional node will not run in runOneNode. So no need to set Conditional state here.

	err, isPanic := safeRun(plan.config, func() error {
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
		result.Err = err
		if isPanic {
			result.setPanic()
		}
		result.EndTime = time.Now()
		return result
	}

	result.setSuccess()
	result.EndTime = time.Now()
	return result
}

func (n *nodeExecutor[T]) canRun(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], node *nodeBox[T], state T, plan *Plan) (bool, *ExecuteResult, error) {
	originalName := node.Node.Name()
	startTime := time.Now()
	// check if it has already executed by other nodes with the same underline node.
	if contains(plan.finishedOriginalNodes, originalName) {
		if node.Condition != nil && contains(plan.failedNodes, originalName) {
			result := &ExecuteResult{
				BoxName:       node.BoxName,
				OriginalName:  originalName,
				Node:          node.Node,
				SkippedReason: fmt.Sprintf(message(plan.config.Language, underlineNodeHasFailed), originalName),
				StartTime:     startTime,
				EndTime:       time.Now(),
				ExecuteBy:     plan.currentNode,
			}
			if plan.inParallel.Load() {
				result.setRunInParallel()
			}
			result.setSkipped()
			result.setConditional()
			return false, result, nil
		}

		result := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  originalName,
			Node:          node.Node,
			SkippedReason: fmt.Sprintf(message(plan.config.Language, underlineNodeHasExecuted), originalName),
			StartTime:     startTime,
			EndTime:       time.Now(),
			ExecuteBy:     plan.currentNode,
		}
		if plan.inParallel.Load() {
			result.setRunInParallel()
		}
		if node.Condition != nil {
			result.setConditional()
		}
		result.setSuccess()
		result.setSkipped()

		return false, result, nil
	}

	// check if its underline node is executing, if it is, filter it out
	if contains(plan.runningNodes, originalName) {
		return false, nil, nil
	}

	// check if it has failed dependency
	hasFailedDependency, failedNode, err := n.hasFailedDependency(nodes, node, plan)
	if err != nil {
		return false, nil, err
	}
	if hasFailedDependency {
		result := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  originalName,
			Node:          node.Node,
			StartTime:     startTime,
			ExecuteBy:     plan.currentNode,
			SkippedReason: fmt.Sprintf(message(plan.config.Language, nodeHasFailedDependency), node.BoxName, failedNode),
			EndTime:       time.Now(),
		}
		if plan.inParallel.Load() {
			result.setRunInParallel()
		}
		if node.Condition != nil {
			result.setConditional()
		}
		result.setSkipped()

		return false, result, nil
	}

	// check if it meet the condition
	if node.Condition != nil {
		if contains(plan.conditionalTargetNodes, originalName) {
			return false, nil, nil
		}

		var pass bool
		err, isPanic := safeRun(plan.config, func() error {
			pass = node.Condition(ctx, state)
			return nil
		})
		if err != nil {
			result := &ExecuteResult{
				BoxName:      node.BoxName,
				OriginalName: originalName,
				Node:         node.Node,
				StartTime:    startTime,
				ExecuteBy:    plan.currentNode,
				EndTime:      time.Now(),
				Err:          err,
			}
			result.setConditional()
			if isPanic {
				result.setPanic()
			}
			if plan.inParallel.Load() {
				result.setRunInParallel()
			}

			return false, result, nil
		}
		if !pass {
			result := &ExecuteResult{
				BoxName:       node.BoxName,
				OriginalName:  originalName,
				Node:          node.Node,
				StartTime:     startTime,
				ExecuteBy:     plan.currentNode,
				SkippedReason: fmt.Sprintf(message(plan.config.Language, conditionEvaludateToFalse), node.BoxName),
				EndTime:       time.Now(),
			}
			result.setConditional()
			result.setSkipped()
			result.setSuccess()
			if plan.inParallel.Load() {
				result.setRunInParallel()
			}

			return false, result, nil
		}
	}

	return true, nil, nil
}

func (n *nodeExecutor[T]) hasFailedDependency(nodes *container.Dag[string, *nodeBox[T]], node *nodeBox[T], plan *Plan) (bool, string, error) {
	for key := range plan.failedNodes {
		canReach, err := nodes.CanReach(node.BoxName, key)
		if err != nil {
			return false, "", err
		}
		if canReach {
			if failedNode := plan.finishedNodes[key]; failedNode.Conditional() && failedNode.Skipped() {
				// if the node is a conditional node, failed and skipped, there are two situations:
				// 	1. the underline node is failed, then we should return the underline node as the failed node.
				// 	2. the dependency of condition is failed, then we should skip the conditional node check to loop until the failed dependency is met.
				if contains(plan.failedNodes, failedNode.OriginalName) {
					return true, failedNode.OriginalName, nil
				}
				continue
			}
			return true, key, nil
		}
	}
	return false, "", nil
}

func (n *nodeExecutor[T]) asyncRunNode(ctx context.Context, batch []*nodeBox[T], state T, plan *Plan, tunnel chan *ExecuteResult) {
	for _, node := range batch {
		node := node
		plan.runningNodes[node.Node.Name()] = struct{}{}
		// This function should not use defer and recover because there is SafeRun can do this stuff. Hand off the decision to users.
		go func() {
			result := n.runOneNode(ctx, node, state, plan)
			tunnel <- result
		}()
	}
}
