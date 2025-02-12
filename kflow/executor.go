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

	for _, node := range plan.ChainNodes {
		plan.CurrentNode = node
		if err := n.executeNode(ctx, nodes, state, plan); err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeExecutor[T]) executeNode(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], state T, plan *Plan) error {
	// add currrent node to UnfinishedNodes
	plan.TargetNodes[plan.CurrentNode] = struct{}{}
	defer func() {
		plan.TargetNodes = make(map[string]struct{})
		plan.ConditionalTargetNodes = make(map[string]struct{})
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
		} else if plan.InParallel {
			select {
			case result := <-tunnel:
				n.saveResult(result, plan)
			case <-time.After(30 * time.Second):
				return errors.New(message(plan.Config.Language, nodeTimeoutError))
			}
		}
	}

	return nil
}

func (n *nodeExecutor[T]) saveResult(result *ExecuteResult, plan *Plan) {
	plan.FinishedNodes[result.BoxName] = result
	plan.FinishedOriginalNodes[result.OriginalName] = struct{}{}
	delete(plan.RunningNodes, result.OriginalName)
	if !result.Success {
		plan.FailedNodes[result.BoxName] = struct{}{}
	}
}

func (n *nodeExecutor[T]) executeNodesInParallel(ctx context.Context, nodes *container.Dag[string, *nodeBox[T]], state T, plan *Plan, tunnel chan *ExecuteResult, out *[]*ExecuteResult) (bool, error) {
	if len(plan.TargetNodes) == 0 {
		return true, nil
	}

	// decide the targets to be run this time
	targetMap := make(map[string]struct{}, len(plan.TargetNodes)+len(plan.ConditionalTargetNodes))
	for key := range plan.TargetNodes {
		targetMap[key] = struct{}{}
	}
	for key := range plan.ConditionalTargetNodes {
		targetMap[key] = struct{}{}
	}
	targets := make([]string, 0, len(targetMap))
	for key := range targetMap {
		targets = append(targets, key)
	}

	// also decide which nodes have already done
	alreadyDone := make([]string, 0, len(plan.FinishedNodes))
	for key := range plan.FinishedNodes {
		alreadyDone = append(alreadyDone, key)
	}

	// get the next batch
	candidates, err := nodes.NextBatch(targets, container.AlreadyDone[string](alreadyDone), container.Reverse)
	if err != nil {
		return false, err
	}

	fmt.Println(plan.ConditionalTargetNodes, plan.TargetNodes)
	for _, can := range candidates {
		fmt.Print(can.BoxName, " ")
	}
	fmt.Println()

	// if there is nothing to run and nothing running, stop the execution
	if len(candidates) == 0 && len(plan.RunningNodes) == 0 {
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
	length := len(plan.ConditionalTargetNodes)
	for _, node := range batch {
		if node.Condition != nil {
			plan.ConditionalTargetNodes[node.Node.Name()] = struct{}{}
		}
	}
	if len(plan.ConditionalTargetNodes) != length {
		return false, nil
	}

	// if there is only one node needs to be run and no other node running at the same time, it will not start a new goroutine to provide thread-safe feature
	if len(batch) == 1 && len(plan.RunningNodes) == 0 {
		plan.InParallel = false
		node := batch[0]
		result := n.runOneNode(ctx, node, state, plan)
		*out = append(*out, result)
	} else if len(batch) > 0 {
		// if there is running nodes at the same time, run engine in async mode.
		plan.InParallel = true
		n.asyncRunNode(ctx, batch, state, plan, tunnel)
	}

	return false, nil
}

func (n *nodeExecutor[T]) runOneNode(ctx context.Context, node *nodeBox[T], state T, plan *Plan) *ExecuteResult {
	result := &ExecuteResult{
		BoxName:       node.BoxName,
		OriginalName:  node.Node.Name(),
		Node:          node.Node,
		RunInParallel: plan.InParallel,
		StartTime:     time.Now(),
		ExecuteBy:     plan.CurrentNode,
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
		return fmt.Errorf(message(plan.Config.Language, unsupportedNodeType), node.Node.Name())
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
	result := &ExecuteResult{
		BoxName:       node.BoxName,
		OriginalName:  originalName,
		Node:          node.Node,
		RunInParallel: plan.InParallel,
		IsPanic:       false,
		StartTime:     time.Now(),
		ExecuteBy:     plan.CurrentNode,
	}
	// check if it has already executed by other nodes with the same underline node.
	if contains(plan.FinishedOriginalNodes, originalName) {
		result.Success = true
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, underlineNodeHasExecuted), originalName)
		result.EndTime = time.Now()
		return false, result, nil
	}

	// check if its underline node is executing or about to execute, if it is, filter it out
	if contains(plan.RunningNodes, originalName) || contains(plan.ConditionalTargetNodes, originalName) {
		return false, nil, nil
	}

	// check if it has failed dependence
	hasFailedDependence, failedNode, err := n.hasFailedDependence(nodes, node, plan)
	if err != nil {
		return false, nil, err
	}
	if hasFailedDependence {
		result.Success = false
		result.Skipped = true
		result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, nodeHasFailedDependence), node.BoxName, failedNode)
		result.EndTime = time.Now()
		return false, result, nil
	}

	// check if it meet the condition
	if node.Condition != nil {
		var pass bool
		err, isPanic := safeRun(func() error {
			pass = node.Condition(ctx, state)
			return nil
		})
		if err != nil {
			result.Success = false
			result.IsPanic = isPanic
			result.Err = err
			result.EndTime = time.Now()
			return false, result, nil
		}
		if !pass {
			result.Success = true
			result.Skipped = true
			result.SkippedReason = fmt.Sprintf(message(plan.Config.Language, conditionEvaludateToFalse), node.BoxName)
			result.EndTime = time.Now()
			return false, result, nil
		}
	}

	return true, nil, nil
}

func (n *nodeExecutor[T]) hasFailedDependence(nodes *container.Dag[string, *nodeBox[T]], node *nodeBox[T], plan *Plan) (bool, string, error) {
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

func (n *nodeExecutor[T]) asyncRunNode(ctx context.Context, batch []*nodeBox[T], state T, plan *Plan, tunnel chan *ExecuteResult) {
	for _, node := range batch {
		node := node
		plan.RunningNodes[node.Node.Name()] = struct{}{}
		backupResult := &ExecuteResult{
			BoxName:       node.BoxName,
			OriginalName:  node.Node.Name(),
			Node:          node.Node,
			RunInParallel: plan.InParallel,
			StartTime:     time.Now(),
			ExecuteBy:     plan.CurrentNode,
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
