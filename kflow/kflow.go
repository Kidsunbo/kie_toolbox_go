package kflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

// AddNode adss the node to the engine before the engine is running.
func AddNode[T any](engine *Engine[T], node INode) error {
	if !ofType[IBasicNode[T]](node) && !ofType[IFlowNode[T]](node) {
		return fmt.Errorf(message(engine.config.Language, unsupportedNodeType), node.Name())
	}

	return engine.nodes.AddVertex(node.Name(), &nodeBox[T]{
		Node:          node,
		BoxName:       node.Name(),
		Condition:     nil,
		ConditionalBy: "",
	})
}

// ExecuteInSequence will run the targets in the same executor, state and plan, but in a sequential way.
func ExecuteInSequence[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	executor := plan.executor.(IExecutor[T])                  // the type must be this type, panic if it's not
	nodes := plan.nodes.(*container.Dag[string, *nodeBox[T]]) // the type must be this type, panic if it's not
	if plan.inParallel.Load() {
		return errors.New((message(plan.config.Language, operationNotSupportedInParallel)))
	}
	if len(targets) == 0 {
		return errors.New(message(plan.config.Language, noTargetToRun))
	}

	p := plan.copy()
	p.targetNodes = make(map[string]struct{})
	p.conditionalTargetNodes = make(map[string]struct{}, 4)
	p.targetsSummary = nil
	p.runningNodes = make(map[string]struct{}, 4)
	p.chainNodes = targets

	currentNode := p.GetCurrentNode()
	beforeExecuteFinishedNodes := make(map[string]*ExecuteResult, len(p.finishedNodes))
	for k, v := range p.finishedNodes {
		beforeExecuteFinishedNodes[k] = v
	}

	err := executor.Execute(ctx, nodes, state, p)
	for k, node := range p.finishedNodes {
		if _, exist := beforeExecuteFinishedNodes[k]; !exist {
			node.ExecuteBy = currentNode
		}
	}
	return err
}

// ExecuteInParallel will run the targets in the same executor, state and plan, but in parallel.
func ExecuteInParallel[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	executor := plan.executor.(IExecutor[T])                  // the type must be this type, panic if it's not
	nodes := plan.nodes.(*container.Dag[string, *nodeBox[T]]) // the type must be this type, panic if it's not
	if plan.inParallel.Load() {
		return errors.New((message(plan.config.Language, operationNotSupportedInParallel)))
	}
	if len(targets) == 0 {
		return errors.New(message(plan.config.Language, noTargetToRun))
	}

	p := plan.copy()
	p.targetNodes = make(map[string]struct{})
	p.conditionalTargetNodes = make(map[string]struct{}, 4)
	p.targetsSummary = nil
	p.runningNodes = make(map[string]struct{}, 4)
	p.chainNodes = []string{targets[0]}
	for _, target := range targets[1:] {
		p.targetNodes[target] = struct{}{}
	}

	currentNode := p.GetCurrentNode()
	beforeExecuteFinishedNodes := make(map[string]*ExecuteResult, len(p.finishedNodes))
	for k, v := range p.finishedNodes {
		beforeExecuteFinishedNodes[k] = v
	}
	err := executor.Execute(ctx, nodes, state, p)
	for k, node := range p.finishedNodes {
		if _, exist := beforeExecuteFinishedNodes[k]; !exist {
			node.ExecuteBy = currentNode
		}
	}
	return err
}

// RemoveResult will remove the result of the node and all the nodes that depend on it. User can use this function to re-run the node and its descendats.
//
// Caution: this function is pretty costly and the previous result will lost, so be careful to use it.
func RemoveResult[T any](plan *Plan, target string) error {
	if plan.inParallel.Load() {
		return errors.New((message(plan.config.Language, operationNotSupportedInParallel)))
	}

	if node, exist := plan.finishedNodes[target]; !exist {
		return fmt.Errorf(message(plan.config.Language, nodeNotExist), target)
	} else if node.BoxName != node.OriginalName {
		return fmt.Errorf(message(plan.config.Language, unsupportedNodeType), target)
	}
	nodes, ok := plan.nodes.(*container.Dag[string, *nodeBox[T]])
	if !ok {
		return errors.New(message(plan.config.Language, typeAssertFailed))
	}
	for key, result := range plan.finishedNodes {
		if ok, err := nodes.CanReach(key, target); err != nil {
			return err
		} else if ok {
			delete(plan.finishedNodes, key)
			delete(plan.finishedOriginalNodes, key)
			if !result.Success {
				delete(plan.failedNodes, key)
			}
		}
	}
	return nil
}
