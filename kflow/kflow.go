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

// Execute will run the targets in the same executor, state and plan.
func Execute[T any](ctx context.Context, state T, plan *Plan, targets ...string) error {
	executor, ok := plan.executor.(IExecutor[T])
	if !ok {
		return fmt.Errorf(message(plan.config.Language, typeAssertFailed), plan.executor)
	}
	nodes, ok := plan.nodes.(*container.Dag[string, *nodeBox[T]])
	if !ok {
		return fmt.Errorf(message(plan.config.Language, typeAssertFailed), plan.nodes)
	}
	if plan.inParallel.Load() {
		return errors.New((message(plan.config.Language, operationNotSupportedInParallel)))
	}

	p := plan.copy()
	p.targetNodes = make(map[string]struct{})
	p.conditionalTargetNodes = make(map[string]struct{})
	p.targetsSummary = make([]string, 0)
	p.runningNodes = make(map[string]struct{})
	p.currentNode = ""
	p.chainNodes = targets

	return executor.Execute(ctx, nodes, state, p)
}
