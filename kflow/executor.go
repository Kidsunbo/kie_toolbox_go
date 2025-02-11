package kflow

import (
	"context"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
	"golang.org/x/sync/errgroup"
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
	plan.UnfinishedNodes[plan.CurrentNode] = struct{}{}

	for len(plan.UnfinishedNodes) != 0 {
		err := n.executeNodesInParallel(ctx, nodes, state, plan)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeExecutor[T]) executeNodesInParallel(ctx context.Context, nodes *container.Dag[string, *NodeBox[T]], state T, plan *Plan) error {
	target := make([]string, 0, len(plan.UnfinishedNodes))
	for key := range plan.UnfinishedNodes {
		target = append(target, key)
	}

	alreadyDone := make([]string, 0, len(plan.FinishedNodes))
	for key := range plan.FinishedNodes {
		alreadyDone = append(alreadyDone, key)
	}

	batch, err := nodes.NextBatch(target, container.AlreadyDone[string](alreadyDone), container.Reverse)
	if err != nil {
		return err
	}

	if len(batch) == 0 {
		return nil
	} else if len(batch) == 1 {
		node := batch[0]
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
		if !contains(plan.FinishedOriginalNodes, originalName) {
			if err, isPanic := safeRun(func() error { return n.runOneNode(ctx, node, state, plan, result) }); err != nil {
				result.Success = false
				result.Err = err
				result.IsPanic = isPanic
			}
		}else {
			result.Skipped = true

		}
		plan.FinishedOriginalNodes[originalName] = struct{}{}
	} else {
		plan.InParallel = true
		defer func() {
			plan.InParallel = false
		}()
		eg := errgroup.Group{}

		if err := eg.Wait(); err != nil {
			return err
		}
	}

	return nil
}

func (n *nodeExecutor[T]) runOneNode(ctx context.Context, node *NodeBox[T], state T, plan *Plan, result *ExecuteResult) error {

	return nil
}
