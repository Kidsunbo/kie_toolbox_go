package kflow

import (
	"context"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

type nodeExecutor[T any] struct {
}

func newNodeExecutor[T any]() *nodeExecutor[T] {
	return &nodeExecutor[T]{}
}

func (n *nodeExecutor[T]) Execute(ctx context.Context, nodes *container.Dag[string, NodeBox[T]], note T, plan *Plan) error {

	return nil
}
