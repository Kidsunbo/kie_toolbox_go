package kflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Kidsunbo/kie_toolbox_go/container"
)

type nodeEngine[T any] struct {
	config   *config
	nodes    *container.Dag[string, NodeBox[T]]
	executor IExecutor[T]
}

func NewEngine[T any](name string, params ...any) *nodeEngine[T] {
	config := &config{
		Static:   false,
		Language: chinese,
	}
	for _, param := range params {
		if v, ok := param.(flag); ok {
			if v == StaticNode {
				config.Static = true
			} else if v == ReportInChinese {
				config.Language = chinese
			} else if v == ReportInEnglish {
				config.Language = english
			}
		}
	}

	var flags []any
	if config.Static {
		flags = append(flags, container.DisableThreadSafe)
	}
	if config.Language == english {
		flags = append(flags, container.EnglishError)
	}

	return &nodeEngine[T]{
		config:   config,
		nodes:    container.NewDag[string, NodeBox[T]](name, flags...),
		executor: newNodeExecutor[T](),
	}
}

func (n *nodeEngine[T]) Prepare() error {
	if n.nodes.IsChecked() {
		return nil
	}

	pass, cycles := n.nodes.CheckCycle()
	if !pass {
		return fmt.Errorf(message(n.config.Language, cycleDetectedError), cycles)
	}

	return nil
}

func (n *nodeEngine[T]) Run(ctx context.Context, note T) error {
	return n.RunNodes(ctx, note, nil)
}

func (n *nodeEngine[T]) RunNode(ctx context.Context, note T, node string) error {
	return n.RunNodes(ctx, note, []string{node})
}

func (n *nodeEngine[T]) RunNodes(ctx context.Context, note T, nodes []string) error {
	if err := n.check(); err != nil {
		return err
	}

	if err := n.execute(ctx, note, nodes); err != nil {
		return err
	}

	return nil
}

func (n *nodeEngine[T]) check() error {
	if !n.nodes.IsChecked() {
		return errors.New(message(n.config.Language, notPreparedError))
	}

	return nil
}

func (n *nodeEngine[T]) execute(ctx context.Context, note T, nodes []string) error {
	return n.executor.Execute(ctx, n.nodes, note, n.makePlan(nodes))
}

func (n *nodeEngine[T]) makePlan(nodes []string) *Plan {
	return &Plan{
		Nodes:     nodes,
		StartTime: time.Now(),
	}
}

func (n *nodeEngine[T]) MountExecutor(executor IExecutor[T]) {
	n.executor = executor
}
