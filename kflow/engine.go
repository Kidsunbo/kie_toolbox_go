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
	nodes    *container.Dag[string, *NodeBox[T]]
	executor IExecutor[T]
}

// NewEngine creates a node engine that runs all the node.
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
		nodes:    container.NewDag[string, *NodeBox[T]](name, flags...),
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

// Run starts the engine and accept the state object. At least one node name needs to be passed in. If multiple nodes has been passed in, it will chain all them together.
func (n *nodeEngine[T]) Run(ctx context.Context, state T, node string, rest ...string) error {
	nodes := append([]string{node}, rest...)
	if err := n.check(nodes); err != nil {
		return err
	}

	if err := n.execute(ctx, state, nodes); err != nil {
		return err
	}

	return nil
}

func (n *nodeEngine[T]) check(nodes []string) error {
	if !n.nodes.IsChecked() {
		return errors.New(message(n.config.Language, notPreparedError))
	}

	for _, node := range nodes {
		if !n.nodes.HasVertex(node) {
			return fmt.Errorf(message(n.config.Language, nodeNotExist), node)
		}
	}

	return nil
}

func (n *nodeEngine[T]) execute(ctx context.Context, state T, nodes []string) error {
	return n.executor.Execute(ctx, n.nodes, state, n.makePlan(nodes))
}

func (n *nodeEngine[T]) makePlan(nodes []string) *Plan {
	return &Plan{
		Config:                n.config,
		ChainNodes:            nodes,
		CurrentNode:           "",
		InParallel:            false,
		UnfinishedNodes:       map[string]struct{}{},
		FinishedNodes:         map[string]*ExecuteResult{},
		FailedNodes:           map[string]struct{}{},
		FinishedOriginalNodes: map[string]struct{}{},
		StartTime:             time.Now(),
	}
}

func (n *nodeEngine[T]) MountExecutor(executor IExecutor[T]) {
	n.executor = executor
}
