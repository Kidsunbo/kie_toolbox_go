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
	nodes    *container.Dag[string, *nodeBox[T]]
	executor IExecutor[T]
}

// NewEngine creates a node engine that runs all the node.
func NewEngine[T any](name string, params ...any) *nodeEngine[T] {
	config := &config{
		Language: chinese,
	}
	for _, param := range params {
		if v, ok := param.(flag); ok {
			if v == ReportInChinese {
				config.Language = chinese
			} else if v == ReportInEnglish {
				config.Language = english
			}
		}
	}

	var flags []any
	if config.Language == english {
		flags = append(flags, container.EnglishError)
	}

	return &nodeEngine[T]{
		config:   config,
		nodes:    container.NewDag[string, *nodeBox[T]](name, flags...),
		executor: newNodeExecutor[T](),
	}
}

func (n *nodeEngine[T]) Prepare() error {
	if n.nodes.IsChecked() {
		return nil
	}

	// add conditional nodes
	allNodes := n.nodes.GetAllVertices()
	refNodes := make(map[string]*nodeBox[T], len(allNodes))
	for _, node := range allNodes {
		refNodes[node.BoxName] = node
	}

	for _, node := range allNodes {
		if depNode, ok := node.Node.(IDependency[T, *Dependence[T]]); ok {
			for _, v := range depNode.Dependence() {
				if v == nil {
					continue
				}
				underlineNode, exist := refNodes[v.DependenceName]
				if !exist {
					return fmt.Errorf(message(n.config.Language, nodeNotExist), v.DependenceName)
				}
				if v.Condition == nil {
					n.nodes.AddEdge(node.BoxName, v.DependenceName)
				} else {
					condNode := &nodeBox[T]{
						Node:       underlineNode.Node,
						BoxName:    fmt.Sprintf("%v_by_%v", underlineNode.BoxName, node.Node.Name()),
						Conditions: v.Condition,
					}
					n.nodes.AddVertex(condNode.BoxName, condNode)
					for _, dep := range v.ConditionDependence {
						if !contains(refNodes, dep) {
							return fmt.Errorf(message(n.config.Language, nodeNotExist), dep)
						}
						n.nodes.AddEdge(condNode.BoxName, dep)
					}
					n.nodes.AddEdge(node.BoxName, condNode.BoxName)
				}
			}
		} else if depNode, ok := node.Node.(IDependency[T, string]); ok {
			for _, dep := range depNode.Dependence() {
				if !contains(refNodes, dep) {
					return fmt.Errorf(message(n.config.Language, nodeNotExist), dep)
				}
				n.nodes.AddEdge(node.BoxName, dep)
			}
		}
	}

	pass, cycles := n.nodes.CheckCycle()
	if !pass {
		return fmt.Errorf(message(n.config.Language, cycleDetectedError), cycles)
	}

	return nil
}

// Run starts the engine and accept the state object. At least one node name needs to be passed in. If multiple nodes has been passed in, it will chain all them together.
func (n *nodeEngine[T]) Run(ctx context.Context, state T, node string, rest ...string) error {
	_, err := n.RunAndObserve(ctx, state, node, rest...)
	return err
}

func (n *nodeEngine[T]) RunAndObserve(ctx context.Context, state T, node string, rest ...string) (*Plan, error) {
	nodes := append([]string{node}, rest...)
	if err := n.check(nodes); err != nil {
		return nil, err
	}

	plan := n.makePlan(nodes)
	if err := n.execute(ctx, state, plan); err != nil {
		return nil, err
	}

	return plan, nil
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

func (n *nodeEngine[T]) execute(ctx context.Context, state T, plan *Plan) error {
	return n.executor.Execute(ctx, n.nodes, state, plan)
}

func (n *nodeEngine[T]) makePlan(nodes []string) *Plan {
	return &Plan{
		Config:                n.config,
		ChainNodes:            nodes,
		CurrentNode:           "",
		InParallel:            false,
		TargetNodes:           make(map[string]struct{}),
		RunningNodes:          make(map[string]struct{}, 3),
		FinishedNodes:         make(map[string]*ExecuteResult, len(nodes)*5),
		FailedNodes:           make(map[string]struct{}),
		FinishedOriginalNodes: make(map[string]struct{}, len(nodes)*4),
		StartTime:             time.Now(),
	}
}

func (n *nodeEngine[T]) MountExecutor(executor IExecutor[T]) {
	n.executor = executor
}
