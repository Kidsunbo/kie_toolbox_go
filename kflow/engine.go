package kflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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
					err := n.nodes.AddEdge(node.BoxName, v.DependenceName)
					if err != nil {
						return err
					}
				} else {
					nodeName := node.Node.Name()
					condNode := &nodeBox[T]{
						Node:          underlineNode.Node,
						BoxName:       fmt.Sprintf("%v_by_%v", underlineNode.BoxName, nodeName),
						Condition:     v.Condition,
						ConditionalBy: nodeName,
					}
					err := n.nodes.AddVertex(condNode.BoxName, condNode)
					if err != nil {
						return err
					}
					for _, dep := range v.ConditionDependence {
						if !contains(refNodes, dep) {
							return fmt.Errorf(message(n.config.Language, nodeNotExist), dep)
						}
						err := n.nodes.AddEdge(condNode.BoxName, dep)
						if err != nil {
							return err
						}
					}
					err = n.nodes.AddEdge(node.BoxName, condNode.BoxName)
					if err != nil {
						return err
					}
				}
			}
		} else if depNode, ok := node.Node.(IDependency[T, string]); ok {
			for _, dep := range depNode.Dependence() {
				if !contains(refNodes, dep) {
					return fmt.Errorf(message(n.config.Language, nodeNotExist), dep)
				}
				err := n.nodes.AddEdge(node.BoxName, dep)
				if err != nil {
					return err
				}
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
		config:                 n.config,
		chainNodes:             nodes,
		failedNodes:            make(map[string]struct{}),
		finishedOriginalNodes:  make(map[string]struct{}, len(nodes)*4),
		conditionalTargetNodes: make(map[string]struct{}),
		startTime:              time.Now(),
		finishedNodes:          make(map[string]*ExecuteResult, len(nodes)*5),
		runningNodes:           make(map[string]struct{}, 3),
		inParallel:             atomic.Bool{},
		currentNode:            "",
		targetsSummary:         []string{},
		targetNodes:            make(map[string]struct{}),
	}
}

func (n *nodeEngine[T]) MountExecutor(executor IExecutor[T]) {
	n.executor = executor
}

func (n *nodeEngine[T]) Dot() string {
	nodes := n.nodes.GetAllVertices()
	edges := n.nodes.GetAllEdges()

	const dependenceByCondition = 1
	const conditionalDependence = 2
	const normalDependence = 3

	deps := make(map[string]map[string]int, len(nodes))
	for _, node := range nodes {
		deps[node.BoxName] = make(map[string]int)
	}
	for _, node := range nodes {
		if node.Condition == nil {
			for _, edge := range edges[node.BoxName] {
				deps[node.BoxName][edge] = normalDependence
			}
		} else {
			for _, edge := range edges[node.BoxName] {
				deps[node.BoxName][edge] = dependenceByCondition
			}
			deps[node.BoxName][node.Node.Name()] = conditionalDependence
		}
	}

	sb := strings.Builder{}
	sb.WriteString("digraph G {\n")
	for _, node := range nodes {
		if node.Condition == nil {
			sb.WriteString(fmt.Sprintf("  %v;\n", node.BoxName))
		} else {
			sb.WriteString(fmt.Sprintf("  %v [shape=diamond];\n", node.BoxName))
		}
	}
	for from, dep := range deps {
		for to, degree := range dep {
			switch degree {
			case normalDependence:
				sb.WriteString(fmt.Sprintf("  %v -> %v;\n", from, to))
			case conditionalDependence:
				sb.WriteString(fmt.Sprintf("  %v -> %v [color=red];\n", from, to))
			case dependenceByCondition:
				sb.WriteString(fmt.Sprintf("  %v -> %v [color=blue];\n", from, to))
			}
		}
	}
	sb.WriteString("}\n")
	return sb.String()
}
