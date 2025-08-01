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

type Engine[T any] struct {
	config   *config
	nodes    *container.Dag[string, *nodeBox[T]]
	executor IExecutor[T]
}

// NewEngine creates a node engine that runs all the node.
func NewEngine[T any](name string, params ...any) *Engine[T] {
	config := &config{
		Name:     name,
		Language: chinese,
		Timeout:  30,
		SafeRun:  false,
	}
	for _, param := range params {
		if v, ok := param.(flag); ok {
			if v == ReportInChinese {
				config.Language = chinese
			} else if v == ReportInEnglish {
				config.Language = english
			} else if v == SafeRun {
				config.SafeRun = true
			}
		} else if v, ok := param.(AsyncTimeout); ok {
			config.Timeout = time.Duration(v)
		}
	}

	var flags []any
	if config.Language == english {
		flags = append(flags, container.EnglishError)
	}

	return &Engine[T]{
		config:   config,
		nodes:    container.NewDag[string, *nodeBox[T]](name, flags...),
		executor: newNodeExecutor[T](),
	}
}

// Prepare will compile the graph and anylize the dependence. It will also check and report the cyclic.
func (n *Engine[T]) Prepare() error {
	if n.nodes.IsChecked() {
		return nil
	}

	// add conditional nodes
	allNodes := n.nodes.GetAllVertices()
	refNodes := make(map[string]*nodeBox[T], len(allNodes))
	for _, node := range allNodes {
		refNodes[node.BoxName] = node
	}

	originalEdge := make(map[string]string)
	for _, node := range allNodes {
		if depNode, ok := node.Node.(IDependency[T, *Dependency[T]]); ok {
			for _, v := range depNode.Dependencies() {
				if v == nil {
					continue
				}
				underlineNode, exist := refNodes[v.DependencyName]
				if !exist {
					return fmt.Errorf(message(n.config.Language, nodeNotExist), v.DependencyName)
				}
				if v.Condition == nil {
					err := n.nodes.AddEdge(node.BoxName, v.DependencyName)
					if err != nil {
						return err
					}
				} else {
					nodeName := node.Node.Name()
					boxName := fmt.Sprintf("%v_by_%v", underlineNode.BoxName, nodeName)
					if n.nodes.HasVertex(boxName) {
						continue
					}
					condNode := &nodeBox[T]{
						Node:          underlineNode.Node,
						BoxName:       boxName,
						Condition:     v.Condition,
						ConditionalBy: nodeName,
					}
					err := n.nodes.AddVertex(condNode.BoxName, condNode)
					if err != nil {
						return err
					}

					// temporarily add edge between condition node and origin node for cycle checking, it will be removed after checking.
					originalEdge[condNode.BoxName] = underlineNode.BoxName
					err = n.nodes.AddEdge(condNode.BoxName, underlineNode.BoxName)
					if err != nil {
						return err
					}
					for _, dep := range v.ConditionDependencies {
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
			for _, dep := range depNode.Dependencies() {
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

	for k, v := range originalEdge {
		err := n.nodes.RemoveEdge(k, v)
		if err != nil {
			return err
		}
	}
	pass, cycles = n.nodes.CheckCycle()
	if !pass {
		return fmt.Errorf(message(n.config.Language, cycleDetectedError), cycles)
	}

	return nil
}

// Run starts the engine and accept the state object. At least one node name needs to be passed in. If multiple nodes has been passed in, it will chain all them together.
func (n *Engine[T]) Run(ctx context.Context, state T, node string, rest ...string) error {
	nodes := append([]string{node}, rest...)
	if err := n.check(nodes); err != nil {
		return err
	}

	if err := n.execute(ctx, state, nodes); err != nil {
		return err
	}

	return nil
}

func (n *Engine[T]) check(nodes []string) error {
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

func (n *Engine[T]) execute(ctx context.Context, state T, nodes []string) error {
	return n.executor.Execute(ctx, n.nodes, state, n.makePlan(nodes))
}

func (n *Engine[T]) makePlan(nodes []string) *Plan {
	return &Plan{
		config:                 n.config,
		executor:               n.executor,
		nodes:                  n.nodes,
		stop:                   atomic.Bool{},
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

func (n *Engine[T]) MountExecutor(executor IExecutor[T]) {
	n.executor = executor
}

func (n *Engine[T]) Dot() string {
	nodes := n.nodes.GetAllVertices()
	edges := n.nodes.GetAllEdges()

	const dependencyByCondition = 1
	const conditionalDependency = 2
	const normalDependency = 3

	deps := make(map[string]map[string]int, len(nodes))
	for _, node := range nodes {
		deps[node.BoxName] = make(map[string]int)
	}
	for _, node := range nodes {
		if node.Condition == nil {
			for _, edge := range edges[node.BoxName] {
				deps[node.BoxName][edge] = normalDependency
			}
		} else {
			for _, edge := range edges[node.BoxName] {
				deps[node.BoxName][edge] = dependencyByCondition
			}
			deps[node.BoxName][node.Node.Name()] = conditionalDependency
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
			case normalDependency:
				sb.WriteString(fmt.Sprintf("  %v -> %v;\n", from, to))
			case conditionalDependency:
				sb.WriteString(fmt.Sprintf("  %v -> %v [color=red];\n", from, to))
			case dependencyByCondition:
				sb.WriteString(fmt.Sprintf("  %v -> %v [color=blue];\n", from, to))
			}
		}
	}
	sb.WriteString("}\n")
	return sb.String()
}
