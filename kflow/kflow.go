package kflow

import "fmt"

func AddNode[T any](engine *nodeEngine[T], node INode) error {
	if !ofType[IBasicNode[T]](node) && !ofType[IFlowNode[T]](node) {
		return fmt.Errorf(message(engine.config.Language, unsupportedNodeType), node.Name())
	}

	return engine.nodes.AddVertex(node.Name(), &nodeBox[T]{
		Node:       node,
		BoxName:    node.Name(),
		Conditions: nil,
	})
}
