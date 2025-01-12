package kflow

import "github.com/Kidsunbo/kie_toolbox_go/container"

func NewEngine[Input any, Output any, Context any](name string, params ...any) *Engine[Input, Output, Context] {
	var staticNode bool
	for _, param := range params {
		if v, ok := param.(Flag); ok {
			if v == StaticNodes {
				staticNode = true
			}
		}
	}

	// the nodes field
	var nodes *container.Dag[string, INode]
	if staticNode {
		nodes = container.NewDag[string, INode](name, container.DisableThreadSafe)
	} else {
		nodes = container.NewDag[string, INode](name)
	}

	return &Engine[Input, Output, Context]{
		nodes: nodes,
	}
}

