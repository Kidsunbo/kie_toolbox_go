package container

import (
	"fmt"
	"testing"
)

func TestAddVertex(t *testing.T) {
	dag := NewDag[int, int]("debug")
	if err := dag.AddVertex(1, 1); err != nil {
		panic(err)
	}
	if err := dag.AddVertex(2, 2); err != nil {
		panic(err)
	}
	if err := dag.AddVertex(3, 3); err != nil {
		panic(err)
	}
	if err := dag.AddVertex(4, 4); err != nil {
		panic(err)
	}
	if err := dag.AddVertex(5, 5); err != nil {
		panic(err)
	}


	if err := dag.AddEdge(1, 3); err != nil {
		panic(err)
	}
	if err := dag.AddEdge(1, 2); err != nil {
		panic(err)
	}
	if err := dag.AddEdge(2, 3); err != nil {
		panic(err)
	}
	if err := dag.AddEdge(2, 4); err != nil {
		panic(err)
	}
	if err := dag.AddEdge(5, 1); err != nil {
		panic(err)
	}
	if err := dag.AddEdge(5, 3); err != nil {
		panic(err)
	}

	fmt.Println(dag.TopologicalBatchFrom(1))
	fmt.Println(dag.TopologicalBatchFrom(5))
	fmt.Println(dag.TopologicalBatchFrom(2))
	fmt.Println(dag.TopologicalBatchFrom(4))
}
