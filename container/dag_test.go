package container

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddVertex(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.EqualError(t, dag.AddVertex(3, 3), "failed to add vertex, vertex 3 already exist")

	assert.Equal(t, 0, len(dag.cachedTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 8, len(dag.vertices))
	assert.False(t, dag.checked)
}

func TestRemoveVertex(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.Equal(t, 0, len(dag.cachedTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 8, len(dag.vertices))
	assert.False(t, dag.checked)

	assert.NoError(t, dag.RemoveVertex(4))
	assert.EqualError(t, dag.RemoveVertex(4), "failed to remove vertex, vertex 4 doesn't exist")

	assert.Equal(t, 0, len(dag.cachedTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 7, len(dag.vertices))
	assert.False(t, dag.checked)
}

func TestHasVertex(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.Equal(t, 0, len(dag.cachedTopo))
	assert.Equal(t, 8, len(dag.vertices))

	assert.True(t, dag.HasVertex(4))
	assert.False(t, dag.HasVertex(10))
}

func TestAddEdge(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))

	assert.Equal(t, 0, len(dag.cachedTopo))
	assert.Equal(t, 4, len(dag.vertices))
	assert.Equal(t, 0, len(dag.vertices[1].incoming))
	assert.Equal(t, 0, len(dag.vertices[1].outgoing))
	assert.Equal(t, 0, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 0, len(dag.vertices[3].outgoing))
	assert.Equal(t, 0, len(dag.vertices[4].incoming))
	assert.Equal(t, 0, len(dag.vertices[4].outgoing))

	assert.NoError(t, dag.AddEdge(3, 4))
	assert.Equal(t, 0, len(dag.vertices[1].incoming))
	assert.Equal(t, 0, len(dag.vertices[1].outgoing))
	assert.Equal(t, 0, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 1, len(dag.vertices[3].outgoing))
	assert.Equal(t, 1, len(dag.vertices[4].incoming))
	assert.Equal(t, 0, len(dag.vertices[4].outgoing))

	assert.EqualError(t, dag.AddEdge(3, 3), "failed to add edge, the from and to can not be the same")
	assert.EqualError(t, dag.AddEdge(10, 3), "failed to add edge, the from vertex 10 doesn't exist")
	assert.EqualError(t, dag.AddEdge(3, 10), "failed to add edge, the to vertex 10 doesn't exist")

	assert.NoError(t, dag.AddEdge(3, 4))
	assert.Equal(t, 0, len(dag.vertices[1].incoming))
	assert.Equal(t, 0, len(dag.vertices[1].outgoing))
	assert.Equal(t, 0, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 1, len(dag.vertices[3].outgoing))
	assert.Equal(t, 1, len(dag.vertices[4].incoming))
	assert.Equal(t, 0, len(dag.vertices[4].outgoing))

	assert.NoError(t, dag.AddEdge(4, 1))
	assert.Equal(t, 1, len(dag.vertices[1].incoming))
	assert.Equal(t, 0, len(dag.vertices[1].outgoing))
	assert.Equal(t, 0, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 1, len(dag.vertices[3].outgoing))
	assert.Equal(t, 1, len(dag.vertices[4].incoming))
	assert.Equal(t, 1, len(dag.vertices[4].outgoing))
}

func TestRemoveEdge(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))

	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(3, 2))
	assert.Equal(t, 0, len(dag.vertices[1].incoming))
	assert.Equal(t, 1, len(dag.vertices[1].outgoing))
	assert.Equal(t, 2, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 2, len(dag.vertices[3].outgoing))
	assert.Equal(t, 1, len(dag.vertices[4].incoming))
	assert.Equal(t, 0, len(dag.vertices[4].outgoing))

	assert.NoError(t, dag.RemoveEdge(3, 2))
	assert.Equal(t, 0, len(dag.vertices[1].incoming))
	assert.Equal(t, 1, len(dag.vertices[1].outgoing))
	assert.Equal(t, 1, len(dag.vertices[2].incoming))
	assert.Equal(t, 0, len(dag.vertices[2].outgoing))
	assert.Equal(t, 0, len(dag.vertices[3].incoming))
	assert.Equal(t, 1, len(dag.vertices[3].outgoing))
	assert.Equal(t, 1, len(dag.vertices[4].incoming))
	assert.Equal(t, 0, len(dag.vertices[4].outgoing))

	assert.EqualError(t, dag.RemoveEdge(3, 3), "failed to remove edge, the from and to can not be the same")
	assert.EqualError(t, dag.RemoveEdge(10, 3), "failed to remove edge, the from vertex 10 doesn't exist")
	assert.EqualError(t, dag.RemoveEdge(3, 10), "failed to remove edge, the to vertex 10 doesn't exist")
}

func TestCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 4))

	pass, cycles := dag.CheckCycle()
	assert.True(t, pass)
	assert.Nil(t, cycles)
	assert.True(t, dag.IsChecked())

	assert.NoError(t, dag.AddEdge(4, 1))
	assert.False(t, dag.IsChecked())
	pass, cycles = dag.CheckCycle()
	assert.False(t, pass)
	assert.Nil(t, cycles)
	assert.False(t, dag.IsChecked())
}
