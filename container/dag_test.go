package container

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mapToSlice[K comparable, T any](m map[K]T) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

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

	assert.Equal(t, 0, len(dag.cachedFullTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 8, len(dag.vertices))
	assert.False(t, dag.checked.Load())
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

	assert.Equal(t, 0, len(dag.cachedFullTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 8, len(dag.vertices))
	assert.False(t, dag.checked.Load())

	assert.NoError(t, dag.RemoveVertex(4))
	assert.EqualError(t, dag.RemoveVertex(4), "failed to remove vertex, vertex 4 doesn't exist")

	assert.Equal(t, 0, len(dag.cachedFullTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 7, len(dag.vertices))
	assert.False(t, dag.checked.Load())
}

func TestGetAllVertex(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.ElementsMatch(t, dag.GetAllVertices(), []int{1, 2, 3, 4, 5, 6, 7, 8})

	dag.RemoveVertex(3)
	assert.ElementsMatch(t, dag.GetAllVertices(), []int{1, 2, 4, 5, 6, 7, 8})

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

	assert.Equal(t, 0, len(dag.cachedFullTopo))
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

	assert.Equal(t, 0, len(dag.cachedFullTopo))
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
	assert.Equal(t, 1, len(cycles))
	assert.ElementsMatch(t, cycles[0], []int{1, 2, 4})
	assert.False(t, dag.IsChecked())

	assert.NoError(t, dag.RemoveEdge(4, 1))
	pass, cycles = dag.CheckCycle()
	assert.True(t, pass)
	assert.Nil(t, cycles)
	assert.True(t, dag.IsChecked())

	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddVertex(9, 9))
	assert.NoError(t, dag.AddVertex(10, 10))
	assert.NoError(t, dag.AddVertex(11, 11))
	assert.NoError(t, dag.AddVertex(12, 12))
	assert.NoError(t, dag.AddVertex(13, 13))
	assert.NoError(t, dag.AddEdge(4, 5))
	assert.NoError(t, dag.AddEdge(5, 7))
	assert.NoError(t, dag.AddEdge(7, 6))
	assert.NoError(t, dag.AddEdge(6, 2))
	assert.NoError(t, dag.AddEdge(8, 9))
	assert.NoError(t, dag.AddEdge(9, 10))
	assert.NoError(t, dag.AddEdge(10, 11))
	assert.NoError(t, dag.AddEdge(10, 8))
	assert.NoError(t, dag.AddEdge(12, 13))
	assert.NoError(t, dag.AddEdge(13, 12))
	assert.False(t, dag.IsChecked())
	pass, cycles = dag.CheckCycle()
	assert.False(t, pass)
	assert.Equal(t, 3, len(cycles))
	sort.Slice(cycles, func(i, j int) bool {
		sort.Ints(cycles[i])
		sort.Ints(cycles[j])
		return cycles[i][0] < cycles[j][0]
	})
	assert.ElementsMatch(t, cycles[0], []int{2, 4, 5, 6, 7})
	assert.ElementsMatch(t, cycles[1], []int{8, 9, 10})
	assert.ElementsMatch(t, cycles[2], []int{12, 13})
	assert.False(t, dag.IsChecked())

}

func TestTopologicalSort(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(1, 4))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(4, 6))
	assert.NoError(t, dag.AddEdge(4, 7))
	assert.NoError(t, dag.AddEdge(2, 5))
	assert.NoError(t, dag.AddEdge(5, 7))
	assert.NoError(t, dag.AddEdge(7, 8))
	dag.CheckCycle()
	ts, err := dag.TopologicalSort()
	assert.NoError(t, err)
	assert.ElementsMatch(t, ts[:2], []int{1, 2})
	assert.ElementsMatch(t, ts[2:4], []int{3, 5})
	assert.ElementsMatch(t, ts[4:5], []int{4})
	assert.ElementsMatch(t, ts[5:7], []int{6, 7})
	assert.ElementsMatch(t, ts[7:8], []int{8})
	assert.Equal(t, len(dag.cachedFullTopo), 5)
	assert.ElementsMatch(t, dag.cachedFullTopo[0], []int{1, 2})
	assert.ElementsMatch(t, dag.cachedFullTopo[1], []int{3, 5})
	assert.ElementsMatch(t, dag.cachedFullTopo[2], []int{4})
	assert.ElementsMatch(t, dag.cachedFullTopo[3], []int{6, 7})
	assert.ElementsMatch(t, dag.cachedFullTopo[4], []int{8})

	assert.NoError(t, dag.RemoveEdge(3, 4))
	dag.CheckCycle()
	ts, err = dag.TopologicalSort()
	assert.NoError(t, err)
	assert.ElementsMatch(t, ts[:2], []int{1, 2})
	assert.ElementsMatch(t, ts[2:5], []int{3, 4, 5})
	assert.ElementsMatch(t, ts[5:7], []int{6, 7})
	assert.ElementsMatch(t, ts[7:8], []int{8})
	assert.Equal(t, len(dag.cachedFullTopo), 4)
	assert.ElementsMatch(t, dag.cachedFullTopo[0], []int{1, 2})
	assert.ElementsMatch(t, dag.cachedFullTopo[1], []int{3, 4, 5})
	assert.ElementsMatch(t, dag.cachedFullTopo[2], []int{6, 7})
	assert.ElementsMatch(t, dag.cachedFullTopo[3], []int{8})
}

func TestTopologicalBatchSequentially(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddVertex(9, 9))
	assert.NoError(t, dag.AddVertex(10, 10))
	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(1, 4))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(4, 6))
	assert.NoError(t, dag.AddEdge(4, 7))
	assert.NoError(t, dag.AddEdge(2, 5))
	assert.NoError(t, dag.AddEdge(5, 7))
	assert.NoError(t, dag.AddEdge(7, 8))
	dag.CheckCycle()
	tb, err := dag.TopologicalBatch(false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{1, 2, 9, 10})
	assert.ElementsMatch(t, tb[1], []int{3, 5})
	assert.ElementsMatch(t, tb[2], []int{4})
	assert.ElementsMatch(t, tb[3], []int{6, 7})
	assert.ElementsMatch(t, tb[4], []int{8})
	cachedData, ok := dag.cachedVertexTopo[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}, 3: {}, 4: {}, 6: {}, 7: {}, 8: {}}))
	cachedData, ok = dag.cachedVertexTopo[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}}))
	cachedData, ok = dag.cachedVertexTopo[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}, 5: {}, 7: {}, 8: {}}))

	assert.NoError(t, dag.RemoveEdge(1, 3))
	assert.NoError(t, dag.RemoveEdge(1, 4))
	assert.NoError(t, dag.RemoveEdge(3, 4))
	assert.NoError(t, dag.RemoveEdge(4, 6))
	assert.NoError(t, dag.RemoveEdge(4, 7))
	assert.NoError(t, dag.RemoveEdge(2, 5))
	assert.NoError(t, dag.RemoveEdge(5, 7))
	assert.NoError(t, dag.RemoveEdge(7, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(4, 5))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(6, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 5))
	assert.NoError(t, dag.AddEdge(2, 6))
	assert.NoError(t, dag.AddEdge(2, 8))
	assert.NoError(t, dag.AddEdge(10, 2))
	assert.NoError(t, dag.AddEdge(10, 1))
	assert.NoError(t, dag.AddEdge(9, 1))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(false)
	assert.NoError(t, err)
	assert.Equal(t, 7, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1})
	assert.ElementsMatch(t, tb[2], []int{2, 3})
	assert.ElementsMatch(t, tb[3], []int{4})
	assert.ElementsMatch(t, tb[4], []int{5})
	assert.ElementsMatch(t, tb[5], []int{6})
	assert.ElementsMatch(t, tb[6], []int{7, 8})

	assert.NoError(t, dag.RemoveEdge(1, 2))
	assert.NoError(t, dag.RemoveEdge(6, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(false, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2})
	assert.ElementsMatch(t, tb[1], []int{3, 8})
	assert.ElementsMatch(t, tb[2], []int{4})
	assert.ElementsMatch(t, tb[3], []int{5})
	assert.ElementsMatch(t, tb[4], []int{6})
	assert.ElementsMatch(t, tb[5], []int{7})
}

func TestTopologicalBatchReversely(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddVertex(9, 9))
	assert.NoError(t, dag.AddVertex(10, 10))
	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(1, 4))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(4, 6))
	assert.NoError(t, dag.AddEdge(4, 7))
	assert.NoError(t, dag.AddEdge(2, 5))
	assert.NoError(t, dag.AddEdge(5, 7))
	assert.NoError(t, dag.AddEdge(7, 8))
	dag.CheckCycle()
	tb, err := dag.TopologicalBatch(true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{9, 10, 8, 6})
	assert.ElementsMatch(t, tb[1], []int{7})
	assert.ElementsMatch(t, tb[2], []int{5, 4})
	assert.ElementsMatch(t, tb[3], []int{3, 2})
	assert.ElementsMatch(t, tb[4], []int{1})
	cachedData, ok := dag.cachedVertexTopo[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}, 3: {}, 4: {}, 6: {}, 7: {}, 8: {}}))
	cachedData, ok = dag.cachedVertexTopo[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}}))
	cachedData, ok = dag.cachedVertexTopo[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}, 5: {}, 7: {}, 8: {}}))

	assert.NoError(t, dag.RemoveEdge(1, 3))
	assert.NoError(t, dag.RemoveEdge(1, 4))
	assert.NoError(t, dag.RemoveEdge(3, 4))
	assert.NoError(t, dag.RemoveEdge(4, 6))
	assert.NoError(t, dag.RemoveEdge(4, 7))
	assert.NoError(t, dag.RemoveEdge(2, 5))
	assert.NoError(t, dag.RemoveEdge(5, 7))
	assert.NoError(t, dag.RemoveEdge(7, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(4, 5))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(6, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 5))
	assert.NoError(t, dag.AddEdge(2, 6))
	assert.NoError(t, dag.AddEdge(2, 8))
	assert.NoError(t, dag.AddEdge(10, 2))
	assert.NoError(t, dag.AddEdge(10, 1))
	assert.NoError(t, dag.AddEdge(9, 1))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(true)
	assert.NoError(t, err)
	assert.Equal(t, 7, len(tb))
	assert.ElementsMatch(t, tb[0], []int{8, 7})
	assert.ElementsMatch(t, tb[1], []int{6})
	assert.ElementsMatch(t, tb[2], []int{5})
	assert.ElementsMatch(t, tb[3], []int{4, 2})
	assert.ElementsMatch(t, tb[4], []int{3})
	assert.ElementsMatch(t, tb[5], []int{1})
	assert.ElementsMatch(t, tb[6], []int{9, 10})

	assert.NoError(t, dag.RemoveEdge(1, 2))
	assert.NoError(t, dag.RemoveEdge(6, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(true, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(tb))
	assert.ElementsMatch(t, tb[0], []int{7, 8})
	assert.ElementsMatch(t, tb[1], []int{6})
	assert.ElementsMatch(t, tb[2], []int{5})
	assert.ElementsMatch(t, tb[3], []int{2, 4})
	assert.ElementsMatch(t, tb[4], []int{3})
	assert.ElementsMatch(t, tb[5], []int{1})
}

func TestCopy(t *testing.T) {
	dag := NewDag[int, int]("debug")
	dag.AddVertex(1, 1)
	dag.AddVertex(2, 2)
	dag.AddVertex(3, 3)
	dag.AddVertex(4, 4)

	dag.AddEdge(1, 2)
	dag.AddEdge(2, 4)
	dag.AddEdge(3, 4)
	dagCp := dag.Copy()
	assert.Equal(t, "debug_copy", dagCp.name)
	assert.NotSame(t, dag.vertices, dagCp.vertices)
}

func TestRaceAdd(t *testing.T) {
	dag := NewDag[string, string]("debug")
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		key := i
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				v := fmt.Sprintf("%v_%v", key, i)
				dag.AddVertex(v, v)
			}

			for i := 0; i < 10000; i++ {
				v := fmt.Sprintf("%v_%v", key, i)
				dag.RemoveVertex(v)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSort(t *testing.T) {
	dag := NewDag[int, int]("debug")
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)

	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			dag.TopologicalSort()
			dag.TopologicalBatch(false)
			dag.TopologicalBatch(true)
		}()
	}
	wg.Wait()
}

func TestRaceSortNoThreadSafeNoCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug", ConfigDisableThreadSafe(true))
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)

	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSortNoThreadSafeButCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug", ConfigDisableThreadSafe(true))
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				dag.CheckCycle()
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
				dag.setChecked(false)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSortThreadSafeAndCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug")
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				dag.CheckCycle()
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
				dag.setChecked(false)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSortThreadSafeNoCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug")
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)
	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkDag(b *testing.B) {
	dag := NewDag[int, int]("debug")
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				dag.CheckCycle()
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
				dag.setChecked(false)
			}
		}()
	}
	wg.Wait()

}

func BenchmarkDag2(b *testing.B) {
	dag := NewDag[int, int]("debug")
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)
	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
			}
		}()
	}
	wg.Wait()

}

func BenchmarkDag3(b *testing.B) {
	dag := NewDag[int, int]("debug", ConfigDisableThreadSafe(true))
	for i := 0; i < 100; i++ {
		dag.AddVertex(i, i)
	}

	dag.AddEdge(10, 30)
	dag.AddEdge(10, 20)
	dag.AddEdge(14, 40)
	dag.AddEdge(31, 40)
	dag.AddEdge(43, 50)
	dag.AddEdge(70, 65)
	dag.AddEdge(43, 51)
	dag.AddEdge(32, 33)
	dag.AddEdge(1, 70)
	dag.AddEdge(11, 13)
	dag.AddEdge(34, 31)
	dag.AddEdge(2, 3)
	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				dag.TopologicalSort()
				dag.TopologicalBatch(false)
				dag.TopologicalBatch(true)
			}
		}()
	}
	wg.Wait()

}
