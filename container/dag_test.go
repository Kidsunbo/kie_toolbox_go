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

func TestAddVertexEnglishError(t *testing.T) {
	dag := NewDag[int, int]("debug", EnglishError)
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.EqualError(t, dag.AddVertex(3, 3), "failed to add vertex, vertex 3 has already existed")

	assert.Equal(t, 0, len(dag.cachedFullTopo))
	assert.Equal(t, "debug", dag.name)
	assert.Equal(t, 8, len(dag.vertices))
	assert.False(t, dag.checked.Load())
}

func TestAddVertexDefaultError(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))

	assert.EqualError(t, dag.AddVertex(1, 1), "添加节点失败，节点1已经存在")
	assert.EqualError(t, dag.AddVertex(1, 2), "添加节点失败，节点1已经存在")
	assert.EqualError(t, dag.AddVertex(2, 2), "添加节点失败，节点2已经存在")
	assert.EqualError(t, dag.AddVertex(2, 3), "添加节点失败，节点2已经存在")
}

func TestAddVertexChineseError(t *testing.T) {
	dag := NewDag[int, int]("debug", ChineseError)
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))

	assert.EqualError(t, dag.AddVertex(1, 1), "添加节点失败，节点1已经存在")
	assert.EqualError(t, dag.AddVertex(1, 2), "添加节点失败，节点1已经存在")
	assert.EqualError(t, dag.AddVertex(2, 2), "添加节点失败，节点2已经存在")
	assert.EqualError(t, dag.AddVertex(2, 3), "添加节点失败，节点2已经存在")
}

func TestRemoveVertex(t *testing.T) {
	dag := NewDag[int, int]("debug", EnglishError)
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

func TestRemoveVertexDefaultError(t *testing.T) {
	dag := NewDag[int, int]("debug")

	assert.EqualError(t, dag.RemoveVertex(1), "移除节点失败，节点1不存在")
	assert.EqualError(t, dag.RemoveVertex(2), "移除节点失败，节点2不存在")
}

func TestRemoveVertexChineseError(t *testing.T) {
	dag := NewDag[int, int]("debug", ChineseError)

	assert.EqualError(t, dag.RemoveVertex(1), "移除节点失败，节点1不存在")
	assert.EqualError(t, dag.RemoveVertex(2), "移除节点失败，节点2不存在")
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

func TestGetAllEdges(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))

	assert.NoError(t, dag.AddEdge(1, 5))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(1, 3))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(3, 5))
	dag.CheckCycle()

	edges := dag.GetAllEdges()
	assert.Equal(t, 3, len(edges))
	assert.ElementsMatch(t, edges[1], []int{2, 3, 5})
	assert.ElementsMatch(t, edges[2], []int{3})
	assert.ElementsMatch(t, edges[3], []int{5})

	dag.RemoveEdge(3, 5)
	edges = dag.GetAllEdges()
	assert.Equal(t, 2, len(edges))
	assert.ElementsMatch(t, edges[1], []int{2, 3, 5})
	assert.ElementsMatch(t, edges[2], []int{3})

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

func TestGetVertex(t *testing.T) {
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

	value, exist := dag.GetVertex(4)
	assert.True(t, exist)
	assert.Equal(t, 4, value)
	value, exist = dag.GetVertex(10)
	assert.False(t, exist)
	assert.Equal(t, 0, value)
}

func TestAddEdge(t *testing.T) {
	dag := NewDag[int, int]("debug", EnglishError)
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
	dag := NewDag[int, int]("debug", EnglishError)
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
	tb, err := dag.TopologicalBatch()
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{1, 2, 9, 10})
	assert.ElementsMatch(t, tb[1], []int{3, 5})
	assert.ElementsMatch(t, tb[2], []int{4})
	assert.ElementsMatch(t, tb[3], []int{6, 7})
	assert.ElementsMatch(t, tb[4], []int{8})
	cachedData, ok := dag.cachedVertexTopo[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}}))
	_, ok = dag.cachedReverseVertex[1]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}, 7: {}, 4: {}, 5: {}, 2: {}, 3: {}, 1: {}}))
	_, ok = dag.cachedReverseVertex[8]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}}))
	_, ok = dag.cachedReverseVertex[2]
	assert.False(t, ok)

	tb, err = dag.TopologicalBatch(1, 2, 8)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{1, 2})
	assert.ElementsMatch(t, tb[1], []int{3, 5})
	assert.ElementsMatch(t, tb[2], []int{4})
	assert.ElementsMatch(t, tb[3], []int{7})
	assert.ElementsMatch(t, tb[4], []int{8})
	cachedData, ok = dag.cachedVertexTopo[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}}))
	_, ok = dag.cachedReverseVertex[1]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}, 7: {}, 4: {}, 5: {}, 2: {}, 3: {}, 1: {}}))
	_, ok = dag.cachedReverseVertex[8]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}}))
	_, ok = dag.cachedReverseVertex[2]
	assert.False(t, ok)

	tb, err = dag.TopologicalBatch(10)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{10})
	cachedData, ok = dag.cachedVertexTopo[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}}))
	_, ok = dag.cachedReverseVertex[1]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}, 7: {}, 4: {}, 5: {}, 2: {}, 3: {}, 1: {}}))
	_, ok = dag.cachedReverseVertex[8]
	assert.False(t, ok)
	cachedData, ok = dag.cachedVertexTopo[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}}))
	_, ok = dag.cachedReverseVertex[2]
	assert.False(t, ok)

	assert.NoError(t, dag.RemoveEdge(1, 3))
	assert.NoError(t, dag.RemoveEdge(1, 4))
	assert.NoError(t, dag.RemoveEdge(3, 4))
	assert.NoError(t, dag.RemoveEdge(4, 6))
	assert.NoError(t, dag.RemoveEdge(4, 7))
	assert.NoError(t, dag.RemoveEdge(2, 5))
	assert.NoError(t, dag.RemoveEdge(5, 7))
	assert.NoError(t, dag.RemoveEdge(7, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch()
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
	tb, err = dag.TopologicalBatch()
	assert.NoError(t, err)
	assert.Equal(t, 7, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1})
	assert.ElementsMatch(t, tb[2], []int{2, 3})
	assert.ElementsMatch(t, tb[3], []int{4})
	assert.ElementsMatch(t, tb[4], []int{5})
	assert.ElementsMatch(t, tb[5], []int{6})
	assert.ElementsMatch(t, tb[6], []int{7, 8})

	tb, err = dag.TopologicalBatch([]int64{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 7, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1})
	assert.ElementsMatch(t, tb[2], []int{2, 3})
	assert.ElementsMatch(t, tb[3], []int{4})
	assert.ElementsMatch(t, tb[4], []int{5})
	assert.ElementsMatch(t, tb[5], []int{6})
	assert.ElementsMatch(t, tb[6], []int{7, 8})

	tb, err = dag.TopologicalBatch([]int{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1})
	assert.ElementsMatch(t, tb[2], []int{2})

	assert.NoError(t, dag.RemoveEdge(1, 2))
	assert.NoError(t, dag.RemoveEdge(6, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1, 2})

	tb, err = dag.TopologicalBatch([]int{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1, 2})

	tb, err = dag.TopologicalBatch([]int{5, 2})
	assert.NoError(t, err)
	assert.Equal(t, 5, len(tb))
	assert.ElementsMatch(t, tb[0], []int{9, 10})
	assert.ElementsMatch(t, tb[1], []int{1, 2})
	assert.ElementsMatch(t, tb[2], []int{3})
	assert.ElementsMatch(t, tb[3], []int{4})
	assert.ElementsMatch(t, tb[4], []int{5})

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
	tb, err := dag.TopologicalBatch(Reverse)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{9, 10, 8, 6})
	assert.ElementsMatch(t, tb[1], []int{7})
	assert.ElementsMatch(t, tb[2], []int{5, 4})
	assert.ElementsMatch(t, tb[3], []int{3, 2})
	assert.ElementsMatch(t, tb[4], []int{1})
	cachedData, ok := dag.cachedReverseVertex[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}, 3: {}, 4: {}, 6: {}, 7: {}, 8: {}}))
	_, ok = dag.cachedVertexTopo[1]
	assert.False(t, ok)
	cachedData, ok = dag.cachedReverseVertex[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}}))
	_, ok = dag.cachedVertexTopo[8]
	assert.False(t, ok)
	cachedData, ok = dag.cachedReverseVertex[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}, 5: {}, 7: {}, 8: {}}))
	_, ok = dag.cachedVertexTopo[2]
	assert.False(t, ok)

	tb, err = dag.TopologicalBatch(1, 2, 8, Reverse)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tb[0], []int{8, 6})
	assert.ElementsMatch(t, tb[1], []int{7})
	assert.ElementsMatch(t, tb[2], []int{5, 4})
	assert.ElementsMatch(t, tb[3], []int{3, 2})
	assert.ElementsMatch(t, tb[4], []int{1})
	cachedData, ok = dag.cachedReverseVertex[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{1: {}, 3: {}, 4: {}, 6: {}, 7: {}, 8: {}}))
	_, ok = dag.cachedVertexTopo[1]
	assert.False(t, ok)
	cachedData, ok = dag.cachedReverseVertex[8]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{8: {}}))
	_, ok = dag.cachedVertexTopo[8]
	assert.False(t, ok)
	cachedData, ok = dag.cachedReverseVertex[2]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{2: {}, 5: {}, 7: {}, 8: {}}))
	_, ok = dag.cachedVertexTopo[2]
	assert.False(t, ok)
	cachedData, ok = dag.cachedReverseVertex[10]
	assert.True(t, ok)
	assert.ElementsMatch(t, mapToSlice(cachedData), mapToSlice(map[int]struct{}{10: {}}))
	_, ok = dag.cachedVertexTopo[10]
	assert.False(t, ok)

	assert.NoError(t, dag.RemoveEdge(1, 3))
	assert.NoError(t, dag.RemoveEdge(1, 4))
	assert.NoError(t, dag.RemoveEdge(3, 4))
	assert.NoError(t, dag.RemoveEdge(4, 6))
	assert.NoError(t, dag.RemoveEdge(4, 7))
	assert.NoError(t, dag.RemoveEdge(2, 5))
	assert.NoError(t, dag.RemoveEdge(5, 7))
	assert.NoError(t, dag.RemoveEdge(7, 8))
	dag.CheckCycle()
	tb, err = dag.TopologicalBatch(Reverse)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	tb, err = dag.TopologicalBatch(Reverse, []int32{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	tb, err = dag.TopologicalBatch(Reverse, []int{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tb))
	assert.ElementsMatch(t, tb[0], []int{1, 2})

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
	tb, err = dag.TopologicalBatch(Reverse)
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
	tb, err = dag.TopologicalBatch(Reverse, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(tb))
	assert.ElementsMatch(t, tb[0], []int{7, 8})
	assert.ElementsMatch(t, tb[1], []int{6})
	assert.ElementsMatch(t, tb[2], []int{5})
	assert.ElementsMatch(t, tb[3], []int{2, 4})
	assert.ElementsMatch(t, tb[4], []int{3})
	assert.ElementsMatch(t, tb[5], []int{1})

	tb, err = dag.TopologicalBatch(Reverse, []int{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, 6, len(tb))
	assert.ElementsMatch(t, tb[0], []int{7, 8})
	assert.ElementsMatch(t, tb[1], []int{6})
	assert.ElementsMatch(t, tb[2], []int{5})
	assert.ElementsMatch(t, tb[3], []int{2, 4})
	assert.ElementsMatch(t, tb[4], []int{3})
	assert.ElementsMatch(t, tb[5], []int{1})
}

func TestTopologicalBatchSequentiallyWithAlreadyDone(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(4, 3))

	dag.CheckCycle()
	tb, err := dag.TopologicalBatch(1, 4)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{1, 4})

	tb, err = dag.TopologicalBatch(AlreadyDone[int]{3})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{5})

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{3})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 0)

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{2})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{4})

	tb, err = dag.TopologicalBatch(AlreadyDone[int]{2})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 2)
	assert.ElementsMatch(t, tb[0], []int{4, 5})
	assert.ElementsMatch(t, tb[1], []int{3})

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{2, 3})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 0)

	tb, err = dag.TopologicalBatch(AlreadyDone[int]{2, 3})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{5})

	tb, err = dag.TopologicalBatch(AlreadyDone[int]{2, 4})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{5, 3})

	tb, err = dag.TopologicalBatch(AlreadyDone[int]{5})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 3)
	assert.ElementsMatch(t, tb[0], []int{1, 4})
	assert.ElementsMatch(t, tb[1], []int{2})
	assert.ElementsMatch(t, tb[2], []int{3})

	dag = NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(1, 6))
	assert.NoError(t, dag.AddEdge(4, 5))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))

	dag.CheckCycle()

	tb, err = dag.TopologicalBatch(1, 4)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{1, 4})

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{6, 2})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 0)

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{5})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{1})

	tb, err = dag.TopologicalBatch(1, 7, AlreadyDone[int]{6})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{7})

	tb, err = dag.TopologicalBatch(2, 7, AlreadyDone[int]{6})
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{2, 7})

}

func TestTopologicalBatchReverselyWithAlreadyDone(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(5, 4))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(7, 8))
	pass, cycles := dag.CheckCycle()
	assert.True(t, pass)
	assert.Equal(t, len(cycles), 0)

	tb, err := dag.TopologicalBatch(1, 5, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 4)
	assert.ElementsMatch(t, tb[0], []int{4, 8})
	assert.ElementsMatch(t, tb[1], []int{7, 3})
	assert.ElementsMatch(t, tb[2], []int{6, 2})
	assert.ElementsMatch(t, tb[3], []int{5, 1})

	tb, err = dag.TopologicalBatch(1, 5, AlreadyDone[int]{4, 7, 8}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 3)
	assert.ElementsMatch(t, tb[0], []int{3, 6})
	assert.ElementsMatch(t, tb[1], []int{5, 2})
	assert.ElementsMatch(t, tb[2], []int{1})

	tb, err = dag.TopologicalBatch(1, 5, AlreadyDone[int]{4, 7}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 3)
	assert.ElementsMatch(t, tb[0], []int{3, 6})
	assert.ElementsMatch(t, tb[1], []int{5, 2})
	assert.ElementsMatch(t, tb[2], []int{1})

	tb, err = dag.TopologicalBatch(1, 5, AlreadyDone[int]{4, 7, 8, 6}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 3)
	assert.ElementsMatch(t, tb[0], []int{3, 5})
	assert.ElementsMatch(t, tb[1], []int{2})
	assert.ElementsMatch(t, tb[2], []int{1})

	tb, err = dag.TopologicalBatch(2, 5, AlreadyDone[int]{4, 7, 8, 6}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 2)
	assert.ElementsMatch(t, tb[0], []int{3, 5})
	assert.ElementsMatch(t, tb[1], []int{2})

	tb, err = dag.TopologicalBatch(1, 5, AlreadyDone[int]{2}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 4)
	assert.ElementsMatch(t, tb[0], []int{8, 1})
	assert.ElementsMatch(t, tb[1], []int{7})
	assert.ElementsMatch(t, tb[2], []int{6})
	assert.ElementsMatch(t, tb[3], []int{5})

	tb, err = dag.TopologicalBatch(1, 5, AlreadyDone[int]{6, 2, 4}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{1, 5})

	dag = NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(1, 6))
	assert.NoError(t, dag.AddEdge(4, 5))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))

	dag.CheckCycle()

	tb, err = dag.TopologicalBatch(1, 4, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 4)
	assert.ElementsMatch(t, tb[0], []int{3, 7})
	assert.ElementsMatch(t, tb[1], []int{2, 6})
	assert.ElementsMatch(t, tb[2], []int{1, 5})
	assert.ElementsMatch(t, tb[3], []int{4})

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{6, 2}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 2)
	assert.ElementsMatch(t, tb[0], []int{1, 5})
	assert.ElementsMatch(t, tb[1], []int{4})

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{5}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 3)
	assert.ElementsMatch(t, tb[0], []int{3, 4})
	assert.ElementsMatch(t, tb[1], []int{2})
	assert.ElementsMatch(t, tb[2], []int{1})

	tb, err = dag.TopologicalBatch(2, 4, AlreadyDone[int]{5}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 2)
	assert.ElementsMatch(t, tb[0], []int{3, 4})
	assert.ElementsMatch(t, tb[1], []int{2})

	tb, err = dag.TopologicalBatch(2, 4, AlreadyDone[int]{3, 5}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{2, 4})

	tb, err = dag.TopologicalBatch(3, 4, AlreadyDone[int]{5}, Reverse)
	assert.NoError(t, err)
	assert.Equal(t, len(tb), 1)
	assert.ElementsMatch(t, tb[0], []int{3, 4})

	tb, err = dag.TopologicalBatch(100, AlreadyDone[int]{2}, Reverse)
	assert.EqualError(t, err, "没有名字叫100的节点")
	assert.Equal(t, len(tb), 0)

	tb, err = dag.TopologicalBatch(1, 4, AlreadyDone[int]{101}, Reverse)
	assert.EqualError(t, err, "没有名字叫101的节点")
	assert.Equal(t, len(tb), 0)
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
	dagCp.SetName("new_debug")
	assert.Equal(t, "new_debug", dagCp.name)
	assert.NotSame(t, dag.vertices, dagCp.vertices)
	assert.Equal(t, 1, dag.vertices[1].value)

	dagCp = dag.Copy(Copier[int](func(i int) int { return i + 1 }))
	assert.Equal(t, "debug_copy", dagCp.name)
	assert.NotSame(t, dag.vertices, dagCp.vertices)
	assert.Equal(t, 2, dagCp.vertices[1].value)

	dagCp = dag.Copy(func(i int) int { return i + 1 })
	assert.Equal(t, "debug_copy", dagCp.name)
	assert.NotSame(t, dag.vertices, dagCp.vertices)
	assert.Equal(t, 1, dagCp.vertices[1].value)
}

func TestString(t *testing.T) {
	dag := NewDag[int, int]("debug", EnglishError)
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(5, 4))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(7, 8))

	assert.Equal(t, dag.String(), "name: debug, message: acyclicity not checked")

	dag.CheckCycle()

	assert.NotPanics(t, func() {
		_ = dag.String()
	})
}

func TestDot(t *testing.T) {
	dag := NewDag[int, int]("debug", EnglishError)
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(5, 4))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(7, 8))

	assert.Equal(t, dag.Dot(), "name: debug, message: acyclicity not checked")

	dag.CheckCycle()

	assert.NotPanics(t, func() {
		_ = dag.Dot()
	})
}

func TestCanReach(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))
	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(2, 3))
	assert.NoError(t, dag.AddEdge(3, 4))
	assert.NoError(t, dag.AddEdge(6, 4))

	yes, err := dag.CanReach(1, 2)
	assert.EqualError(t, err, "未检查图中是否包含环，请调用CheckCycle进行检查")
	assert.False(t, yes)

	yes, err = dag.CanReach(10, 2)
	assert.EqualError(t, err, "没有名字叫10的节点")
	assert.False(t, yes)

	yes, err = dag.CanReach(1, 20)
	assert.EqualError(t, err, "没有名字叫20的节点")
	assert.False(t, yes)

	yes, cycles := dag.CheckCycle()
	assert.Equal(t, 0, len(cycles))
	assert.True(t, yes)

	yes, err = dag.CanReach(10, 2)
	assert.EqualError(t, err, "没有名字叫10的节点")
	assert.False(t, yes)

	yes, err = dag.CanReach(1, 20)
	assert.EqualError(t, err, "没有名字叫20的节点")
	assert.False(t, yes)

	yes, err = dag.CanReach(1, 2)
	assert.Nil(t, err)
	assert.True(t, yes)

	yes, err = dag.CanReach(1, 3)
	assert.Nil(t, err)
	assert.True(t, yes)

	yes, err = dag.CanReach(2, 4)
	assert.Nil(t, err)
	assert.True(t, yes)

	yes, err = dag.CanReach(6, 4)
	assert.Nil(t, err)
	assert.True(t, yes)

	yes, err = dag.CanReach(3, 6)
	assert.Nil(t, err)
	assert.False(t, yes)

	yes, err = dag.CanReach(8, 7)
	assert.Nil(t, err)
	assert.False(t, yes)

	yes, err = dag.CanReach(7, 8)
	assert.Nil(t, err)
	assert.False(t, yes)

	yes, err = dag.CanReach(7, 7)
	assert.Nil(t, err)
	assert.True(t, yes)

	yes, err = dag.CanReach(2, 2)
	assert.Nil(t, err)
	assert.True(t, yes)
}

func TestNextBatch(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(1, 5))
	assert.NoError(t, dag.AddEdge(1, 6))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 3))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(7, 4))
	assert.NoError(t, dag.AddEdge(7, 8))
	assert.NoError(t, dag.AddEdge(4, 8))

	pass, cycles := dag.CheckCycle()
	assert.True(t, pass)
	assert.Equal(t, 0, len(cycles))

	next, err := dag.NextBatch()
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{1})

	next, err = dag.NextBatch(AlreadyDone[int]{2})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{5})

	next, err = dag.NextBatch(AlreadyDone[int]{1})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{5, 2})

	next, err = dag.NextBatch(AlreadyDone[int]{6})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{2, 3, 7})

}

func TestNextBatchReversely(t *testing.T) {
	dag := NewDag[int, int]("debug")
	assert.NoError(t, dag.AddVertex(1, 1))
	assert.NoError(t, dag.AddVertex(2, 2))
	assert.NoError(t, dag.AddVertex(3, 3))
	assert.NoError(t, dag.AddVertex(4, 4))
	assert.NoError(t, dag.AddVertex(5, 5))
	assert.NoError(t, dag.AddVertex(6, 6))
	assert.NoError(t, dag.AddVertex(7, 7))
	assert.NoError(t, dag.AddVertex(8, 8))

	assert.NoError(t, dag.AddEdge(1, 2))
	assert.NoError(t, dag.AddEdge(1, 5))
	assert.NoError(t, dag.AddEdge(1, 6))
	assert.NoError(t, dag.AddEdge(5, 6))
	assert.NoError(t, dag.AddEdge(6, 3))
	assert.NoError(t, dag.AddEdge(6, 7))
	assert.NoError(t, dag.AddEdge(7, 4))
	assert.NoError(t, dag.AddEdge(7, 8))
	assert.NoError(t, dag.AddEdge(4, 8))

	pass, cycles := dag.CheckCycle()
	assert.True(t, pass)
	assert.Equal(t, 0, len(cycles))

	next, err := dag.NextBatch(Reverse)
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{2, 3, 8})

	next, err = dag.NextBatch(Reverse, AlreadyDone[int]{2})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{3, 8})

	next, err = dag.NextBatch(Reverse, AlreadyDone[int]{1})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{})

	next, err = dag.NextBatch(Reverse, AlreadyDone[int]{6})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{2, 5})

	next, err = dag.NextBatch(5, Reverse, AlreadyDone[int]{6})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{5})

	next, err = dag.NextBatch(5, Reverse, AlreadyDone[int]{4})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{3, 7})

	next, err = dag.NextBatch(2, Reverse, AlreadyDone[int]{6})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{2})

	next, err = dag.NextBatch(7, Reverse, AlreadyDone[int]{4})
	assert.Nil(t, err)
	assert.ElementsMatch(t, next, []int{7})
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
			dag.TopologicalBatch()
			dag.TopologicalBatch(Reverse)
		}()
	}
	wg.Wait()
}

func TestRaceStringAndCopy(t *testing.T) {
	dag := NewDag[int, int]("debug")
	dag.addVertex(0, 0)
	for i := 1; i <= 10; i++ {
		dag.AddVertex(i, i)
		dag.AddEdge(i-1, i)
	}

	dag.CheckCycle()

	wg := sync.WaitGroup{}
	wg.Add(30)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 1; i < 1000; i++ {
				for j := 0; j < 10; j++ {
					start := i*10 + 1
					end := i*10 + 10
					dag.AddVertex(start, start)
					for s := start + 1; s < end; s++ {
						dag.AddVertex(s, s)
						dag.AddEdge(start-1, start)
					}
					for s := start; s < end; s++ {
						dag.RemoveVertex(s)
					}
					pass, cycles := dag.CheckCycle()
					assert.True(t, pass)
					assert.Equal(t, 0, len(cycles))
				}
			}
		}()
	}

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 1; i < 10000; i++ {
				pass, cycles := dag.CheckCycle()
				assert.True(t, pass)
				assert.Equal(t, 0, len(cycles))
				cp := dag.Copy(nil)
				assert.NotNil(t, cp)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 1; i < 10000; i++ {
				pass, cycles := dag.CheckCycle()
				assert.True(t, pass)
				assert.Equal(t, 0, len(cycles))
				cp := dag.String()
				assert.Greater(t, len(cp), 0)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSortNoThreadSafeNoCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
				dag.NextBatch()
				dag.NextBatch(Reverse)
				dag.NextBatch(AlreadyDone[int]{1, 2})
				dag.NextBatch(AlreadyDone[int]{1, 2}, Reverse)
			}
		}()
	}
	wg.Wait()
}

func TestRaceSortNoThreadSafeButCheckCycle(t *testing.T) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
			}
		}()
	}
	wg.Wait()
}

// Benchmark
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
				dag.NextBatch()
				dag.NextBatch(Reverse)
			}
		}()
	}
	wg.Wait()

}

func BenchmarkDag3(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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
				dag.TopologicalBatch()
				dag.TopologicalBatch(Reverse)
				dag.NextBatch()
				dag.NextBatch(Reverse)
			}
		}()
	}
	wg.Wait()

}

func BenchmarkDag4(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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

	for i := 0; i < b.N; i++ {
		dag.TopologicalSort()
	}
}

func BenchmarkDag5(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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

	for i := 0; i < b.N; i++ {
		dag.TopologicalBatch()
	}
}

func BenchmarkDag6(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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

	for i := 0; i < b.N; i++ {
		dag.TopologicalBatch(Reverse)
	}
}

func BenchmarkDag7(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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

	for i := 0; i < b.N; i++ {
		dag.NextBatch()
	}
}

func BenchmarkDag8(b *testing.B) {
	dag := NewDag[int, int]("debug", DisableThreadSafe)
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

	for i := 0; i < b.N; i++ {
		dag.NextBatch(Reverse)
	}
}
