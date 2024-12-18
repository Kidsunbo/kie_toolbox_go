/*
This library assumes that A is the root and the other vertices are the dependencies. By default, the E and F will be in the batch 3 and C will be in the batch 2.
Sometimes, if you would like the vertex be executed eagerly, you could specify the align right.

                   +---------+
                   |         |
             +---->|    B    +---+
             |     |         |   |
             |     +---------+   |
             |                   |    +--------+
             |                   |    |        |
             |                   +--->|   E    |
+--------+   |     +---------+        |        |
|        |   |     |         |        +--------+
|   A    +---+---->|    C    |
|        |   |     |         |
+--------+   |     +---------+
             |                        +--------+
             |                        |        |
             |                   +--->|   F    |
             |     +---------+   |    |        |
             |     |         |   |    +--------+
             +---->|    D    +---+
                   |         |
                   +---------+
*/

package container

import (
	"errors"
	"fmt"
	"sync"
)

type vertex[K comparable, T any] struct {
	name     K
	value    T
	outgoing map[K]*vertex[K, T]
	incoming map[K]*vertex[K, T]
}

func newVertex[K comparable, T any](name K, value T) *vertex[K, T] {
	return &vertex[K, T]{
		name:     name,
		value:    value,
		outgoing: map[K]*vertex[K, T]{},
		incoming: map[K]*vertex[K, T]{},
	}
}

func (v *vertex[K, T]) String() string {
	return fmt.Sprint(v.name)
}

type Dag[K comparable, T any] struct {
	name             string
	vertices         map[K]*vertex[K, T]
	checked          bool
	cachedFullTopo   [][]T
	cachedVertexTopo sync.Map // key: vertex name, value: map[K]struct{} (key: the dependency, value: the dependent key)
}

// NewDag create a directed acycle graph with each vertex whose key is of type K and the value is of type T.
// Normally, the name is of a string which keep unique in the whole graph. The value can be different.
func NewDag[K comparable, T any](name string) *Dag[K, T] {
	return &Dag[K, T]{
		name:     name,
		vertices: map[K]*vertex[K, T]{},
		checked:  false,
	}
}

// AddVertex add the vertex into the dag. Because adding new vertex might cause a cycle in the graph, so it will make the dag unchecked.
func (d *Dag[K, T]) AddVertex(name K, value T) error {
	if _, exist := d.vertices[name]; exist {
		return fmt.Errorf("failed to add vertex, vertex %v already exist", name)
	}
	d.checked = false
	d.vertices[name] = newVertex(name, value)
	return nil
}

// RemoveVertex removes the vertex from the dag. There is no chance to create cycle in the graph, but the topological batch might change.
func (d *Dag[K, T]) RemoveVertex(name K) error {
	cur, exist := d.vertices[name]
	if !exist {
		return fmt.Errorf("failed to remove vertex, vertex %v doesn't exist", name)
	}

	for _, vertex := range cur.incoming {
		d.RemoveEdge(vertex.name, cur.name)
	}

	for _, vertex := range cur.outgoing {
		d.RemoveEdge(cur.name, vertex.name)
	}

	d.checked = false
	delete(d.vertices, name)
	return nil
}

// HasVertex only checks if the vertex has been added to the dag.
func (d *Dag[K, T]) HasVertex(name K) bool {
	_, exist := d.vertices[name]
	return exist
}

// AddEdge adds the relationship between two vertex, which might cause a cycle in dag. If the key of from vertex and the to vertex doesn't exist, an error is returned
func (d *Dag[K, T]) AddEdge(from, to K) error {
	if from == to {
		return fmt.Errorf("failed to add edge, the from and to can not be the same")
	}

	fromVertex, exist := d.vertices[from]
	if !exist {
		return fmt.Errorf("failed to add edge, the from vertex %v doesn't exist", from)
	}

	toVertex, exist := d.vertices[to]
	if !exist {
		return fmt.Errorf("failed to add edge, the to vertex %v doesn't exist", to)
	}

	d.checked = false
	fromVertex.outgoing[toVertex.name] = toVertex
	toVertex.incoming[fromVertex.name] = fromVertex

	return nil
}

// RemoveEdge removes the relationship between the vertex. There is no chance to create a cycle with this method, but the topological batch might change.
// If the key of from vertex and the to vertex doesn't exist, an error is returned
func (d *Dag[K, T]) RemoveEdge(from, to K) error {
	if from == to {
		return fmt.Errorf("failed to remove edge, the from and to can not be the same")
	}

	fromVertex, exist := d.vertices[from]
	if !exist {
		return fmt.Errorf("failed to remove edge, the from vertex %v doesn't exist", from)
	}

	toVertex, exist := d.vertices[to]
	if !exist {
		return fmt.Errorf("failed to remove edge, the to vertex %v doesn't exist", to)
	}

	d.checked = false
	delete(fromVertex.outgoing, to)
	delete(toVertex.incoming, from)
	return nil
}

// IsChecked return true if the graph is checked and there is no cycle in it.
func (d *Dag[K, T]) IsChecked() bool {
	return d.checked
}

// HasCycle checks if there is cycles in dag. The return values:
//   - yes: true means this graph is of dag and there is no cycle while false means that this graph is not a dag or it's not checked
//   - cycles: if the graph is not a dag, the cycles will be returned.
func (d *Dag[K, T]) CheckCycle() (bool, [][]K) {
	if d.checked {
		return true, nil
	}

	d.cachedFullTopo = nil
	d.cachedVertexTopo = sync.Map{}

	d.checked = true
	return true, nil
}

// TopologicalSort returns the topological sort of all vertices.
func (d *Dag[K, T]) TopologicalSort() ([]T, error) {
	if !d.IsChecked() {
		return nil, errors.New("the graph is not checked for acyclicity, please call CheckCycle first")
	}

	if d.cachedFullTopo != nil {
		return d.flatten(d.cachedFullTopo), nil
	}

	removed := make(map[K]struct{})
	vertices := make(map[K]struct{})

	for k := range d.vertices {
		vertices[k] = struct{}{}
	}

	batches := make([][]T, 0)
	for len(vertices) > 0 {
		batchKeys := make([]K, 0)
		for k := range vertices {
			if d.countLeftIncoming(removed, nil, d.vertices[k]) == 0 {
				batchKeys = append(batchKeys, k)
			}
		}
		batch := make([]T, 0, len(batchKeys))
		for _, k := range batchKeys {
			removed[k] = struct{}{}
			delete(vertices, k)
			batch = append(batch, d.vertices[k].value)
		}
		batches = append(batches, batch)
	}
	d.cachedFullTopo = batches
	return d.flatten(d.cachedFullTopo), nil
}

func (d *Dag[K, T]) countLeftIncoming(removed map[K]struct{}, limitation map[K]struct{}, vertex *vertex[K, T]) int {
	left := 0
	for k := range vertex.incoming {
		if limitation != nil {
			if _, exist := limitation[k]; !exist {
				continue
			}
		}
		if _, exist := removed[k]; !exist {
			left++
		}
	}
	return left
}

func (d *Dag[K, T]) countLeftOutgoing(removed map[K]struct{}, limitation map[K]struct{}, vertex *vertex[K, T]) int {
	left := 0
	for k := range vertex.outgoing {
		if limitation != nil {
			if _, exist := limitation[k]; !exist {
				continue
			}
		}
		if _, exist := removed[k]; !exist {
			left++
		}
	}
	return left
}

func (d *Dag[K, T]) flatten(batches [][]T) []T {
	result := make([]T, 0, len(d.vertices))
	for _, batch := range batches {
		result = append(result, batch...)
	}
	return result
}

// TopologicalBatchFrom returns the batches calculated in the graph with roots specified by names. The order is from the nearest to the farthest. So for most time, you should checkout the batches reversely.
//   - alignRight: by default, the nearest will be as far left as possible. But if this parameter is true, the nearest will be as far right as possible.
//   - names: names specify the roots in the final result. If its length is zero, all the vertices will be considered.
func (d *Dag[K, T]) TopologicalBatch(alignRight bool, names ...K) ([][]T, error) {
	if !d.IsChecked() {
		return nil, errors.New("the graph is not checked for acyclicity, please call CheckCycle first")
	}

	for _, name := range names {
		if _, exist := d.vertices[name]; !exist {
			return nil, fmt.Errorf("there is no vertex named %v", name)
		}
	}

	if len(names) == 0 {
		for _, vertex := range d.vertices {
			names = append(names, vertex.name)
		}
	}
	return d.topologicalBatchForSpecified(alignRight, names...)
}

func (d *Dag[K, T]) topologicalBatchForSpecified(alignRight bool, names ...K) ([][]T, error) {
	deps := make(map[K]map[K]struct{})
	for _, name := range names {
		if value, ok := d.cachedVertexTopo.Load(name); ok {
			typedValue, yes := value.(map[K]struct{})
			if !yes {
				return nil, fmt.Errorf("value type is not right, this must be a bug, please contact to the author")
			}
			deps[name] = typedValue
		}
	}

	for _, name := range names {
		if _, exist := deps[name]; exist {
			continue
		}
		deps[name] = d.collectDependentKeys(name)
	}

	for name, path := range deps {
		d.cachedVertexTopo.Store(name, path)
	}

	if alignRight {
		return d.calculateTopologicalBatchWithAlignRight(deps), nil
	} else {
		return d.calculateTopologicalBatchWithAlignLeft(deps), nil
	}
}

func (d *Dag[K, T]) collectDependentKeys(name K) map[K]struct{} {
	result := make(map[K]struct{})
	vertex := d.vertices[name]
	result[name] = struct{}{}
	for k := range vertex.outgoing {
		d.dfsCollectDependentKeys(result, k)
	}
	return result
}

func (d *Dag[K, T]) calculateTopologicalBatchWithAlignLeft(deps map[K]map[K]struct{}) [][]T {
	vertices := make(map[K]struct{})
	limitation := make(map[K]struct{})
	for _, dep := range deps {
		for k := range dep {
			vertices[k] = struct{}{}
			limitation[k] = struct{}{}
		}
	}
	removed := make(map[K]struct{})
	batches := make([][]T, 0)
	for len(vertices) > 0 {
		batchKeys := make([]K, 0)
		for k := range vertices {
			if d.countLeftIncoming(removed, limitation, d.vertices[k]) == 0 {
				batchKeys = append(batchKeys, k)
			}
		}
		batch := make([]T, 0, len(batchKeys))
		for _, k := range batchKeys {
			removed[k] = struct{}{}
			delete(vertices, k)
			batch = append(batch, d.vertices[k].value)
		}
		batches = append(batches, batch)
	}

	return batches
}

func (d *Dag[K, T]) calculateTopologicalBatchWithAlignRight(deps map[K]map[K]struct{}) [][]T {
	vertices := make(map[K]struct{})
	limitation := make(map[K]struct{})
	for _, dep := range deps {
		for k := range dep {
			vertices[k] = struct{}{}
			limitation[k] = struct{}{}
		}
	}
	removed := make(map[K]struct{})
	batches := make([][]T, 0)
	for len(vertices) > 0 {
		batchKeys := make([]K, 0)
		for k := range vertices {
			if d.countLeftOutgoing(removed, limitation, d.vertices[k]) == 0 {
				batchKeys = append(batchKeys, k)
			}
		}
		batch := make([]T, 0, len(batchKeys))
		for _, k := range batchKeys {
			removed[k] = struct{}{}
			delete(vertices, k)
			batch = append(batch, d.vertices[k].value)
		}
		batches = append(batches, batch)
	}

	return batches
}

func (d *Dag[K, T]) dfsCollectDependentKeys(result map[K]struct{}, name K) {
	result[name] = struct{}{}
	vertex := d.vertices[name]
	for k := range vertex.outgoing {
		d.dfsCollectDependentKeys(result, k)
	}
}

// String returns the string of dag, that can be useful for debug and logging.
func (d *Dag[K, T]) String() string {
	if pass, cycles := d.CheckCycle(); !pass {
		return fmt.Sprintf("there are cycles in the graph, cycles are %v", cycles)
	}

	return fmt.Sprint(d.TopologicalSort())
}

// Copy will copy the whole graph but the cached data.
func (d *Dag[K, T]) Copy() *Dag[K, T] {
	cpVertices := make(map[K]*vertex[K, T])
	for _, vertex := range d.vertices {
		v := newVertex(vertex.name, vertex.value)
		cpVertices[v.name] = v
	}

	for _, vertex := range d.vertices {
		cur := cpVertices[vertex.name]
		for k := range vertex.incoming {
			cur.incoming[k] = cpVertices[k]
		}
		for k := range vertex.outgoing {
			cur.outgoing[k] = cpVertices[k]
		}
	}

	cpDag := NewDag[K, T](d.name + "_copy")
	cpDag.checked = d.checked
	cpDag.vertices = cpVertices

	return cpDag
}
