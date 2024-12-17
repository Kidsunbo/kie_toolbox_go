package container

import (
	"errors"
	"fmt"
)

type vertex[K comparable, T any] struct {
	name     K
	value    T
	batch    int
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
	name       string
	vertices   map[K]*vertex[K, T]
	checked    bool
	cachedTopo [][]*vertex[K, T]
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

// RemoveVertex removes the vertex from the dag. There is no chance to create cycle in the graph, so the check state of dag will keep.
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

// RemoveEdge removes the relationship between the vertex. There is no chance to create a cycle with this method. If the key of from vertex and the to vertex doesn't exist, an error is returned
func (d *Dag[K, T]) RemoveEdge(from, to K) error {
	if from == to {
		return fmt.Errorf("failed to remove edge, the from and to can not be the same")
	}

	fromVertex, exist := d.vertices[from]
	if !exist {
		return fmt.Errorf("failed to add edge, the from vertex %v doesn't exist", from)
	}

	toVertex, exist := d.vertices[to]
	if !exist {
		return fmt.Errorf("failed to add edge, the to vertex %v doesn't exist", to)
	}

	delete(fromVertex.outgoing, to)
	delete(toVertex.incoming, from)
	return nil
}

// IsChecked return if the dag is checked.
func (d *Dag[K, T]) IsChecked() bool {
	return d.checked
}

// HasCycle checks if there is cycles in dag. The return values:
//   - pass: if the check is passed, which means that there is no cycle in dag.
//   - cycles: if the check is failed, the cycles will be returned.
func (d *Dag[K, T]) CheckCycle() (bool, [][]K) {
	if d.checked {
		return true, nil
	}

	cp := d.Copy()
	topo := make([][]*vertex[K, T], 0)
	for len(cp.vertices) != 0 {
		batch := make([]*vertex[K, T], 0)
		for _, vertex := range cp.vertices {
			if len(vertex.incoming) == 0 {
				batch = append(batch, vertex)
			}
		}
		if len(batch) == 0 {
			return false, nil
		}
		for _, vertex := range batch {
			cp.RemoveVertex(vertex.name)
		}
		topo = append(topo, batch)
		for _, vertex := range batch {
			vertex.batch = len(topo)
		}
	}
	d.cachedTopo = topo

	return true, nil
}

// TopologicalSort returns the topological sort of all vertices.
func (d *Dag[K, T]) TopologicalSort() ([]T, error) {
	if pass, _ := d.CheckCycle(); !pass {
		return nil, errors.New("there is at least one cycle in the graph")
	}

	result := make([]T, 0, len(d.vertices))
	for _, batch := range d.cachedTopo {
		for _, vertex := range batch {
			result = append(result, vertex.value)
		}
	}

	return result, nil
}

// TopologicalBatchFrom returns the batches according to the vertex referenced by the parameter. The order of returned batches is from farthest to nearest.
func (d *Dag[K, T]) TopologicalBatchFrom(name K) ([][]T, error) {
	if pass, _ := d.CheckCycle(); !pass {
		return nil, errors.New("there is at least one cycle in the graph")
	}

	result := make([][]T, 0, len(d.cachedTopo))
	for _, batch := range d.cachedTopo {
		outputBatch := make([]T, 0, len(batch))
		for _, vertex := range batch {
			if len(result) == 0 && vertex.name != name {
				continue
			}
			outputBatch = append(outputBatch, vertex.value)
		}
		if len(outputBatch) == 0 {
			continue
		}
		result = append(result, outputBatch)
	}

	return result, nil
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
