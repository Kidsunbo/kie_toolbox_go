/*
For thread-safe:
	This library can support thread-safe, while you can also get a good performance if you disable the thread-safe feature while limit your way to use it. Passing DisableThreadSafe to NewDag
	will disable the thread-safe and you will gain almost 2 to 3 times improvement in performance. But be aware that if you disable the thread-safe feature, you can never modify the vertex and
	edge in pararrel, which means that you should never call AddVertex, RemoveVertex, AddEdge and RemoveEdge at in parallel.


For order:
	Normally, the flow is from left to right, which means that if you would like to finish C, you have to finish A before it. But if the Reverse flag is passed in some method,
	the order changed, which means that A depends on C, so if you would like to finish A, you have to finish B, C, D first.

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
	"strings"
	"sync"
	"sync/atomic"
)

type Flag struct{ id int }
type AlreadyDone[T any] []T
type Copier[T any] func(T) T

var DisableThreadSafe = Flag{id: 1}
var Reverse = Flag{id: 2}
var EnglishError = Flag{id: 3}
var ChineseError = Flag{id: 4}
var OnlyOneBatch = Flag{id: 5}

const (
	chinese int8 = iota
	english
)

type textIndexType int32

const (
	addVertexError textIndexType = iota
	removeVertexError
	addEdgeSameFromAndToError
	addEdgeFromNotExistError
	addEdgeToNotExistError
	removeEdgeSameFromAndToError
	removeEdgeFromNotExistError
	removeEdgeToNotExistError
	notCheckAcyclicityError
	noVertexError
	noNamesWhenAlreadyDoneError
	notCheckAcyclicityString
	dagHasErrorString
	dagNoErrorString
)

type languagePack struct {
	Chinese string
	English string
}

var msg = map[textIndexType]languagePack{
	addVertexError:               {Chinese: "添加节点失败，节点%v已经存在", English: "failed to add vertex, vertex %v has already existed"},
	removeVertexError:            {Chinese: "移除节点失败，节点%v不存在", English: "failed to remove vertex, vertex %v doesn't exist"},
	addEdgeSameFromAndToError:    {Chinese: "添加边失败，边的起点和终点不能一样", English: "failed to add edge, the from and to can not be the same"},
	addEdgeFromNotExistError:     {Chinese: "添加边失败，边的起点%v不存在", English: "failed to add edge, the from vertex %v doesn't exist"},
	addEdgeToNotExistError:       {Chinese: "添加边失败，边的终点%v不存在", English: "failed to add edge, the to vertex %v doesn't exist"},
	removeEdgeSameFromAndToError: {Chinese: "移除边失败，边的起点和终点不能一样", English: "failed to remove edge, the from and to can not be the same"},
	removeEdgeFromNotExistError:  {Chinese: "移除边失败，边的起点%v不存在", English: "failed to remove edge, the from vertex %v doesn't exist"},
	removeEdgeToNotExistError:    {Chinese: "移除边失败，边的终点%v不存在", English: "failed to remove edge, the to vertex %v doesn't exist"},
	notCheckAcyclicityError:      {Chinese: "未检查图中是否包含环，请调用CheckCycle进行检查", English: "the graph is not checked for acyclicity, please call CheckCycle first"},
	noVertexError:                {Chinese: "没有名字叫%v的节点", English: "there is no vertex named %v"},
	notCheckAcyclicityString:     {Chinese: "名称：%v，信息：未检查是否存在环", English: "name: %v, message: acyclicity not checked"},
	dagHasErrorString:            {Chinese: "名称：%v，错误：%v", English: "name: %v, err: %v"},
	dagNoErrorString:             {Chinese: "名称：%v，%v", English: "name: %v, %v"},
}

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
	mu           sync.RWMutex
	disableMutex bool
	name         string
	vertices     map[K]*vertex[K, T]
	language     int8

	checked             atomic.Bool
	cacheLock           sync.RWMutex
	cachedFullTopo      [][]T
	cachedVertexTopo    map[K]map[K]struct{} // key: vertex name, value: all the dependence name, including the indirect ones.
	cachedReverseVertex map[K]map[K]struct{} // key: vertex name, value: all the dependence name, including the indirect ones.
}

// NewDag create a directed acycle graph with each vertex whose key is of type K and the value is of type T.
// Normally, the name is of a string which keep unique in the whole graph. The value can be different.
func NewDag[K comparable, T any](name string, params ...any) *Dag[K, T] {
	disableMutex := false
	language := chinese
	for _, param := range params {
		if v, ok := param.(Flag); ok {
			if v == DisableThreadSafe {
				disableMutex = true
			} else if v == ChineseError {
				language = chinese
			} else if v == EnglishError {
				language = english
			}
		}
	}

	return &Dag[K, T]{
		mu:           sync.RWMutex{},
		disableMutex: disableMutex,
		name:         name,
		vertices:     map[K]*vertex[K, T]{},
		language:     language,
	}
}

// AddVertex add the vertex into the dag. Because adding new vertex might cause a cycle in the graph, so it will make the dag unchecked.
func (d *Dag[K, T]) AddVertex(name K, value T) error {
	if !d.disableMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	return d.addVertex(name, value)
}

func (d *Dag[K, T]) addVertex(name K, value T) error {
	if _, exist := d.vertices[name]; exist {
		return fmt.Errorf(d.message(addVertexError), name)
	}
	d.setChecked(false)
	d.vertices[name] = newVertex(name, value)
	return nil
}

func (d *Dag[K, T]) GetAllVertices() []T {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.getAllVertices()
}

func (d *Dag[K, T]) getAllVertices() []T {
	result := make([]T, 0, len(d.vertices))
	for _, v := range d.vertices {
		result = append(result, v.value)
	}
	return result
}

// GetAllEdges gets all the edges in the graph
func (d *Dag[K, T]) GetAllEdges() map[K][]K {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.getAllEdges()
}

func (d *Dag[K, T]) getAllEdges() map[K][]K {
	result := make(map[K][]K, len(d.vertices))
	for k, v := range d.vertices {
		if len(v.outgoing) == 0 {
			continue
		}
		edges := make([]K, 0, len(v.outgoing))
		for e := range v.outgoing {
			edges = append(edges, e)
		}
		result[k] = edges
	}
	return result
}

// RemoveVertex removes the vertex from the dag. There is no chance to create cycle in the graph, but the topological batch might change.
func (d *Dag[K, T]) RemoveVertex(name K) error {
	if !d.disableMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	return d.removeVertex(name)
}

func (d *Dag[K, T]) removeVertex(name K) error {
	cur, exist := d.vertices[name]
	if !exist {
		return fmt.Errorf(d.message(removeVertexError), name)
	}

	for _, vertex := range cur.incoming {
		d.removeEdge(vertex.name, cur.name)
	}

	for _, vertex := range cur.outgoing {
		d.removeEdge(cur.name, vertex.name)
	}

	d.setChecked(false)
	delete(d.vertices, name)
	return nil
}

// HasVertex only checks if the vertex has been added to the dag.
func (d *Dag[K, T]) HasVertex(name K) bool {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.hasVertex(name)
}

func (d *Dag[K, T]) hasVertex(name K) bool {
	_, exist := d.getVertex(name)
	return exist
}

// GetVertex gets the vertex of the K.
func (d *Dag[K, T]) GetVertex(name K) (T, bool) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.getVertex(name)
}

func (d *Dag[K, T]) getVertex(name K) (T, bool) {
	var t T
	vertex, exist := d.vertices[name]
	if !exist {
		return t, false
	}
	return vertex.value, true
}

// AddEdge adds the relationship between two vertex, which might cause a cycle in dag. If the key of from vertex and the to vertex doesn't exist, an error is returned
func (d *Dag[K, T]) AddEdge(from, to K) error {
	if !d.disableMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	return d.addEdge(from, to)
}

func (d *Dag[K, T]) addEdge(from, to K) error {
	if from == to {
		return errors.New(d.message(addEdgeSameFromAndToError))
	}

	fromVertex, exist := d.vertices[from]
	if !exist {
		return fmt.Errorf(d.message(addEdgeFromNotExistError), from)
	}

	toVertex, exist := d.vertices[to]
	if !exist {
		return fmt.Errorf(d.message(addEdgeToNotExistError), to)
	}

	d.setChecked(false)
	fromVertex.outgoing[toVertex.name] = toVertex
	toVertex.incoming[fromVertex.name] = fromVertex

	return nil
}

// RemoveEdge removes the relationship between the vertex. There is no chance to create a cycle with this method, but the topological batch might change.
// If the key of from vertex and the to vertex doesn't exist, an error is returned
func (d *Dag[K, T]) RemoveEdge(from, to K) error {
	if !d.disableMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	return d.removeEdge(from, to)
}

func (d *Dag[K, T]) removeEdge(from, to K) error {
	if from == to {
		return errors.New(d.message(removeEdgeSameFromAndToError))
	}

	fromVertex, exist := d.vertices[from]
	if !exist {
		return fmt.Errorf(d.message(removeEdgeFromNotExistError), from)
	}

	toVertex, exist := d.vertices[to]
	if !exist {
		return fmt.Errorf(d.message(removeEdgeToNotExistError), to)
	}

	d.setChecked(false)
	delete(fromVertex.outgoing, to)
	delete(toVertex.incoming, from)
	return nil
}

// IsChecked returns true if the graph is checked and there is no cycle in it.
func (d *Dag[K, T]) IsChecked() bool {
	return d.readChecked()
}

// HasCycle checks if there is cycles in dag. The return values:
//   - pass: true means this graph is of dag and there is no cycle while false means that this graph is not a dag or it's not checked
//   - cycles: if the graph is not a dag, the cycles will be returned.
func (d *Dag[K, T]) CheckCycle() (bool, [][]K) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	if d.readChecked() {
		return true, nil
	}

	d.cacheLock.Lock()
	d.cachedFullTopo = nil
	d.cachedVertexTopo = make(map[K]map[K]struct{}, len(d.vertices))
	d.cachedReverseVertex = make(map[K]map[K]struct{}, len(d.vertices))
	d.cacheLock.Unlock()

	if len(d.vertices) == 0 {
		d.setChecked(true)
		return true, nil
	}

	state := make(map[K]int, len(d.vertices))
	low := make(map[K]int, len(d.vertices))
	onStack := make(map[K]bool, len(d.vertices))
	id := 1
	sccCount := 0
	stack := make([]K, 0, len(d.vertices))
	for k := range d.vertices {
		if state[k] == 0 {
			d.dfsCheckCycle(k, state, low, &stack, onStack, &id, &sccCount)
		}
	}
	if sccCount != len(d.vertices) {
		rev := make(map[int][]K)
		for k, v := range low {
			rev[v] = append(rev[v], k)
		}
		cycles := make([][]K, 0)
		for _, k := range rev {
			if len(k) > 1 {
				cycles = append(cycles, k)
			}
		}
		return false, cycles
	}

	d.setChecked(true)
	return true, nil
}

func (d *Dag[K, T]) dfsCheckCycle(key K, state map[K]int, low map[K]int, stack *[]K, onStack map[K]bool, id *int, sccCount *int) {
	*stack = append(*stack, key)
	onStack[key] = true
	state[key] = *id
	low[key] = *id
	*id++

	vertex := d.vertices[key]
	for outKey := range vertex.outgoing {
		if state[outKey] == 0 {
			d.dfsCheckCycle(outKey, state, low, stack, onStack, id, sccCount)
		}
		if onStack[outKey] {
			if low[key] > low[outKey] {
				low[key] = low[outKey]
			}
		}
	}

	if state[key] == low[key] {
		value := (*stack)[len(*stack)-1]
		*stack = (*stack)[:len(*stack)-1]
		for {
			onStack[value] = false
			low[value] = state[key]
			if value == key {
				break
			}
			value = (*stack)[len(*stack)-1]
			*stack = (*stack)[:len(*stack)-1]
		}
		*sccCount++
	}
}

// TopologicalSort returns the topological sort of all vertices.
func (d *Dag[K, T]) TopologicalSort() ([]T, error) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.topologicalSort()
}

func (d *Dag[K, T]) topologicalSort() ([]T, error) {
	if !d.readChecked() {
		return nil, errors.New(d.message(notCheckAcyclicityError))
	}

	d.cacheLock.RLock()
	cachedFullTopo := d.cachedFullTopo
	d.cacheLock.RUnlock()
	if cachedFullTopo != nil {
		return d.flatten(cachedFullTopo), nil
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
	d.cacheLock.Lock()
	d.cachedFullTopo = batches
	d.cacheLock.Unlock()
	return d.flatten(batches), nil
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

// NextBatch returns the batch will no more dependence in this graph with roots specified by names.
// It's only a wrapper for TopologicalBatch with OnlyOneBatch passed in.
// It accept the same parameters of TopologicalBatch.
func (d *Dag[K, T]) NextBatch(params ...any) ([]T, error) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}
	params = append(params, OnlyOneBatch)
	batches, err := d.topologicalBatch(params...)
	if len(batches) > 0 {
		return batches[0], err
	}
	return nil, err
}

// TopologicalBatchFrom returns the batches calculated in the graph with roots specified by names. The order is from the nearest to the farthest. So for most time, you should checkout the batches reversely.
//
// Normal Params:
//   - target (type:K or []K),  target specify the roots in the final result. If its length is zero, all the vertices will be considered as the root.
//
// Config Params:
//   - reverse (type:Flag): by default, the roots are in the first batch and then sub-vertices of the first batch in the second batch in sequential order. If this parameter is true, the non-dependent vertices will be in the first batch and the order is opposite to the default.
//   - already_done (type:AlreadyDone[K]): by default, all the dependencies will be considered. Sometimes, you get some tasks finished and don't wanna they influence the order, you can pass them with this field.
func (d *Dag[K, T]) TopologicalBatch(params ...any) ([][]T, error) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.topologicalBatch(params...)
}

func (d *Dag[K, T]) topologicalBatch(params ...any) ([][]T, error) {
	target := make(map[K]struct{})
	reverse := false
	alreadyDone := make(map[K]struct{})
	onlyOneBatch := false
	for _, param := range params {
		if v, ok := param.(Flag); ok {
			if v == Reverse {
				reverse = true
			} else if v == OnlyOneBatch {
				onlyOneBatch = true
			}
		} else if v, ok := param.(K); ok {
			target[v] = struct{}{}
		} else if v, ok := param.(AlreadyDone[K]); ok {
			for _, k := range v {
				alreadyDone[k] = struct{}{}
			}
		} else if v, ok := param.([]K); ok {
			for _, k := range v {
				target[k] = struct{}{}
			}
		}
	}

	if !d.readChecked() {
		return nil, errors.New(d.message(notCheckAcyclicityError))
	}

	for name := range target {
		if _, exist := d.vertices[name]; !exist {
			return nil, fmt.Errorf(d.message(noVertexError), name)
		}
	}

	for name := range alreadyDone {
		if _, exist := d.vertices[name]; !exist {
			return nil, fmt.Errorf(d.message(noVertexError), name)
		}
	}

	if len(target) == 0 {
		for _, vertex := range d.vertices {
			target[vertex.name] = struct{}{}
		}
	}
	return d.topologicalBatchForSpecified(reverse, onlyOneBatch, alreadyDone, target)
}

func (d *Dag[K, T]) topologicalBatchForSpecified(reverse bool, onlyOneBatch bool, alreadyDone map[K]struct{}, names map[K]struct{}) ([][]T, error) {
	deps := make(map[K]map[K]struct{}, len(names))
	alreadyDoneDeps := make(map[K]map[K]struct{}, len(alreadyDone))

	// this will boost the speed than lock in every loop
	d.cacheLock.RLock()
	cache := d.cachedVertexTopo
	if reverse {
		cache = d.cachedReverseVertex
	}
	for name := range names {
		if value, exist := cache[name]; exist {
			deps[name] = value
		}
	}
	for name := range alreadyDone {
		if value, exist := cache[name]; exist {
			alreadyDoneDeps[name] = value
		}
	}
	d.cacheLock.RUnlock()

	needUpdateCache := false
	for name := range names {
		if _, exist := deps[name]; exist {
			continue
		}
		needUpdateCache = true
		deps[name] = d.collectDependentKeys(name, reverse)
	}
	for name := range alreadyDone {
		if _, exist := alreadyDoneDeps[name]; exist {
			continue
		}
		needUpdateCache = true
		alreadyDoneDeps[name] = d.collectDependentKeys(name, reverse)
	}
	if needUpdateCache {
		func() {
			d.cacheLock.Lock()
			defer d.cacheLock.Unlock()
			cache := d.cachedVertexTopo
			if reverse {
				cache = d.cachedReverseVertex
			}
			for name, dep := range deps {
				cache[name] = dep
			}
			for name, dep := range alreadyDoneDeps {
				cache[name] = dep
			}
		}()
	}

	if reverse {
		return d.calculateTopologicalBatchReversely(onlyOneBatch, deps, alreadyDoneDeps), nil
	} else {
		return d.calculateTopologicalBatchSequentially(onlyOneBatch, deps, alreadyDoneDeps), nil
	}
}

func (d *Dag[K, T]) collectDependentKeys(name K, reverse bool) map[K]struct{} {
	result := make(map[K]struct{})
	vertex := d.vertices[name]
	result[name] = struct{}{}
	if reverse {
		for k := range vertex.outgoing {
			d.dfsCollectDependentKeys(result, k, reverse)
		}
	} else {
		for k := range vertex.incoming {
			d.dfsCollectDependentKeys(result, k, reverse)
		}
	}

	return result
}

func (d *Dag[K, T]) calculateTopologicalBatchSequentially(onlyOneBatch bool, deps map[K]map[K]struct{}, alreadyDoneDeps map[K]map[K]struct{}) [][]T {
	removed := make(map[K]struct{}, len(d.vertices))
	for _, dep := range alreadyDoneDeps {
		for k := range dep {
			removed[k] = struct{}{}
		}
	}

	vertices := make(map[K]struct{}, len(d.vertices))
	limitation := make(map[K]struct{}, len(d.vertices))
	for _, dep := range deps {
		for k := range dep {
			if _, exist := removed[k]; exist {
				continue
			}
			vertices[k] = struct{}{}
			limitation[k] = struct{}{}
		}
	}

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
		if onlyOneBatch {
			break
		}
	}

	return batches
}

func (d *Dag[K, T]) calculateTopologicalBatchReversely(onlyOneBatch bool, deps map[K]map[K]struct{}, alreadyDoneDeps map[K]map[K]struct{}) [][]T {
	removed := make(map[K]struct{}, len(d.vertices))
	for _, dep := range alreadyDoneDeps {
		for k := range dep {
			removed[k] = struct{}{}
		}
	}

	vertices := make(map[K]struct{}, len(d.vertices))
	limitation := make(map[K]struct{}, len(d.vertices))
	for _, dep := range deps {
		for k := range dep {
			if _, exist := removed[k]; exist {
				continue
			}
			vertices[k] = struct{}{}
			limitation[k] = struct{}{}
		}
	}

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
		if onlyOneBatch {
			break
		}
	}

	return batches
}

func (d *Dag[K, T]) dfsCollectDependentKeys(result map[K]struct{}, name K, reverse bool) {
	result[name] = struct{}{}
	vertex := d.vertices[name]
	if reverse {
		for k := range vertex.outgoing {
			d.dfsCollectDependentKeys(result, k, reverse)
		}
	} else {
		for k := range vertex.incoming {
			d.dfsCollectDependentKeys(result, k, reverse)
		}
	}
}

// CanReach checks if one vertex can reach to another.
func (d *Dag[K, T]) CanReach(from, to K) (bool, error) {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	return d.canReach(from, to)
}

func (d *Dag[K, T]) canReach(from, to K) (bool, error) {
	_, exist := d.vertices[from]
	if !exist {
		return false, fmt.Errorf(d.message(noVertexError), from)
	}

	_, exist = d.vertices[to]
	if !exist {
		return false, fmt.Errorf(d.message(noVertexError), to)
	}

	if !d.readChecked() {
		return false, errors.New(d.message(notCheckAcyclicityError))
	}

	cached, exist := func() (bool, bool) {
		d.cacheLock.RLock()
		defer d.cacheLock.RUnlock()
		keys, cached := d.cachedVertexTopo[from]
		if !cached {
			return cached, false
		}
		_, exist := keys[to]
		return cached, exist
	}()

	if !cached {
		d.cacheLock.Lock()
		dep := d.collectDependentKeys(from, true)
		d.cachedVertexTopo[from] = dep
		_, exist = dep[to]
		d.cacheLock.Unlock()
	}

	return exist, nil
}

// String returns the string of dag, that can be useful for debug and logging.
func (d *Dag[K, T]) String() string {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	if !d.readChecked() {
		return fmt.Sprintf(d.message(notCheckAcyclicityString), d.name)
	}

	output, err := d.topologicalSort()
	if err != nil {
		return fmt.Sprintf(d.message(dagHasErrorString), d.name, err)
	}
	return fmt.Sprintf(d.message(dagNoErrorString), d.name, output)
}

func (d *Dag[K, T]) Dot() string {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	if !d.readChecked() {
		return fmt.Sprintf(d.message(notCheckAcyclicityString), d.name)
	}

	sb := strings.Builder{}
	sb.WriteString("digraph G {\n")

	for _, vertex := range d.vertices {
		if len(vertex.outgoing) == 0 {
			sb.WriteString(fmt.Sprintf("  %v;\n", vertex.name))
		}
		for _, outgoing := range vertex.outgoing {
			sb.WriteString(fmt.Sprintf("  %v -> %v;\n", vertex.name, outgoing.name))
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}

// SetName sets the name of the dag.
func (d *Dag[K, T]) SetName(name string) {
	d.name = name
}

// GetName gets the name of the dag.
func (d *Dag[K, T]) GetName() string {
	return d.name
}

// Copy will copy the whole graph but the cached data.
func (d *Dag[K, T]) Copy(params ...any) *Dag[K, T] {
	if !d.disableMutex {
		d.mu.RLock()
		defer d.mu.RUnlock()
	}

	valueCopyFunction := Copier[T](func(t T) T {
		return t
	})
	for _, param := range params {
		if f, ok := param.(Copier[T]); ok {
			valueCopyFunction = f
		}
	}

	cpVertices := make(map[K]*vertex[K, T])
	for _, vertex := range d.vertices {
		value := valueCopyFunction(vertex.value)
		v := newVertex(vertex.name, value)
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
	cpDag.checked.Store(d.checked.Load())
	cpDag.vertices = cpVertices
	cpDag.disableMutex = d.disableMutex

	return cpDag
}

func (d *Dag[K, T]) message(textIndex textIndexType) string {
	switch d.language {
	case chinese:
		return msg[textIndex].Chinese
	case english:
		return msg[textIndex].English
	default:
		return "error: unknown language"
	}
}

func (d *Dag[K, T]) readChecked() bool {
	data := d.checked.Load()
	return data
}

func (d *Dag[K, T]) setChecked(data bool) {
	d.checked.Store(data)
}
