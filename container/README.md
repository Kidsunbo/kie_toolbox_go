# Container

## Dag

`dag` provides a directed acyclic graph with some easy to use methods. Here are some example to use it.

#### Create a new Dag
```go
dag := container.NewDag[string, int]("name") // the key is of string type
```

As shown above, you have to specify the name for the dag, even though this field is only used for printing, it's still a good habit to use name for distinguishing purpose. There are two types passing into the dag. The first one is the type for `key` of each vertex which has to be unqiue in the whole graph. The second is the type of the vertex.
There are some configuration option can be passed into dag.
1. DisableThreadSafe: By default, there is a read-write lock to protect the data. If you will never add new vertex in parallel, you can disable this for a tiny boost.
2. EnglishError: Report the error message in English.
3. ChineseError: Report the error message in Chinese.

#### Modify vertex and edge
```go
err = dag.AddVertex("key1", 1) // add vertex with name key1

err = dag.RemoveVertex("key1") // remove the vertex named key1

err = dag.AddEdge("key1", "key2") // add an edge from key1 to key2

err = dag.RemoveEdge("key1", "key2") // remove the edge from key1 to key2
```

In `AddVertex`, you have to ensure that `key1` doesn't exist in the graph, otherwise error will be returned.

In `RemoveVertex`, you have to ensure that `key1` does exist in the graph, otherwise you will get an error. The edge relating to the vertex will be deleted automatically.

In `AddEdge`, you have to ensure that `key1` and `key2` exist in the graph and they are different. It's ok to add edge which has already existed.

In `RemoveEdge`, you have to ensure that `key1` and `key2` exist in the graph and they are different. It's ok to remove edge which doesn't exist.

Notice that all the modification will set the check state to false, which means you have to check the acyclic again.

#### Check Acyclic
```go
pass, cycles = dag.CheckCycle()
```
If there is no cycle in the graph, the `pass` will be true and `cycles` is empty. Or that `cycles` will show the cycles in the graph.

#### Topological
```go
data, err = dag.TopologicalSort()

batches, err := dag.TopologicalBatch()

batch, err := dag.NextBatch()

```

`TopologicalSort` return all the topological order of the dag.

`TopologicalBatch` return the batches in topological order. For detail, you can read the comment in the `dag.go`. It can specify the root vertices for this topological operation, which means that only the root vertices and their dependency will be considered. You can also specify what vertices should not be considered with `AlreadyDone`. The `Reverse` parameter can be used to align the graph, and the detail can also be found in the comment mentioned above.

`NextBatch` is a wrapper of `TopologicalBatch` which only returns the next batch rather than all the batches.

#### Other operation
```go
another, err = dag.Copy()

yes, err = dag.CanReach()
```

`Copy` can be used to copy the dag. The copier function can be passed in to determine how to copy the vertex.

`CanReach` can be used the check if two vertices have relationship.
