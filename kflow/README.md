# KFlow

`KFlow` is a library to provide task engine related functionalities. It analyzes the dependence and maintains a DAG in the engine.

## Usage
Run the following command to use this library.
```shell
go get github.com/Kidsunbo/kie_toolbox_go/kflow@latest
```

#### Create An Engine

```go
engine := kflow.NewEngine("name")
```
There are several config parameters can be passed into the constructor. `ReportInEnglish` and `ReportInChinese` will determine the language of error message report by the engine. `AsyncTimeout` specifies the timeout of concurrent execution. Notice that, if the node is not running in parallel, it has no timeout control.


#### Add Node

```go
node := new(Node[DataType])
kflow.AddNode(engine, node)

err := kflow.Prepare()
```

Adding a node is easy as shown above. But make sure that the `Node` type is following the requirement. The basic requirement is the node has to implement `kflow.INode` and either `kflow.IBasicNode[Data]` or `kflow.IFlowNode[Data]`. `Data` is the data structure passed to every node.

The difference between `IBasicNode` and `IFlowNode` is that the latter can accept `Plan` which contains the running information of the process and adding new target dynamically in the `Run` method. Normally, user should use `IBaseNode` for safety. Do remember check if the current node is running **in parallel** before reading or modify the targets with `Plan` because of thread-safe consideration, the plan is only allowed to be accessed synchronously.

Remember to call `Prepare` because it do the checking and anylize the dependence information.

#### Example of Node

```go
type Node struct {
    Node[State]
}

func (*Node) Name() string {return "node"}

func (*Node) Dependence() []string {
    return []string{"other_node"}
} 

func (*Node) Dependence() []*Dependence[State] {
    return []*Dependence{
        node.StaticDependence("other_node"),
        node.ConditionalDependence(
            "other_node", 
            /* the condition check function */ func(ctx context.Context, state State)bool{return true}, 
            []string{"conditional_dependence"},
        ),
    }
}

func (*Node) Run(ctx context.Context, state State) error {
    return nil
}

func (*Node) Run(ctx context.Context, state State, plan *Plan) error {
    return nil
}
```

As shown, a valid node must have a `Name` function. There are two kinds of `Dependence` method and feel free to choose one you need. You can also ignore `Dependence` if the node doesn't have any dependence. There are also two kinds of `Run` method as shown in the example, while the former is for `BasicNode` and the latter is for `FlowNode`.