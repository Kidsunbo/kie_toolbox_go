package kflow

type Node[T any] struct{}

func (n *Node[T]) ConditionalDependence(name string, when Condition[T], conditionDependence []string) *Dependence[T] {
	return &Dependence[T]{
		DependenceName:      name,
		Condition:           when,
		ConditionDependence: conditionDependence,
	}
}

func (n *Node[T]) StaticDependence(name string) *Dependence[T] {
	return &Dependence[T]{
		DependenceName:      name,
		Condition:           nil,
		ConditionDependence: nil,
	}
}

type NodeBox[T any] struct {
	Node       INode[T]
	BoxName    string	// box name is the same with node.Name() if there is no condition. But if there is, the name will be <node.Name()>_by_<relying_node_name>。
	Conditions Condition[T]
}
